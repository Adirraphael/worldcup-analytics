"""
snowflake_connector.py
Handles all Snowflake interactions: connection, schema setup, data loading,
and query helpers used by the dashboard.
"""

import logging
import os
from contextlib import contextmanager

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def _get_connect_kwargs() -> dict:
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}. "
            "Copy .env.example → .env and fill in your credentials."
        )

    kwargs = {
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database":  os.getenv("SNOWFLAKE_DATABASE",  "WORLDCUP"),
        "schema":    os.getenv("SNOWFLAKE_SCHEMA",    "PUBLIC"),
    }
    role = os.getenv("SNOWFLAKE_ROLE")
    if role:
        kwargs["role"] = role
    return kwargs


def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Return an open Snowflake connection using credentials from .env."""
    return snowflake.connector.connect(**_get_connect_kwargs())


@contextmanager
def connection_context():
    """Context manager that opens and closes a Snowflake connection."""
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema setup
# ---------------------------------------------------------------------------

def setup_database(conn: snowflake.connector.SnowflakeConnection) -> None:
    """Create database/schema if they don't exist, then use them."""
    database = os.getenv("SNOWFLAKE_DATABASE", "WORLDCUP")
    schema   = os.getenv("SNOWFLAKE_SCHEMA",   "PUBLIC")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

    with conn.cursor() as cur:
        cur.execute(f"USE WAREHOUSE {warehouse}")
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        cur.execute(f"USE DATABASE {database}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(f"USE SCHEMA {schema}")

    logger.info("Database: %s | Schema: %s", database, schema)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Uppercase column names, fix separators, and rename Snowflake reserved words."""
    df = df.copy()
    df.columns = [
        c.upper().replace(" ", "_").replace("-", "_").replace(".", "_")
        for c in df.columns
    ]
    # Snowflake doesn't allow columns starting with a digit
    df.columns = [f"COL_{c}" if c[0].isdigit() else c for c in df.columns]

    # Rename columns that clash with Snowflake reserved keywords so they can
    # be referenced in SQL without quoting tricks.
    _RESERVED = {
        "SECOND": "RUNNER_UP",  # SECOND is a date-part keyword in Snowflake
    }
    df.columns = [_RESERVED.get(c, c) for c in df.columns]
    return df


def load_table(
    conn: snowflake.connector.SnowflakeConnection,
    df: pd.DataFrame,
    table_name: str,
    overwrite: bool = True,
) -> bool:
    """Load a DataFrame into a Snowflake table, creating it if necessary.

    Parameters
    ----------
    conn:       open Snowflake connection
    df:         data to load
    table_name: Snowflake table name (will be upper-cased)
    overwrite:  if True, truncate existing data before loading
    """
    if df.empty:
        logger.warning("Skipping empty DataFrame for table %s", table_name)
        return False

    clean_df = _sanitize_columns(df)
    tname = table_name.upper()

    try:
        success, n_chunks, n_rows, _ = write_pandas(
            conn=conn,
            df=clean_df,
            table_name=tname,
            auto_create_table=True,
            overwrite=overwrite,
        )
        if success:
            logger.info("  ✓  %-35s %d rows loaded", tname, n_rows)
        else:
            logger.error("  ✗  %-35s write_pandas returned failure", tname)
        return success
    except Exception as exc:
        logger.error("  ✗  %-35s %s", tname, exc)
        return False


def load_all_tables(
    data: dict[str, pd.DataFrame],
    overwrite: bool = True,
) -> dict[str, bool]:
    """Open a connection, set up the schema, and load all tables.

    Parameters
    ----------
    data:      dict mapping table_name → DataFrame
    overwrite: whether to replace existing data

    Returns
    -------
    dict mapping table_name → success bool
    """
    results: dict[str, bool] = {}

    with connection_context() as conn:
        setup_database(conn)
        logger.info("=== Loading %d tables into Snowflake ===", len(data))

        for name, df in data.items():
            results[name] = load_table(conn, df, name, overwrite=overwrite)

    ok  = sum(v for v in results.values())
    bad = len(results) - ok
    logger.info("=== Load complete: %d succeeded, %d failed ===", ok, bad)
    return results


# ---------------------------------------------------------------------------
# Query helpers (used by the dashboard)
# ---------------------------------------------------------------------------

def run_query(sql: str, params: tuple | None = None) -> pd.DataFrame:
    """Execute *sql* and return the results as a DataFrame.

    Parameters
    ----------
    sql:    SQL string (use %s placeholders for parameterised queries)
    params: tuple of parameter values matching the placeholders
    """
    with connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def get_table_columns(table_name: str) -> list[str]:
    """Return the actual column names for *table_name* as stored in Snowflake.

    Uses SELECT * LIMIT 0 so no rows are transferred.  Returns an empty list
    if the table doesn't exist or the query fails.
    """
    try:
        df = run_query(f"SELECT * FROM {table_name.upper()} LIMIT 0")
        return list(df.columns)
    except Exception:
        return []


def get_tournament_years() -> list[int]:
    """Return a sorted list of all tournament years in the database."""
    try:
        df = run_query(
            "SELECT DISTINCT YEAR FROM TOURNAMENTS ORDER BY YEAR DESC"
        )
        return df["YEAR"].tolist()
    except Exception:
        return list(range(2022, 1929, -4))


def fetch_top_scorers(men_only: bool = True) -> pd.DataFrame:
    """Aggregate all-time top scorers directly from the GOALS table.

    Discovers actual column names at runtime so the query never breaks due
    to assumed column names that differ in the real Snowflake table.
    """
    cols = get_table_columns("GOALS")
    if not cols:
        return pd.DataFrame()

    cols_upper = [c.upper() for c in cols]

    # ── determine player expression ──────────────────────────────────────
    # Verified columns: GIVEN_NAME + FAMILY_NAME (no combined PLAYER_NAME).
    has_given  = "GIVEN_NAME"  in cols_upper
    has_family = "FAMILY_NAME" in cols_upper
    has_player = "PLAYER_NAME" in cols_upper  # fallback if schema changes

    if has_given and has_family:
        player_expr  = "TRIM(COALESCE(GIVEN_NAME,'') || ' ' || COALESCE(FAMILY_NAME,''))"
        player_group = "GIVEN_NAME, FAMILY_NAME"
    elif has_player:
        player_expr  = "PLAYER_NAME"
        player_group = "PLAYER_NAME"
    else:
        logger.error("GOALS table has no recognisable player-name column. Columns: %s", cols_upper)
        return pd.DataFrame()

    # ── team-name column ─────────────────────────────────────────────────
    team_col = next(
        (c for c in cols_upper if "PLAYER_TEAM_NAME" in c), None
    ) or next(
        (c for c in cols_upper if "TEAM" in c and "NAME" in c), None
    )
    team_select = f",\n            {team_col} AS TEAM_NAME" if team_col else ""
    team_group  = f", {team_col}"                           if team_col else ""

    # ── own-goal filter ──────────────────────────────────────────────────
    own_goal_col    = "OWN_GOAL" if "OWN_GOAL" in cols_upper else None
    own_goal_filter = f"AND ({own_goal_col} = FALSE OR {own_goal_col} IS NULL)" if own_goal_col else ""

    # ── men's tournament filter ──────────────────────────────────────────
    tourn_col  = "TOURNAMENT_NAME" if "TOURNAMENT_NAME" in cols_upper else None
    men_filter = (
        f"AND {tourn_col} NOT ILIKE '%Women%'"
        f" AND {tourn_col} NOT ILIKE '%U-20%'"
        f" AND {tourn_col} NOT ILIKE '%Under-20%'"
        f" AND {tourn_col} NOT ILIKE '%Confederations%'"
        f" AND {tourn_col} NOT ILIKE '%Olympic%'"
    ) if (men_only and tourn_col) else ""

    logger.info(
        "fetch_top_scorers — player_expr: %s | team: %s | own_goal: %s | tourn: %s",
        player_expr, team_col, own_goal_col, tourn_col,
    )

    sql = f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS RANK,
            {player_expr}   AS PLAYER_NAME
            {team_select},
            COUNT(*)        AS GOALS
        FROM GOALS
        WHERE 1=1
            {own_goal_filter}
            {men_filter}
        GROUP BY {player_group}{team_group}
        ORDER BY GOALS DESC
        LIMIT 30
    """

    try:
        return run_query(sql)
    except Exception as exc:
        logger.error("fetch_top_scorers failed: %s", exc)
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Dashboard-specific queries
# ---------------------------------------------------------------------------

# Men's-only filter — plain % signs, used in queries WITHOUT a %s parameter
# (no Python format-string substitution happens, so % is safe).
_MEN_FILTER = """
    TOURNAMENT_NAME NOT ILIKE '%Women%'
    AND TOURNAMENT_NAME NOT ILIKE '%U-20%'
    AND TOURNAMENT_NAME NOT ILIKE '%Under-20%'
    AND TOURNAMENT_NAME NOT ILIKE '%Confederations%'
    AND TOURNAMENT_NAME NOT ILIKE '%Olympic%'
"""

# Same filter with %% instead of % so Python's %-format used by the Snowflake
# cursor (sql % params) treats them as literal percent signs, not placeholders.
# Use this inside any query that also contains a %s parameter.
_MEN_FILTER_ESC = """
    TOURNAMENT_NAME NOT ILIKE '%%Women%%'
    AND TOURNAMENT_NAME NOT ILIKE '%%U-20%%'
    AND TOURNAMENT_NAME NOT ILIKE '%%Under-20%%'
    AND TOURNAMENT_NAME NOT ILIKE '%%Confederations%%'
    AND TOURNAMENT_NAME NOT ILIKE '%%Olympic%%'
"""

# Subquery to translate a calendar year → the matching tournament name(s).
# Uses _MEN_FILTER_ESC because this snippet is embedded in parameterised queries
# that pass YEAR via %s — the %% escaping prevents "not enough arguments" errors.
_YEAR_SUBQUERY = f"""
    TOURNAMENT_NAME IN (
        SELECT TOURNAMENT_NAME FROM TOURNAMENTS
        WHERE YEAR = %s AND {_MEN_FILTER_ESC}
    )
"""

QUERIES = {
    # TOURNAMENTS actual columns (verified):
    # YEAR, TOURNAMENT_NAME, HOST_COUNTRY, WINNER, HOST_WON, COUNT_TEAMS
    "tournament_winners": f"""
        SELECT
            YEAR,
            TOURNAMENT_NAME,
            HOST_COUNTRY,
            WINNER,
            COUNT_TEAMS    AS TEAMS_ENTERED
        FROM TOURNAMENTS
        WHERE {_MEN_FILTER}
        ORDER BY YEAR DESC
    """,

    # GROUP_STANDINGS actual columns (verified):
    # TOURNAMENT_NAME, GROUP_NAME, POSITION, TEAM_NAME,
    # PLAYED, WINS, DRAWS, LOSSES, GOALS_FOR, GOALS_AGAINST, GOAL_DIFFERENCE, POINTS
    # No YEAR column — filter via TOURNAMENT_NAME.
    "top_scoring_teams": f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY SUM(GOALS_FOR) DESC) AS RANK,
            TEAM_NAME,
            SUM(PLAYED)         AS PLAYED,
            SUM(WINS)           AS WINS,
            SUM(DRAWS)          AS DRAWS,
            SUM(LOSSES)         AS LOSSES,
            SUM(GOALS_FOR)      AS GOALS_FOR,
            SUM(GOALS_AGAINST)  AS GOALS_AGAINST,
            SUM(GOALS_FOR) - SUM(GOALS_AGAINST) AS GOAL_DIFFERENCE
        FROM GROUP_STANDINGS
        WHERE {_MEN_FILTER}
        GROUP BY TEAM_NAME
        ORDER BY GOALS_FOR DESC
        LIMIT 30
    """,

    # top_scorers is handled by fetch_top_scorers() which discovers column
    # names at runtime — placeholder keeps the dict key for the dispatcher.
    "top_scorers": "SELECT 1",

    # MATCHES actual columns (verified):
    # TOURNAMENT_NAME, MATCH_ID, STAGE_NAME, GROUP_NAME, MATCH_DATE,
    # HOME_TEAM_NAME, AWAY_TEAM_NAME, HOME_TEAM_SCORE, AWAY_TEAM_SCORE,
    # EXTRA_TIME, PENALTY_SHOOTOUT, SCORE_PENALTIES, STADIUM_NAME,
    # CITY_NAME, COUNTRY_NAME, KNOCKOUT_STAGE, GROUP_STAGE
    # No YEAR column — filter via TOURNAMENT_NAME subquery.
    "matches_by_year": f"""
        SELECT
            MATCH_ID,
            TOURNAMENT_NAME,
            STAGE_NAME,
            GROUP_NAME,
            MATCH_DATE,
            HOME_TEAM_NAME,
            HOME_TEAM_SCORE,
            AWAY_TEAM_SCORE,
            AWAY_TEAM_NAME,
            EXTRA_TIME,
            PENALTY_SHOOTOUT,
            SCORE_PENALTIES,
            STADIUM_NAME,
            CITY_NAME,
            COUNTRY_NAME
        FROM MATCHES
        WHERE {_YEAR_SUBQUERY}
        ORDER BY MATCH_ID
    """,

    # GROUP_STANDINGS — no YEAR column, filter via TOURNAMENT_NAME subquery.
    "group_standings_by_year": f"""
        SELECT
            GROUP_NAME,
            POSITION,
            TEAM_NAME,
            PLAYED,
            WINS        AS WON,
            DRAWS,
            LOSSES      AS LOST,
            GOALS_FOR,
            GOALS_AGAINST,
            GOAL_DIFFERENCE,
            POINTS
        FROM GROUP_STANDINGS
        WHERE {_YEAR_SUBQUERY}
        ORDER BY GROUP_NAME, POSITION
    """,

    # Knockout results come from MATCHES filtered by KNOCKOUT_STAGE = TRUE.
    # KNOCKOUT_STAGE_MATCHES table may not exist; MATCHES is always available.
    "knockout_by_year": f"""
        SELECT
            STAGE_NAME,
            HOME_TEAM_NAME,
            HOME_TEAM_SCORE,
            AWAY_TEAM_SCORE,
            AWAY_TEAM_NAME,
            EXTRA_TIME,
            PENALTY_SHOOTOUT,
            SCORE_PENALTIES
        FROM MATCHES
        WHERE KNOCKOUT_STAGE = TRUE
          AND {_YEAR_SUBQUERY}
        ORDER BY MATCH_ID
    """,

    # GOALS actual columns (verified):
    # TOURNAMENT_NAME, MATCH_ID, FAMILY_NAME, GIVEN_NAME, PLAYER_TEAM_NAME,
    # MINUTE_LABEL, MINUTE_REGULATION, MATCH_PERIOD, OWN_GOAL, PENALTY,
    # HOME_TEAM, AWAY_TEAM, STAGE_NAME
    # No YEAR, no PLAYER_NAME — combine GIVEN_NAME + FAMILY_NAME.
    "goals_by_year": f"""
        SELECT
            TRIM(COALESCE(GIVEN_NAME, '') || ' ' || COALESCE(FAMILY_NAME, '')) AS PLAYER_NAME,
            PLAYER_TEAM_NAME  AS TEAM,
            MINUTE_LABEL      AS MINUTE,
            MATCH_PERIOD,
            OWN_GOAL,
            PENALTY,
            HOME_TEAM         AS HOME_TEAM_NAME,
            AWAY_TEAM         AS AWAY_TEAM_NAME,
            STAGE_NAME
        FROM GOALS
        WHERE {_YEAR_SUBQUERY}
        ORDER BY MATCH_ID, MINUTE_REGULATION
    """,

    # AWARD_WINNERS — gracefully skipped if table doesn't exist.
    "awards_by_year": f"""
        SELECT
            AWARD_NAME,
            PLAYER_NAME,
            TEAM_NAME
        FROM AWARD_WINNERS
        WHERE {_YEAR_SUBQUERY}
        ORDER BY AWARD_NAME
    """,
}


def query(name: str, year: int | None = None) -> pd.DataFrame:
    """Run a named query, optionally filtered by year.

    Parameters
    ----------
    name: key from QUERIES dict
    year: if provided, passed as the single %s parameter
    """
    if name not in QUERIES:
        raise ValueError(f"Unknown query '{name}'. Available: {list(QUERIES)}")
    sql = QUERIES[name]
    params = (year,) if year is not None else None
    return run_query(sql, params)
