"""
data_fetcher.py
Fetches complete FIFA World Cup historical data (1930-2022) from two open-source datasets:
  - Primary:  jfjelstul/worldcup  (GitHub CSV, MIT license) — comprehensive structured tables
  - Supplement: openfootball/world-cup (GitHub JSON) — used as a fallback / cross-check
All data is cached locally under data/ to avoid re-fetching on every run.
"""

import io
import json
import logging
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source configuration
# ---------------------------------------------------------------------------

# Primary source: jfjelstul/worldcup  (covers 1930-2022)
JFJELSTUL_BASE = (
    "https://raw.githubusercontent.com/jfjelstul/worldcup/master/data-csv/"
)

# Supplementary source: openfootball JSON (used only when primary fails)
OPENFOOTBALL_BASE = (
    "https://raw.githubusercontent.com/openfootball/world-cup/master/"
)

# CSV files to download from the primary source
PRIMARY_DATASETS: dict[str, str] = {
    "tournaments":             "tournaments.csv",
    "teams":                   "teams.csv",
    "players":                 "players.csv",
    "matches":                 "matches.csv",
    "goals":                   "goals.csv",
    "group_standings":         "group_standings.csv",
    "group_matches":           "group_matches.csv",
    "knockout_stage_matches":  "knockout_stage_matches.csv",
    "squads":                  "squads.csv",
    "award_winners":           "award_winners.csv",
    "stadiums":                "stadiums.csv",
    "host_countries":          "host_countries.csv",
    "managers":                "managers.csv",
    "confederations":          "confederations.csv",
    "groups":                  "groups.csv",
    "bookings":                "bookings.csv",
    "substitutions":           "substitutions.csv",
    "penalty_kicks":           "penalty_kicks.csv",
}

DATA_DIR = Path(__file__).parent.parent / "data"


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _fetch_csv(url: str, cache_path: Path | None = None) -> pd.DataFrame | None:
    """Download a CSV from *url* and return it as a DataFrame.

    If *cache_path* is provided the file is cached locally so subsequent
    calls skip the network request.
    """
    if cache_path and cache_path.exists():
        logger.info("  cache hit  %s", cache_path.name)
        return pd.read_csv(cache_path, low_memory=False)

    logger.info("  fetching   %s", url)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text), low_memory=False)
        if cache_path:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(cache_path, index=False)
        return df
    except Exception as exc:
        logger.warning("  FAILED     %s  (%s)", url, exc)
        return None


def _fetch_json(url: str, cache_path: Path | None = None) -> dict | list | None:
    """Download a JSON resource from *url*, with optional local caching."""
    if cache_path and cache_path.exists():
        logger.info("  cache hit  %s", cache_path.name)
        return json.loads(cache_path.read_text())

    logger.info("  fetching   %s", url)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if cache_path:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(data, indent=2))
        return data
    except Exception as exc:
        logger.warning("  FAILED     %s  (%s)", url, exc)
        return None


# ---------------------------------------------------------------------------
# Primary fetch: jfjelstul/worldcup
# ---------------------------------------------------------------------------

def fetch_primary_datasets(cache: bool = True) -> dict[str, pd.DataFrame]:
    """Download all CSV tables from jfjelstul/worldcup.

    Returns a dict mapping table name → DataFrame.  Missing tables are
    omitted rather than raising an exception.
    """
    logger.info("=== Fetching primary datasets (jfjelstul/worldcup) ===")
    result: dict[str, pd.DataFrame] = {}

    for name, filename in PRIMARY_DATASETS.items():
        url = f"{JFJELSTUL_BASE}{filename}"
        cache_path = (DATA_DIR / filename) if cache else None
        df = _fetch_csv(url, cache_path)
        if df is not None:
            result[name] = df
            logger.info("  ✓  %-30s %d rows, %d cols", name, len(df), len(df.columns))
        else:
            logger.warning("  ✗  %-30s SKIPPED", name)

    return result


# ---------------------------------------------------------------------------
# Supplementary fetch: openfootball (fallback / enrichment)
# ---------------------------------------------------------------------------

_OPENFOOTBALL_YEARS = [
    1930, 1934, 1938, 1950, 1954, 1958, 1962, 1966, 1970, 1974,
    1978, 1982, 1986, 1990, 1994, 1998, 2002, 2006, 2010, 2014,
    2018, 2022,
]


def _parse_openfootball_match(match: dict, year: int) -> dict:
    """Flatten a single openfootball match object."""
    team1 = match.get("team1", {})
    team2 = match.get("team2", {})
    score = match.get("score", {})
    ft = score.get("ft", [None, None])
    et = score.get("et", [None, None])
    p  = score.get("p",  [None, None])

    return {
        "year":             year,
        "date":             match.get("date"),
        "time":             match.get("time"),
        "stage":            match.get("stage", match.get("round", "Group Stage")),
        "group":            match.get("group"),
        "stadium":          match.get("stadium", {}).get("name") if isinstance(match.get("stadium"), dict) else match.get("stadium"),
        "city":             match.get("city"),
        "home_team":        team1.get("name") if isinstance(team1, dict) else team1,
        "away_team":        team2.get("name") if isinstance(team2, dict) else team2,
        "home_score_ft":    ft[0] if ft else None,
        "away_score_ft":    ft[1] if ft else None,
        "home_score_et":    et[0] if et else None,
        "away_score_et":    et[1] if et else None,
        "home_score_pen":   p[0]  if p  else None,
        "away_score_pen":   p[1]  if p  else None,
    }


def fetch_openfootball_matches(years: list[int] | None = None, cache: bool = True) -> pd.DataFrame:
    """Fetch all match results from openfootball/world-cup JSON files.

    This is used as a fallback or cross-check against the primary dataset.
    """
    logger.info("=== Fetching supplementary data (openfootball) ===")
    years = years or _OPENFOOTBALL_YEARS
    all_matches: list[dict] = []

    for year in years:
        url = f"{OPENFOOTBALL_BASE}{year}/index.json"
        cache_path = (DATA_DIR / f"openfootball_{year}.json") if cache else None
        data = _fetch_json(url, cache_path)

        if data is None:
            continue

        # openfootball index.json structure: {"rounds": [{"name": ..., "matches": [...]}]}
        rounds = data.get("rounds", [])
        for rnd in rounds:
            for match in rnd.get("matches", []):
                m = _parse_openfootball_match(match, year)
                m["round"] = rnd.get("name", "")
                all_matches.append(m)

    if not all_matches:
        return pd.DataFrame()

    df = pd.DataFrame(all_matches)
    logger.info("  ✓  openfootball_matches  %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# Derived / enriched tables
# ---------------------------------------------------------------------------

def build_top_scorers(goals_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate goal-level data into a per-player career goals table."""
    if goals_df.empty:
        return pd.DataFrame()

    # Identify relevant columns defensively
    player_col = next(
        (c for c in goals_df.columns if "player_name" in c.lower()), None
    )
    team_col = next(
        (c for c in goals_df.columns if "team_name" in c.lower()), None
    )
    own_goal_col = next(
        (c for c in goals_df.columns if "own_goal" in c.lower()), None
    )

    if player_col is None:
        logger.warning("goals table has no player_name column; skipping top_scorers")
        return pd.DataFrame()

    df = goals_df.copy()

    # Exclude own goals from scorer tallies
    if own_goal_col:
        df = df[df[own_goal_col].astype(str).str.lower().isin(["false", "0", "no", ""])]

    group_cols = [player_col]
    if team_col:
        group_cols.append(team_col)

    agg = df.groupby(group_cols).size().reset_index(name="goals")
    agg = agg.sort_values("goals", ascending=False).reset_index(drop=True)
    agg.insert(0, "rank", agg.index + 1)
    return agg


def build_team_stats(matches_df: pd.DataFrame) -> pd.DataFrame:
    """Build an all-time team statistics table from the matches table."""
    if matches_df.empty:
        return pd.DataFrame()

    # Detect column names (may vary between dataset versions)
    home_team = next((c for c in matches_df.columns if c.lower() in ("home_team_name",)), None)
    away_team = next((c for c in matches_df.columns if c.lower() in ("away_team_name",)), None)
    home_score = next((c for c in matches_df.columns if c.lower() in ("home_team_score",)), None)
    away_score = next((c for c in matches_df.columns if c.lower() in ("away_team_score",)), None)

    if not all([home_team, away_team, home_score, away_score]):
        logger.warning("matches table is missing expected columns; skipping team_stats")
        return pd.DataFrame()

    df = matches_df[[home_team, away_team, home_score, away_score]].dropna(
        subset=[home_score, away_score]
    ).copy()
    df[home_score] = pd.to_numeric(df[home_score], errors="coerce")
    df[away_score] = pd.to_numeric(df[away_score], errors="coerce")
    df = df.dropna(subset=[home_score, away_score])

    records: list[dict] = []
    for _, row in df.iterrows():
        hs, as_ = int(row[home_score]), int(row[away_score])
        records.append({
            "team": row[home_team],
            "goals_for": hs, "goals_against": as_,
            "wins": int(hs > as_), "draws": int(hs == as_), "losses": int(hs < as_), "played": 1,
        })
        records.append({
            "team": row[away_team],
            "goals_for": as_, "goals_against": hs,
            "wins": int(as_ > hs), "draws": int(as_ == hs), "losses": int(as_ < hs), "played": 1,
        })

    stats = (
        pd.DataFrame(records)
        .groupby("team")
        .sum()
        .reset_index()
        .sort_values("goals_for", ascending=False)
        .reset_index(drop=True)
    )
    stats.insert(0, "rank", stats.index + 1)
    return stats


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def fetch_all_data(cache: bool = True) -> dict[str, pd.DataFrame]:
    """Fetch, combine, and return all World Cup datasets.

    Returns
    -------
    dict[str, pd.DataFrame]
        Keys are table names suitable for use as Snowflake table names.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Primary structured data
    data = fetch_primary_datasets(cache=cache)

    # Supplementary openfootball matches (stored as a separate table)
    openfootball_df = fetch_openfootball_matches(cache=cache)
    if not openfootball_df.empty:
        data["openfootball_matches"] = openfootball_df

    # Derived tables
    if "goals" in data:
        top_scorers = build_top_scorers(data["goals"])
        if not top_scorers.empty:
            data["top_scorers"] = top_scorers

    if "matches" in data:
        team_stats = build_team_stats(data["matches"])
        if not team_stats.empty:
            data["team_stats"] = team_stats

    logger.info("=== Fetch complete — %d tables ===", len(data))
    for name, df in data.items():
        logger.info("  %-35s %d rows", name, len(df))

    return data
