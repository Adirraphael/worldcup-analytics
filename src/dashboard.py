"""
dashboard.py
FIFA World Cup historical data dashboard — run with:
    streamlit run src/dashboard.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from snowflake_connector import query, run_query, get_tournament_years, fetch_top_scorers

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="FIFA World Cup Dashboard",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

st.markdown(
    """
    <style>
    .stApp { background-color: #0E1117; }

    .dashboard-header {
        background: linear-gradient(135deg, #C8102E 0%, #8B0000 60%, #1a1a2e 100%);
        border-radius: 12px;
        padding: 1.5rem 2rem;
        margin-bottom: 1.5rem;
    }
    .dashboard-header h1 {
        color: #FFFFFF;
        font-size: 2rem;
        font-weight: 800;
        margin: 0;
        letter-spacing: -0.5px;
    }
    .dashboard-header p {
        color: rgba(255,255,255,0.75);
        font-size: 0.9rem;
        margin: 0.3rem 0 0 0;
    }

    .kpi-card {
        background: #1A1F2E;
        border: 1px solid #2D3748;
        border-radius: 10px;
        padding: 1.2rem 1.5rem;
        text-align: center;
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: 700;
        color: #C8102E;
        line-height: 1.1;
    }
    .kpi-label {
        font-size: 0.8rem;
        color: #718096;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-top: 0.3rem;
    }

    .section-title {
        font-size: 1.15rem;
        font-weight: 700;
        color: #E2E8F0;
        border-left: 4px solid #C8102E;
        padding-left: 0.75rem;
        margin: 1.5rem 0 0.75rem 0;
    }

    /* Story callout — narrative text block */
    .story-callout {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border-left: 4px solid #C8102E;
        border-radius: 0 8px 8px 0;
        padding: 1.1rem 1.5rem;
        margin: 0.75rem 0 1.5rem 0;
        color: #CBD5E0;
        font-size: 0.97rem;
        line-height: 1.75;
    }
    .story-callout strong { color: #FFFFFF; }

    .stDataFrame { border-radius: 8px; }

    /* ── Sidebar ─────────────────────────────────────────────── */
    section[data-testid="stSidebar"] { background: #111827; }

    /* Nav menu container */
    section[data-testid="stSidebar"] .stRadio > div { gap: 2px; }

    /* Hide the radio circle wrapper (first div child of each label) */
    section[data-testid="stSidebar"] .stRadio label > div:first-child {
        display: none !important;
    }
    section[data-testid="stSidebar"] .stRadio input[type="radio"] { display: none !important; }

    /* Style each label as a clean nav button */
    section[data-testid="stSidebar"] .stRadio label {
        display: block !important;
        padding: 0.5rem 0.85rem !important;
        border-radius: 7px;
        cursor: pointer;
        transition: background 0.15s ease, color 0.15s ease;
        color: #718096;
        font-size: 0.88rem;
        font-weight: 500;
        margin: 1px 0;
        letter-spacing: 0.01em;
        user-select: none;
    }
    section[data-testid="stSidebar"] .stRadio label:hover {
        background: #1f2937;
        color: #E2E8F0;
    }
    section[data-testid="stSidebar"] div[role="radiogroup"] label[aria-checked="true"] {
        background: #1f2937 !important;
        color: #FFFFFF !important;
        font-weight: 600 !important;
    }
    section[data-testid="stSidebar"] .stRadio [data-testid="stMarkdownContainer"] p {
        margin: 0 !important;
        line-height: 1.2;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Plotly shared theme
# ---------------------------------------------------------------------------

_AXIS_STYLE = dict(gridcolor="#2D3748", zerolinecolor="#2D3748")

PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#E2E8F0", family="Inter, sans-serif"),
    margin=dict(l=20, r=20, t=40, b=20),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="rgba(0,0,0,0)"),
    xaxis=_AXIS_STYLE,
    yaxis=_AXIS_STYLE,
)


def plotly_layout(**overrides) -> dict:
    result = dict(PLOTLY_LAYOUT)
    for key, val in overrides.items():
        if key in ("xaxis", "yaxis") and isinstance(val, dict):
            result[key] = {**_AXIS_STYLE, **val}
        else:
            result[key] = val
    return result


PRIMARY_COLOR = "#C8102E"
GOLD, SILVER, BRONZE = "#FFD700", "#C0C0C0", "#CD7F32"

# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------


@st.cache_data(ttl=3600, show_spinner=False)
def load_tournament_years() -> list[int]:
    return get_tournament_years()


@st.cache_data(ttl=3600, show_spinner=False)
def load_tournament_winners() -> pd.DataFrame:
    return query("tournament_winners")


@st.cache_data(ttl=3600, show_spinner=False)
def load_top_scoring_teams() -> pd.DataFrame:
    return query("top_scoring_teams")


@st.cache_data(ttl=3600, show_spinner=False)
def load_top_scorers() -> pd.DataFrame:
    return fetch_top_scorers(men_only=True)


@st.cache_data(ttl=3600, show_spinner=False)
def load_matches(year: int) -> pd.DataFrame:
    return query("matches_by_year", year)


@st.cache_data(ttl=3600, show_spinner=False)
def load_group_standings(year: int) -> pd.DataFrame:
    return query("group_standings_by_year", year)


@st.cache_data(ttl=3600, show_spinner=False)
def load_knockout(year: int) -> pd.DataFrame:
    return query("knockout_by_year", year)


@st.cache_data(ttl=3600, show_spinner=False)
def load_goals(year: int) -> pd.DataFrame:
    return query("goals_by_year", year)


@st.cache_data(ttl=3600, show_spinner=False)
def load_awards(year: int) -> pd.DataFrame:
    return query("awards_by_year", year)


@st.cache_data(ttl=3600, show_spinner=False)
def load_tournament_timeline() -> pd.DataFrame:
    try:
        # Build from three simple single-table queries to avoid complex JOIN failures
        winners = query("tournament_winners")  # YEAR, TOURNAMENT_NAME, HOST_COUNTRY, WINNER, TEAMS_ENTERED
        if winners.empty:
            return pd.DataFrame()

        goals_agg = run_query(
            "SELECT TOURNAMENT_NAME, COUNT(*) AS GOALS_SCORED FROM GOALS GROUP BY TOURNAMENT_NAME"
        )
        matches_agg = run_query(
            "SELECT TOURNAMENT_NAME, COUNT(DISTINCT MATCH_ID) AS MATCHES_PLAYED FROM MATCHES GROUP BY TOURNAMENT_NAME"
        )

        tl = winners.rename(columns={"TEAMS_ENTERED": "COUNT_TEAMS"})
        if not goals_agg.empty:
            tl = tl.merge(goals_agg, on="TOURNAMENT_NAME", how="left")
        else:
            tl["GOALS_SCORED"] = 0
        if not matches_agg.empty:
            tl = tl.merge(matches_agg, on="TOURNAMENT_NAME", how="left")
        else:
            tl["MATCHES_PLAYED"] = 0

        tl["GOALS_SCORED"] = pd.to_numeric(tl["GOALS_SCORED"], errors="coerce").fillna(0)
        tl["MATCHES_PLAYED"] = pd.to_numeric(tl["MATCHES_PLAYED"], errors="coerce").fillna(0)
        return tl
    except Exception as exc:
        st.error(f"Could not load timeline data: {exc}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------


def kpi(value, label: str) -> str:
    return (
        f'<div class="kpi-card">'
        f'<div class="kpi-value">{value}</div>'
        f'<div class="kpi-label">{label}</div>'
        f"</div>"
    )


def section(title: str) -> None:
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)


def styled_table(df: pd.DataFrame, height: int = 400) -> None:
    st.dataframe(df, use_container_width=True, height=height, hide_index=True)


def callout(text: str) -> None:
    st.markdown(f'<div class="story-callout">{text}</div>', unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown(
        """
        <div style="text-align:center; padding: 1rem 0 0.5rem 0;">
            <span style="font-size:3rem;">⚽</span>
            <h2 style="color:#C8102E; margin:0.5rem 0 0; font-size:1.2rem; font-weight:800;">
                WORLD CUP<br>DASHBOARD
            </h2>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.divider()

    years = load_tournament_years()
    if not years:
        years = list(range(2022, 1929, -4))

    selected_year = st.selectbox(
        "Tournament year",
        options=["All time"] + [str(y) for y in years],
        index=0,
        key="year_filter",
    )
    year_int = int(selected_year) if selected_year != "All time" else None

    st.divider()

    PAGES = [
        "Overview",
        "Matches",
        "Group Stage",
        "Knockout Stage",
        "Top Scorers",
        "Team Stats",
        "Evolution of the Game",
        "Nations & Dynasties",
    ]
    selected_page = st.radio("", PAGES, label_visibility="collapsed")

    st.divider()
    st.markdown(
        "<div style='color:#4A5568; font-size:0.75rem; text-align:center;'>"
        "Data: jfjelstul/worldcup<br>openfootball/world-cup"
        "</div>",
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# Header banner
# ---------------------------------------------------------------------------

year_label = f"— {selected_year}" if selected_year != "All time" else "(1930 – 2022)"
page_label = selected_page.split("  ", 1)[-1] if "  " in selected_page else selected_page
st.markdown(
    f"""
    <div class="dashboard-header">
        <h1>⚽ FIFA World Cup {year_label}</h1>
        <p>{page_label} &nbsp;·&nbsp; Men's tournament &nbsp;·&nbsp; 22 editions</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Page: Overview
# ---------------------------------------------------------------------------

if "Overview" in selected_page:
    winners_df = load_tournament_winners()

    if not winners_df.empty:
        total_tournaments = len(winners_df)
        total_teams = winners_df["TEAMS_ENTERED"].sum() if "TEAMS_ENTERED" in winners_df.columns else "—"

        col1, col2, col3, col4 = st.columns(4)
        col1.markdown(kpi(total_tournaments, "Tournaments"), unsafe_allow_html=True)
        col2.markdown(
            kpi(int(total_teams) if total_teams != "—" else "—", "Total team slots"),
            unsafe_allow_html=True,
        )

        if "WINNER" in winners_df.columns:
            champ_counts = winners_df["WINNER"].value_counts()
            most_titles_team = champ_counts.index[0]
            most_titles_count = champ_counts.iloc[0]
            col3.markdown(
                kpi(f"{most_titles_count}×", f"Most titles — {most_titles_team}"),
                unsafe_allow_html=True,
            )
            col4.markdown(kpi(len(champ_counts), "Different champions"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    section("Tournament winners by year")
    if not winners_df.empty:
        display_cols = [
            c
            for c in ["YEAR", "TOURNAMENT_NAME", "HOST_COUNTRY", "WINNER", "TEAMS_ENTERED"]
            if c in winners_df.columns
        ]
        df_show = winners_df[display_cols].copy()
        if year_int:
            df_show = df_show[df_show["YEAR"] == year_int]
        styled_table(df_show, height=500)
    else:
        st.info("No tournament data found. Run `python load_data.py` first.")

    if not winners_df.empty and "WINNER" in winners_df.columns:
        section("World Cup titles by country")
        title_counts = (
            winners_df.groupby("WINNER")
            .size()
            .reset_index(name="TITLES")
            .sort_values("TITLES", ascending=True)
        )
        fig = px.bar(
            title_counts,
            x="TITLES",
            y="WINNER",
            orientation="h",
            color="TITLES",
            color_continuous_scale=["#3D0000", PRIMARY_COLOR, "#FF6B6B"],
            labels={"TITLES": "Titles", "WINNER": ""},
            title="All-time World Cup titles",
        )
        fig.update_coloraxes(showscale=False)
        fig.update_traces(marker_line_width=0)
        fig.update_layout(**PLOTLY_LAYOUT, height=420)
        st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Page: Matches
# ---------------------------------------------------------------------------

elif "Matches" in selected_page:
    if year_int is None:
        st.info("Select a specific tournament year in the sidebar to view match results.")
    else:
        matches_df = load_matches(year_int)

        if matches_df.empty:
            st.warning(f"No match data found for {year_int}.")
        else:
            total_g = None
            if "HOME_TEAM_SCORE" in matches_df.columns and "AWAY_TEAM_SCORE" in matches_df.columns:
                total_g = pd.to_numeric(
                    matches_df["HOME_TEAM_SCORE"], errors="coerce"
                ).sum() + pd.to_numeric(matches_df["AWAY_TEAM_SCORE"], errors="coerce").sum()

            c1, c2, c3 = st.columns(3)
            c1.markdown(kpi(len(matches_df), "Matches"), unsafe_allow_html=True)
            if total_g is not None:
                c2.markdown(kpi(int(total_g), "Goals"), unsafe_allow_html=True)
                c3.markdown(
                    kpi(f"{total_g / len(matches_df):.2f}", "Goals / match"),
                    unsafe_allow_html=True,
                )

            st.markdown("<br>", unsafe_allow_html=True)
            section("All matches")

            display_cols = [
                c
                for c in [
                    "STAGE_NAME",
                    "GROUP_NAME",
                    "MATCH_DATE",
                    "HOME_TEAM_NAME",
                    "HOME_TEAM_SCORE",
                    "AWAY_TEAM_SCORE",
                    "AWAY_TEAM_NAME",
                    "EXTRA_TIME",
                    "PENALTY_SHOOTOUT",
                    "SCORE_PENALTIES",
                    "STADIUM_NAME",
                    "CITY_NAME",
                ]
                if c in matches_df.columns
            ]

            stages = (
                sorted(matches_df["STAGE_NAME"].dropna().unique())
                if "STAGE_NAME" in matches_df.columns
                else []
            )
            if stages:
                sel_stage = st.multiselect(
                    "Filter by stage", stages, default=stages, key="stage_filter"
                )
                filtered = matches_df[matches_df["STAGE_NAME"].isin(sel_stage)]
            else:
                filtered = matches_df

            styled_table(filtered[display_cols], height=550)

            if "HOME_TEAM_SCORE" in matches_df.columns:
                section("Goals per match")
                plot_df = filtered[display_cols].copy()
                plot_df["HOME_TEAM_SCORE"] = pd.to_numeric(
                    plot_df["HOME_TEAM_SCORE"], errors="coerce"
                )
                plot_df["AWAY_TEAM_SCORE"] = pd.to_numeric(
                    plot_df["AWAY_TEAM_SCORE"], errors="coerce"
                )
                plot_df["TOTAL_GOALS"] = plot_df["HOME_TEAM_SCORE"] + plot_df["AWAY_TEAM_SCORE"]
                plot_df["MATCH"] = plot_df["HOME_TEAM_NAME"] + " v " + plot_df["AWAY_TEAM_NAME"]
                fig2 = px.bar(
                    plot_df.sort_values("TOTAL_GOALS", ascending=False).head(20),
                    x="MATCH",
                    y="TOTAL_GOALS",
                    color="TOTAL_GOALS",
                    color_continuous_scale=["#3D0000", PRIMARY_COLOR, "#FF6B6B"],
                    labels={"TOTAL_GOALS": "Goals", "MATCH": ""},
                    title="Top 20 highest-scoring matches",
                )
                fig2.update_coloraxes(showscale=False)
                fig2.update_traces(marker_line_width=0)
                fig2.update_layout(**plotly_layout(height=380, xaxis=dict(tickangle=-45)))
                st.plotly_chart(fig2, use_container_width=True)

# ---------------------------------------------------------------------------
# Page: Group Stage
# ---------------------------------------------------------------------------

elif "Group" in selected_page:
    if year_int is None:
        st.info("Select a specific tournament year to view group standings.")
    else:
        gs_df = load_group_standings(year_int)

        if gs_df.empty:
            st.warning(f"No group standings data for {year_int}.")
        else:
            section(f"{year_int} Group Stage Standings")

            groups = (
                sorted(gs_df["GROUP_NAME"].dropna().unique())
                if "GROUP_NAME" in gs_df.columns
                else []
            )

            if not groups:
                styled_table(gs_df)
            else:
                for i in range(0, len(groups), 2):
                    left_col, right_col = st.columns(2)
                    for col, grp_idx in [(left_col, i), (right_col, i + 1)]:
                        if grp_idx >= len(groups):
                            break
                        grp = groups[grp_idx]
                        grp_df = gs_df[gs_df["GROUP_NAME"] == grp].copy()
                        display_cols = [
                            c
                            for c in [
                                "POSITION",
                                "TEAM_NAME",
                                "PLAYED",
                                "WON",
                                "DRAWS",
                                "LOST",
                                "GOALS_FOR",
                                "GOALS_AGAINST",
                                "GOAL_DIFFERENCE",
                                "POINTS",
                            ]
                            if c in grp_df.columns
                        ]
                        grp_df = grp_df[display_cols]

                        with col:
                            st.markdown(
                                f"<div class='section-title'>Group {grp}</div>",
                                unsafe_allow_html=True,
                            )

                            def highlight_qualified(row):
                                if "POSITION" in row.index and row["POSITION"] <= 2:
                                    return ["background-color: #1a3a1a"] * len(row)
                                return [""] * len(row)

                            st.dataframe(
                                grp_df.style.apply(highlight_qualified, axis=1),
                                use_container_width=True,
                                hide_index=True,
                                height=min(180, 40 + 35 * len(grp_df)),
                            )

            section("Points comparison across groups")
            if "POINTS" in gs_df.columns and "TEAM_NAME" in gs_df.columns:
                fig3 = px.bar(
                    gs_df.sort_values("POINTS", ascending=False),
                    x="TEAM_NAME",
                    y="POINTS",
                    color="GROUP_NAME" if "GROUP_NAME" in gs_df.columns else None,
                    labels={"TEAM_NAME": "", "POINTS": "Points", "GROUP_NAME": "Group"},
                    title=f"{year_int} group stage points",
                )
                fig3.update_traces(marker_line_width=0)
                fig3.update_layout(**plotly_layout(height=380, xaxis=dict(tickangle=-45)))
                st.plotly_chart(fig3, use_container_width=True)

# ---------------------------------------------------------------------------
# Page: Knockout Stage
# ---------------------------------------------------------------------------

elif "Knockout" in selected_page:
    if year_int is None:
        st.info("Select a specific tournament year to view knockout results.")
    else:
        ko_df = load_knockout(year_int)

        if ko_df.empty:
            st.warning(f"No knockout data for {year_int}.")
        else:
            section(f"{year_int} Knockout Stage")

            stage_order = [
                "Round of 16",
                "Quarter-finals",
                "Semi-finals",
                "Third-place play-off",
                "Third place play-off",
                "Final",
            ]

            stages_present = (
                ko_df["STAGE_NAME"].dropna().unique()
                if "STAGE_NAME" in ko_df.columns
                else []
            )
            ordered_stages = [s for s in stage_order if s in stages_present]
            remaining = [s for s in stages_present if s not in stage_order]
            ordered_stages = ordered_stages + remaining

            for stage in ordered_stages:
                stage_df = ko_df[ko_df["STAGE_NAME"] == stage].copy()

                display_cols = [
                    c
                    for c in [
                        "HOME_TEAM_NAME",
                        "HOME_TEAM_SCORE",
                        "AWAY_TEAM_SCORE",
                        "AWAY_TEAM_NAME",
                        "EXTRA_TIME",
                        "PENALTY_SHOOTOUT",
                        "SCORE_PENALTIES",
                    ]
                    if c in stage_df.columns
                ]

                section(stage)
                for _, row in stage_df[display_cols].iterrows():
                    ht = row.get("HOME_TEAM_NAME", "")
                    hs = row.get("HOME_TEAM_SCORE", "")
                    as_ = row.get("AWAY_TEAM_SCORE", "")
                    at = row.get("AWAY_TEAM_NAME", "")
                    pen = row.get("SCORE_PENALTIES", "")

                    try:
                        winner = ht if int(hs) > int(as_) else at
                    except Exception:
                        winner = ""

                    pen_label = (
                        f"  *(pen. {pen})*"
                        if pen and str(pen).strip() not in ("", "nan")
                        else ""
                    )

                    c1, c2, c3 = st.columns([3, 1, 3])
                    c1.markdown(
                        f"<div style='text-align:right; font-weight:{'800' if winner==ht else '400'}; "
                        f"color:{'#FFFFFF' if winner==ht else '#718096'}'>{ht}</div>",
                        unsafe_allow_html=True,
                    )
                    c2.markdown(
                        f"<div style='text-align:center; font-weight:700; color:#C8102E; font-size:1.1rem'>"
                        f"{hs} – {as_}{pen_label}</div>",
                        unsafe_allow_html=True,
                    )
                    c3.markdown(
                        f"<div style='font-weight:{'800' if winner==at else '400'}; "
                        f"color:{'#FFFFFF' if winner==at else '#718096'}'>{at}</div>",
                        unsafe_allow_html=True,
                    )

# ---------------------------------------------------------------------------
# Page: Top Scorers
# ---------------------------------------------------------------------------

elif "Scorers" in selected_page:
    section("Top goal scorers of all time")

    scorers_df = load_top_scorers()

    if scorers_df.empty:
        st.info("No scorer data available.")
    else:
        if year_int:
            goals_year = load_goals(year_int)
            if not goals_year.empty and "PLAYER_NAME" in goals_year.columns:
                excl = (
                    goals_year["OWN_GOAL"].astype(str).str.lower().isin(["true", "1", "yes"])
                    if "OWN_GOAL" in goals_year.columns
                    else pd.Series([False] * len(goals_year))
                )
                gy = goals_year[~excl]
                agg = (
                    gy.groupby(["PLAYER_NAME", "TEAM"])
                    .size()
                    .reset_index(name="GOALS")
                    .sort_values("GOALS", ascending=False)
                    .head(20)
                    .reset_index(drop=True)
                )
                agg.insert(0, "RANK", agg.index + 1)
                section(f"Top scorers — {year_int}")
                left, right = st.columns([2, 3])
                with left:
                    styled_table(agg, height=500)
                with right:
                    fig5a = px.bar(
                        agg,
                        x="GOALS",
                        y="PLAYER_NAME",
                        orientation="h",
                        color="GOALS",
                        color_continuous_scale=["#3D0000", PRIMARY_COLOR, "#FF6B6B"],
                        labels={"GOALS": "Goals", "PLAYER_NAME": ""},
                        title=f"Top scorers — {year_int}",
                    )
                    fig5a.update_coloraxes(showscale=False)
                    fig5a.update_traces(marker_line_width=0)
                    fig5a.update_layout(**PLOTLY_LAYOUT, height=480)
                    st.plotly_chart(fig5a, use_container_width=True)

        section("All-time top scorers")
        left2, right2 = st.columns([2, 3])
        with left2:
            styled_table(scorers_df.head(30), height=600)
        with right2:
            top20 = scorers_df.head(20).sort_values("GOALS")
            colors = [
                GOLD
                if i == len(top20) - 1
                else SILVER
                if i == len(top20) - 2
                else BRONZE
                if i == len(top20) - 3
                else PRIMARY_COLOR
                for i in range(len(top20))
            ]
            fig5 = go.Figure(
                go.Bar(
                    x=top20["GOALS"],
                    y=top20["PLAYER_NAME"],
                    orientation="h",
                    marker_color=colors,
                    text=top20["GOALS"],
                    textposition="outside",
                )
            )
            fig5.update_layout(
                **plotly_layout(
                    height=600,
                    title="All-time top 20 scorers",
                    xaxis=dict(title="Career goals"),
                )
            )
            st.plotly_chart(fig5, use_container_width=True)

# ---------------------------------------------------------------------------
# Page: Team Stats
# ---------------------------------------------------------------------------

elif "Team Stats" in selected_page:
    section("All-time team statistics")

    teams_df = load_top_scoring_teams()

    if teams_df.empty:
        st.info("No team statistics data available.")
    else:
        if year_int:
            matches_yr = load_matches(year_int)
            if not matches_yr.empty:
                teams_in_year = set()
                for col in ("HOME_TEAM_NAME", "AWAY_TEAM_NAME"):
                    if col in matches_yr.columns:
                        teams_in_year.update(matches_yr[col].dropna().tolist())
                if "TEAM_NAME" in teams_df.columns:
                    teams_df = teams_df[teams_df["TEAM_NAME"].isin(teams_in_year)]

        if not teams_df.empty:
            c1, c2, c3, c4 = st.columns(4)
            top_team = teams_df.iloc[0]["TEAM_NAME"] if "TEAM_NAME" in teams_df.columns else "—"
            top_goals = teams_df.iloc[0]["GOALS_FOR"] if "GOALS_FOR" in teams_df.columns else "—"
            c1.markdown(kpi(len(teams_df), "Teams"), unsafe_allow_html=True)
            c2.markdown(kpi(top_team, "Most goals scored"), unsafe_allow_html=True)
            c3.markdown(
                kpi(int(top_goals) if top_goals != "—" else "—", f"Goals ({top_team})"),
                unsafe_allow_html=True,
            )
            if "WINS" in teams_df.columns:
                top_wins_team = teams_df.sort_values("WINS", ascending=False).iloc[0]
                c4.markdown(
                    kpi(
                        int(top_wins_team["WINS"]),
                        f"Most wins ({top_wins_team['TEAM_NAME']})",
                    ),
                    unsafe_allow_html=True,
                )

        st.markdown("<br>", unsafe_allow_html=True)
        left, right = st.columns([2, 3])

        with left:
            display_cols = [
                c
                for c in [
                    "RANK",
                    "TEAM_NAME",
                    "PLAYED",
                    "WINS",
                    "DRAWS",
                    "LOSSES",
                    "GOALS_FOR",
                    "GOALS_AGAINST",
                    "GOAL_DIFFERENCE",
                ]
                if c in teams_df.columns
            ]
            styled_table(teams_df[display_cols].head(30), height=600)

        with right:
            if "GOALS_FOR" in teams_df.columns and "TEAM_NAME" in teams_df.columns:
                top15 = teams_df.head(15).copy()
                fig6a = px.bar(
                    top15,
                    x="TEAM_NAME",
                    y=(
                        ["GOALS_FOR", "GOALS_AGAINST"]
                        if "GOALS_AGAINST" in top15.columns
                        else ["GOALS_FOR"]
                    ),
                    barmode="group",
                    color_discrete_map={
                        "GOALS_FOR": PRIMARY_COLOR,
                        "GOALS_AGAINST": "#4A5568",
                    },
                    labels={"TEAM_NAME": "", "value": "Goals", "variable": ""},
                    title="Goals scored vs conceded — top 15 teams",
                )
                fig6a.update_traces(marker_line_width=0)
                fig6a.update_layout(**plotly_layout(height=340, xaxis=dict(tickangle=-35)))
                st.plotly_chart(fig6a, use_container_width=True)

            if "WINS" in teams_df.columns:
                top10 = teams_df.head(10).copy()
                fig6b = px.bar(
                    top10,
                    x="TEAM_NAME",
                    y=["WINS", "DRAWS", "LOSSES"],
                    barmode="stack",
                    color_discrete_map={
                        "WINS": PRIMARY_COLOR,
                        "DRAWS": "#F6AD55",
                        "LOSSES": "#4A5568",
                    },
                    labels={"TEAM_NAME": "", "value": "Matches", "variable": "Result"},
                    title="Win / draw / loss breakdown — top 10 teams",
                )
                fig6b.update_traces(marker_line_width=0)
                fig6b.update_layout(**plotly_layout(height=290, xaxis=dict(tickangle=-30)))
                st.plotly_chart(fig6b, use_container_width=True)

# ---------------------------------------------------------------------------
# Page: Evolution of the Game
# ---------------------------------------------------------------------------

elif "Evolution" in selected_page:
    timeline_df = load_tournament_timeline()

    if timeline_df.empty:
        st.info("No timeline data available. Run `python load_data.py` first.")
    else:
        tl = timeline_df.copy()
        tl["YEAR"] = pd.to_numeric(tl["YEAR"], errors="coerce")
        tl["MATCHES_PLAYED"] = pd.to_numeric(tl["MATCHES_PLAYED"], errors="coerce")
        tl["GOALS_SCORED"] = pd.to_numeric(tl["GOALS_SCORED"], errors="coerce")
        tl["COUNT_TEAMS"] = pd.to_numeric(tl["COUNT_TEAMS"], errors="coerce")
        tl = tl.dropna(subset=["YEAR", "MATCHES_PLAYED", "GOALS_SCORED"])
        tl = tl[tl["MATCHES_PLAYED"] > 0]
        tl["GOALS_PER_MATCH"] = (tl["GOALS_SCORED"] / tl["MATCHES_PLAYED"]).round(2)
        tl = tl.sort_values("YEAR").reset_index(drop=True)

        # ── KPI row ───────────────────────────────────────────────────────
        total_goals = int(tl["GOALS_SCORED"].sum())
        avg_gpm = round(tl["GOALS_PER_MATCH"].mean(), 2)
        peak_row = tl.loc[tl["GOALS_PER_MATCH"].idxmax()]
        low_row = tl.loc[tl["GOALS_PER_MATCH"].idxmin()]

        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(kpi(f"{total_goals:,}", "Total goals (all time)"), unsafe_allow_html=True)
        c2.markdown(kpi(avg_gpm, "Avg goals per match"), unsafe_allow_html=True)
        c3.markdown(
            kpi(f"{peak_row['GOALS_PER_MATCH']}", f"Most open ({int(peak_row['YEAR'])})"),
            unsafe_allow_html=True,
        )
        c4.markdown(
            kpi(f"{low_row['GOALS_PER_MATCH']}", f"Most defensive ({int(low_row['YEAR'])})"),
            unsafe_allow_html=True,
        )

        st.markdown("<br>", unsafe_allow_html=True)

        # ── Opening story ─────────────────────────────────────────────────
        callout(
            f"Over 92 years and 22 tournaments, the World Cup has mirrored the evolution of "
            f"football itself. The <strong>{int(peak_row['YEAR'])} tournament</strong> was the "
            f"most spectacular on record — <strong>{peak_row['GOALS_PER_MATCH']} goals per match</strong> "
            f"— while <strong>{int(low_row['YEAR'])}</strong> produced the most defensive football "
            f"in history at just <strong>{low_row['GOALS_PER_MATCH']} goals per game</strong>. "
            f"From free-scoring pioneers to modern tactical battles, the numbers tell a story "
            f"that spans generations."
        )

        # ── Goals per match trend ─────────────────────────────────────────
        section("How open was each tournament? Goals per match, 1930–2022")

        fig_gpm = go.Figure()
        fig_gpm.add_trace(
            go.Scatter(
                x=tl["YEAR"],
                y=tl["GOALS_PER_MATCH"],
                mode="lines+markers",
                line=dict(color=PRIMARY_COLOR, width=2.5),
                marker=dict(size=8, color=PRIMARY_COLOR),
                hovertemplate="<b>%{x}</b><br>%{y} goals/match<extra></extra>",
                name="Goals per match",
            )
        )
        fig_gpm.add_hline(
            y=avg_gpm,
            line_dash="dash",
            line_color="#718096",
            annotation_text=f"92-year average: {avg_gpm}",
            annotation_font_color="#718096",
            annotation_position="bottom right",
        )
        fig_gpm.update_layout(
            **plotly_layout(
                height=380,
                title="Goals per match by tournament",
                xaxis=dict(title="Year", tickmode="linear", dtick=4, tickangle=-45),
                yaxis=dict(title="Goals per match"),
            )
        )
        st.plotly_chart(fig_gpm, use_container_width=True)

        # ── Insight after chart ────────────────────────────────────────────
        # Find the lowest-scoring run (3 consecutive below average)
        below_avg = tl[tl["GOALS_PER_MATCH"] < avg_gpm]["YEAR"].tolist()
        below_str = ", ".join(str(int(y)) for y in below_avg[:5]) if below_avg else ""

        callout(
            f"The dip in the late 1980s–early 1990s was no accident. In <strong>1990</strong>, "
            f"teams adopted ultra-defensive tactics, producing the lowest scoring rate in World Cup "
            f"history. FIFA responded by awarding <strong>3 points for a win</strong> (up from 2) "
            f"from the 1994 tournament onwards — and the effect was immediate: goals per match "
            f"climbed back toward the historical average and have stayed there ever since."
        )

        # ── Tournament expansion ──────────────────────────────────────────
        section("The growing tournament — how many teams competed?")

        if "COUNT_TEAMS" in tl.columns and tl["COUNT_TEAMS"].notna().any():
            teams_df_plot = tl.dropna(subset=["COUNT_TEAMS"]).copy()
            teams_df_plot["COUNT_TEAMS"] = teams_df_plot["COUNT_TEAMS"].astype(int)

            fig_teams = px.bar(
                teams_df_plot,
                x="YEAR",
                y="COUNT_TEAMS",
                color="COUNT_TEAMS",
                color_continuous_scale=["#3D0000", PRIMARY_COLOR, "#FF6B6B"],
                labels={"YEAR": "Year", "COUNT_TEAMS": "Teams"},
                title="Number of teams per tournament",
                text="COUNT_TEAMS",
            )
            fig_teams.update_coloraxes(showscale=False)
            fig_teams.update_traces(marker_line_width=0, textposition="outside")
            fig_teams.update_layout(
                **plotly_layout(
                    height=360,
                    xaxis=dict(tickmode="linear", dtick=4, tickangle=-45),
                    yaxis=dict(title="Teams"),
                )
            )
            st.plotly_chart(fig_teams, use_container_width=True)

            callout(
                "The World Cup started with <strong>13 teams</strong> in 1930 — a competition "
                "that fitted inside one country in three weeks. It grew to <strong>16 teams</strong> "
                "by 1938, then <strong>24 in 1982</strong> as FIFA opened the door to more "
                "confederations. The <strong>32-team format</strong> arrived in 1998 and "
                "produced the modern era of football's biggest stage. In 2026, the tournament "
                "will expand again to <strong>48 teams</strong>."
            )

        # ── Goals vs matches scatter ──────────────────────────────────────
        section("Total goals vs matches played — volume by era")

        fig_scatter = px.scatter(
            tl,
            x="MATCHES_PLAYED",
            y="GOALS_SCORED",
            text="YEAR",
            size="COUNT_TEAMS" if "COUNT_TEAMS" in tl.columns else None,
            color="GOALS_PER_MATCH",
            color_continuous_scale=["#4A5568", PRIMARY_COLOR, "#FF6B6B"],
            labels={
                "MATCHES_PLAYED": "Matches played",
                "GOALS_SCORED": "Goals scored",
                "GOALS_PER_MATCH": "Goals/match",
            },
            title="Tournament volume — goals vs matches (bubble size = teams entered)",
            hover_data={"YEAR": True, "WINNER": True, "GOALS_PER_MATCH": True},
        )
        fig_scatter.update_traces(textposition="top center", textfont_size=10)
        fig_scatter.update_layout(**plotly_layout(height=400))
        st.plotly_chart(fig_scatter, use_container_width=True)

# ---------------------------------------------------------------------------
# Page: Nations & Dynasties
# ---------------------------------------------------------------------------

elif "Nations" in selected_page:
    winners_df = load_tournament_winners()
    timeline_df = load_tournament_timeline()

    if winners_df.empty:
        st.info("No data available. Run `python load_data.py` first.")
    else:
        # Pre-compute key facts
        title_counts = winners_df.groupby("WINNER").size().reset_index(name="TITLES")
        title_counts = title_counts.sort_values("TITLES", ascending=False).reset_index(drop=True)
        unique_winners = len(title_counts)
        top_nation = title_counts.iloc[0]["WINNER"]
        top_nation_titles = int(title_counts.iloc[0]["TITLES"])

        # Confederation mapping
        _CONF = {
            "Brazil": "CONMEBOL",
            "Argentina": "CONMEBOL",
            "Uruguay": "CONMEBOL",
            "Germany": "UEFA",
            "West Germany": "UEFA",
            "Italy": "UEFA",
            "France": "UEFA",
            "England": "UEFA",
            "Spain": "UEFA",
        }
        winners_df["CONFEDERATION"] = winners_df["WINNER"].map(_CONF).fillna("Other")
        conf_counts = winners_df.groupby("CONFEDERATION").size().reset_index(name="TITLES")

        eu_titles = int(
            conf_counts.loc[conf_counts["CONFEDERATION"] == "UEFA", "TITLES"].sum()
        )
        sa_titles = int(
            conf_counts.loc[conf_counts["CONFEDERATION"] == "CONMEBOL", "TITLES"].sum()
        )

        # ── KPI row ───────────────────────────────────────────────────────
        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(kpi(unique_winners, "Nations ever won"), unsafe_allow_html=True)
        c2.markdown(
            kpi(f"{top_nation_titles}×", f"Most titles — {top_nation}"),
            unsafe_allow_html=True,
        )
        c3.markdown(kpi(eu_titles, "UEFA (Europe) titles"), unsafe_allow_html=True)
        c4.markdown(kpi(sa_titles, "CONMEBOL (South America)"), unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # ── Opening story ─────────────────────────────────────────────────
        callout(
            f"In 92 years of competition, only <strong>{unique_winners} nations</strong> have "
            f"ever won the FIFA World Cup. The trophy has never left Europe or South America — "
            f"<strong>Europe leads {eu_titles}–{sa_titles}</strong> overall, but South America "
            f"dominated the early decades before European football caught up. "
            f"<strong>{top_nation}</strong> stands alone at the top with "
            f"<strong>{top_nation_titles} titles</strong>, the only nation ever to win on three "
            f"different continents."
        )

        # ── Title leaderboard + continental split ─────────────────────────
        section("The Championship Roll of Honour")

        left, right = st.columns([3, 2])

        with left:
            fig_titles = px.bar(
                title_counts.sort_values("TITLES"),
                x="TITLES",
                y="WINNER",
                orientation="h",
                color="TITLES",
                color_continuous_scale=["#3D0000", PRIMARY_COLOR, "#FF6B6B"],
                labels={"TITLES": "Titles", "WINNER": ""},
                title="All-time World Cup titles",
                text="TITLES",
            )
            fig_titles.update_coloraxes(showscale=False)
            fig_titles.update_traces(marker_line_width=0, textposition="outside")
            fig_titles.update_layout(**PLOTLY_LAYOUT, height=380)
            st.plotly_chart(fig_titles, use_container_width=True)

        with right:
            fig_conf = px.pie(
                conf_counts,
                names="CONFEDERATION",
                values="TITLES",
                title="Titles by confederation",
                color="CONFEDERATION",
                color_discrete_map={
                    "UEFA": PRIMARY_COLOR,
                    "CONMEBOL": "#FFD700",
                    "Other": "#4A5568",
                },
                hole=0.45,
            )
            fig_conf.update_traces(
                textposition="inside",
                textinfo="label+percent",
                marker=dict(line=dict(color="#0E1117", width=2)),
            )
            fig_conf.update_layout(**PLOTLY_LAYOUT, height=380, showlegend=False)
            st.plotly_chart(fig_conf, use_container_width=True)

        # ── Championship timeline ─────────────────────────────────────────
        section("Spotting the dynasties — every title, 1930–2022")

        callout(
            "Each dot below is a World Cup title. Look for the <strong>clusters</strong>: "
            "Brazil's back-to-back in <strong>1958–62</strong> and again in <strong>1994–2002</strong>; "
            "Italy and (West) Germany trading titles through the <strong>1970s–80s</strong>; "
            "and Europe's extraordinary run of <strong>four consecutive titles from 2006–2018</strong> "
            "before Argentina reclaimed glory in 2022."
        )

        fig_timeline = px.scatter(
            winners_df.sort_values("YEAR"),
            x="YEAR",
            y="WINNER",
            color="CONFEDERATION",
            color_discrete_map={
                "UEFA": PRIMARY_COLOR,
                "CONMEBOL": "#FFD700",
                "Other": "#48BB78",
            },
            size=[18] * len(winners_df),
            title="World Cup titles by year",
            hover_data={"YEAR": True, "WINNER": True, "HOST_COUNTRY": True},
            labels={"YEAR": "Year", "WINNER": "", "CONFEDERATION": "Confederation"},
        )
        fig_timeline.update_traces(marker=dict(line=dict(width=1, color="#0E1117")))
        fig_timeline.update_layout(
            **plotly_layout(
                height=420,
                xaxis=dict(tickmode="linear", dtick=4, tickangle=-45),
            )
        )
        st.plotly_chart(fig_timeline, use_container_width=True)

        # ── Decade-by-decade ──────────────────────────────────────────────
        section("Who owned each decade?")

        winners_df["DECADE"] = (winners_df["YEAR"] // 10 * 10).astype(str) + "s"
        decade_df = (
            winners_df.groupby(["DECADE", "WINNER"])
            .size()
            .reset_index(name="TITLES")
        )

        fig_decade = px.bar(
            decade_df,
            x="DECADE",
            y="TITLES",
            color="WINNER",
            title="Titles per decade by country",
            labels={"DECADE": "Decade", "TITLES": "Titles", "WINNER": "Nation"},
            barmode="stack",
        )
        fig_decade.update_traces(marker_line_width=0)
        fig_decade.update_layout(**plotly_layout(height=380))
        st.plotly_chart(fig_decade, use_container_width=True)

        # ── Host advantage ────────────────────────────────────────────────
        section("The Home Field Advantage")

        if "HOST_COUNTRY" in winners_df.columns and "WINNER" in winners_df.columns:
            host_win_df = winners_df[
                winners_df["HOST_COUNTRY"] == winners_df["WINNER"]
            ].copy()
            host_wins = len(host_win_df)
            total_t = len(winners_df)
            host_pct = round(host_wins / total_t * 100, 1) if total_t else 0
            expected_pct = round(100 / total_t, 1) if total_t else 0

            hc1, hc2, hc3 = st.columns(3)
            hc1.markdown(kpi(f"{host_wins} / {total_t}", "Host nations that won"), unsafe_allow_html=True)
            hc2.markdown(kpi(f"{host_pct}%", "Host win rate"), unsafe_allow_html=True)
            hc3.markdown(kpi(f"~{expected_pct}%", "Expected rate (1/field)"), unsafe_allow_html=True)

            callout(
                f"Hosting the World Cup is the ultimate home advantage. "
                f"<strong>{host_wins} of {total_t} tournaments</strong> have been won by the "
                f"host nation — a <strong>{host_pct}% win rate</strong>, roughly "
                f"{round(host_pct / expected_pct, 1)}× what pure chance would predict. "
                f"The roar of a home crowd, familiar conditions, and no travel fatigue add up "
                f"to a measurable edge that no amount of tactics can fully explain."
            )

            if not host_win_df.empty:
                host_win_df = host_win_df.copy()
                host_win_df["YEAR"] = host_win_df["YEAR"].astype(int)
                st.dataframe(
                    host_win_df[["YEAR", "HOST_COUNTRY", "WINNER"]].rename(
                        columns={
                            "YEAR": "Year",
                            "HOST_COUNTRY": "Host country",
                            "WINNER": "Champion",
                        }
                    ),
                    use_container_width=False,
                    hide_index=True,
                )

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.divider()
st.markdown(
    "<div style='text-align:center; color:#4A5568; font-size:0.78rem; padding:0.5rem 0'>"
    "Data: "
    "<a href='https://github.com/jfjelstul/worldcup' style='color:#718096'>jfjelstul/worldcup</a>"
    " · "
    "<a href='https://github.com/openfootball/world-cup' style='color:#718096'>openfootball/world-cup</a>"
    "</div>",
    unsafe_allow_html=True,
)
