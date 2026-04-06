"""
dashboard.py
FIFA World Cup historical data dashboard — run with:
    streamlit run src/dashboard.py
"""

import sys
import os
from pathlib import Path

# Ensure src/ is on the path regardless of where streamlit is invoked from
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from snowflake_connector import query, get_tournament_years, fetch_top_scorers

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
# Custom CSS — clean, modern dark aesthetic
# ---------------------------------------------------------------------------

st.markdown(
    """
    <style>
    /* ── Global ─────────────────────────────────────────────── */
    .stApp { background-color: #0E1117; }

    /* ── Header banner ──────────────────────────────────────── */
    .dashboard-header {
        background: linear-gradient(135deg, #C8102E 0%, #8B0000 60%, #1a1a2e 100%);
        border-radius: 12px;
        padding: 2rem 2.5rem;
        margin-bottom: 1.5rem;
    }
    .dashboard-header h1 {
        color: #FFFFFF;
        font-size: 2.4rem;
        font-weight: 800;
        margin: 0;
        letter-spacing: -0.5px;
    }
    .dashboard-header p {
        color: rgba(255,255,255,0.75);
        font-size: 1rem;
        margin: 0.4rem 0 0 0;
    }

    /* ── KPI cards ───────────────────────────────────────────── */
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

    /* ── Section titles ─────────────────────────────────────── */
    .section-title {
        font-size: 1.15rem;
        font-weight: 700;
        color: #E2E8F0;
        border-left: 4px solid #C8102E;
        padding-left: 0.75rem;
        margin: 1.5rem 0 0.75rem 0;
    }

    /* ── Tables ─────────────────────────────────────────────── */
    .stDataFrame { border-radius: 8px; }

    /* ── Tabs ───────────────────────────────────────────────── */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        background: #1A1F2E;
        border-radius: 8px;
        padding: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 6px;
        color: #718096;
        font-weight: 600;
    }
    .stTabs [aria-selected="true"] {
        background: #C8102E !important;
        color: #FFFFFF !important;
    }

    /* ── Sidebar ─────────────────────────────────────────────── */
    section[data-testid="stSidebar"] {
        background: #111827;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Plotly shared theme
# ---------------------------------------------------------------------------

# Base axis style — kept separate so it can be merged without conflicts.
_AXIS_STYLE = dict(gridcolor="#2D3748", zerolinecolor="#2D3748")

# Base layout WITHOUT xaxis/yaxis so callers can pass their own without
# triggering "got multiple values for keyword argument" errors.
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
    """Return PLOTLY_LAYOUT merged with *overrides*.

    xaxis / yaxis dicts are deep-merged so callers can add e.g. tickangle
    without duplicating the base grid-colour settings.
    """
    result = dict(PLOTLY_LAYOUT)
    for key, val in overrides.items():
        if key in ("xaxis", "yaxis") and isinstance(val, dict):
            result[key] = {**_AXIS_STYLE, **val}
        else:
            result[key] = val
    return result


COLOR_SCALE = px.colors.sequential.Reds_r
PRIMARY_COLOR = "#C8102E"
GOLD, SILVER, BRONZE = "#FFD700", "#C0C0C0", "#CD7F32"

# ---------------------------------------------------------------------------
# Cached data loaders
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


# ---------------------------------------------------------------------------
# Reusable rendering helpers
# ---------------------------------------------------------------------------

def kpi(value: str | int, label: str) -> str:
    return f"""
    <div class="kpi-card">
        <div class="kpi-value">{value}</div>
        <div class="kpi-label">{label}</div>
    </div>"""


def section(title: str) -> None:
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)


def styled_table(df: pd.DataFrame, height: int = 400) -> None:
    st.dataframe(df, use_container_width=True, height=height, hide_index=True)


def medal_emoji(pos: int) -> str:
    return {1: "🥇", 2: "🥈", 3: "🥉"}.get(pos, str(pos))


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown(
        """
        <div style="text-align:center; padding: 1rem 0 0.5rem 0;">
            <span style="font-size:3rem;">⚽</span>
            <h2 style="color:#C8102E; margin:0; font-size:1.2rem; font-weight:800;">
                WORLD CUP<br>DASHBOARD
            </h2>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.divider()

    years = load_tournament_years()
    if not years:
        st.warning("Could not load tournament years from Snowflake.")
        years = list(range(2022, 1929, -4))

    selected_year = st.selectbox(
        "Tournament year",
        options=["All time"] + [str(y) for y in years],
        index=0,
        key="year_filter",
    )
    year_int = int(selected_year) if selected_year != "All time" else None

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
st.markdown(
    f"""
    <div class="dashboard-header">
        <h1>⚽ FIFA World Cup {year_label}</h1>
        <p>Complete historical data · 1930 – 2022 · 22 tournaments</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Tab layout
# ---------------------------------------------------------------------------

tabs = st.tabs([
    "🏆  Overview",
    "⚽  Matches",
    "📊  Group Stage",
    "🥊  Knockout",
    "🎯  Scorers",
    "📈  Team Stats",
])

# ============================================================
# TAB 1 — OVERVIEW
# ============================================================
with tabs[0]:
    # KPI row
    winners_df = load_tournament_winners()

    if not winners_df.empty:
        # Verified TOURNAMENTS columns: YEAR, TOURNAMENT_NAME, HOST_COUNTRY,
        # WINNER, HOST_WON, COUNT_TEAMS  (no GOALS_SCORED / MATCHES_PLAYED)
        total_tournaments = len(winners_df)
        total_teams       = winners_df["TEAMS_ENTERED"].sum() if "TEAMS_ENTERED" in winners_df.columns else "—"

        col1, col2, col3, col4 = st.columns(4)
        col1.markdown(kpi(total_tournaments, "Tournaments"), unsafe_allow_html=True)
        col2.markdown(kpi(int(total_teams) if total_teams != "—" else "—", "Total Team Slots"), unsafe_allow_html=True)

        # Most titles
        if "WINNER" in winners_df.columns:
            champ_counts      = winners_df["WINNER"].value_counts()
            most_titles_team  = champ_counts.index[0]
            most_titles_count = champ_counts.iloc[0]
            col3.markdown(
                kpi(f"{most_titles_count}x", f"Most titles — {most_titles_team}"),
                unsafe_allow_html=True,
            )
            # Unique champions
            col4.markdown(kpi(len(champ_counts), "Different Champions"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Tournament winners table ───────────────────────────────
    section("Tournament winners by year")

    if not winners_df.empty:
        # Only show columns that actually exist in the table
        display_cols = [c for c in
            ["YEAR", "TOURNAMENT_NAME", "HOST_COUNTRY", "WINNER", "TEAMS_ENTERED"]
            if c in winners_df.columns
        ]
        df_show = winners_df[display_cols].copy()
        if year_int:
            df_show = df_show[df_show["YEAR"] == year_int]
        styled_table(df_show, height=500)
    else:
        st.info("No tournament data found. Run `python load_data.py` first.")

    # ── Titles bar chart ───────────────────────────────────────
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
            x="TITLES", y="WINNER",
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

# ============================================================
# TAB 2 — MATCHES
# ============================================================
with tabs[1]:
    if year_int is None:
        st.info("Select a specific tournament year in the sidebar to view match results.")
    else:
        matches_df = load_matches(year_int)

        if matches_df.empty:
            st.warning(f"No match data found for {year_int}.")
        else:
            # Mini KPIs
            total_g = None
            if "HOME_TEAM_SCORE" in matches_df.columns and "AWAY_TEAM_SCORE" in matches_df.columns:
                total_g = (
                    pd.to_numeric(matches_df["HOME_TEAM_SCORE"], errors="coerce").sum()
                    + pd.to_numeric(matches_df["AWAY_TEAM_SCORE"], errors="coerce").sum()
                )
            c1, c2, c3 = st.columns(3)
            c1.markdown(kpi(len(matches_df), "Matches"), unsafe_allow_html=True)
            if total_g is not None:
                c2.markdown(kpi(int(total_g), "Goals"), unsafe_allow_html=True)
                c3.markdown(kpi(f"{total_g/len(matches_df):.2f}", "Goals / match"), unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)
            section("All matches")

            display_cols = [c for c in [
                "STAGE_NAME", "GROUP_NAME", "MATCH_DATE",
                "HOME_TEAM_NAME", "HOME_TEAM_SCORE", "AWAY_TEAM_SCORE", "AWAY_TEAM_NAME",
                "EXTRA_TIME", "PENALTY_SHOOTOUT", "SCORE_PENALTIES",
                "STADIUM_NAME", "CITY_NAME",
            ] if c in matches_df.columns]

            # Stage filter
            stages = sorted(matches_df["STAGE_NAME"].dropna().unique()) if "STAGE_NAME" in matches_df.columns else []
            if stages:
                sel_stage = st.multiselect("Filter by stage", stages, default=stages, key="stage_filter")
                filtered = matches_df[matches_df["STAGE_NAME"].isin(sel_stage)]
            else:
                filtered = matches_df

            styled_table(filtered[display_cols], height=550)

            # Goals by match chart
            if "HOME_TEAM_SCORE" in matches_df.columns:
                section("Goals per match")
                plot_df = filtered[display_cols].copy()
                plot_df["HOME_TEAM_SCORE"] = pd.to_numeric(plot_df["HOME_TEAM_SCORE"], errors="coerce")
                plot_df["AWAY_TEAM_SCORE"] = pd.to_numeric(plot_df["AWAY_TEAM_SCORE"], errors="coerce")
                plot_df["TOTAL_GOALS"] = plot_df["HOME_TEAM_SCORE"] + plot_df["AWAY_TEAM_SCORE"]
                plot_df["MATCH"] = plot_df["HOME_TEAM_NAME"] + " v " + plot_df["AWAY_TEAM_NAME"]
                fig2 = px.bar(
                    plot_df.sort_values("TOTAL_GOALS", ascending=False).head(20),
                    x="MATCH", y="TOTAL_GOALS",
                    color="TOTAL_GOALS",
                    color_continuous_scale=["#3D0000", PRIMARY_COLOR, "#FF6B6B"],
                    labels={"TOTAL_GOALS": "Goals", "MATCH": ""},
                    title="Top 20 highest-scoring matches",
                )
                fig2.update_coloraxes(showscale=False)
                fig2.update_traces(marker_line_width=0)
                fig2.update_layout(**plotly_layout(height=380,
                                   xaxis=dict(tickangle=-45)))
                st.plotly_chart(fig2, use_container_width=True)

# ============================================================
# TAB 3 — GROUP STAGE
# ============================================================
with tabs[2]:
    if year_int is None:
        st.info("Select a specific tournament year to view group standings.")
    else:
        gs_df = load_group_standings(year_int)

        if gs_df.empty:
            st.warning(f"No group standings data for {year_int}.")
        else:
            section(f"{year_int} Group Stage Standings")

            groups = sorted(gs_df["GROUP_NAME"].dropna().unique()) if "GROUP_NAME" in gs_df.columns else []

            if not groups:
                styled_table(gs_df)
            else:
                # Render each group in a 2-column layout
                for i in range(0, len(groups), 2):
                    left_col, right_col = st.columns(2)
                    for col, grp_idx in [(left_col, i), (right_col, i + 1)]:
                        if grp_idx >= len(groups):
                            break
                        grp = groups[grp_idx]
                        grp_df = gs_df[gs_df["GROUP_NAME"] == grp].copy()
                        display_cols = [c for c in [
                            "POSITION", "TEAM_NAME", "PLAYED", "WON", "DRAWS", "LOST",
                            "GOALS_FOR", "GOALS_AGAINST", "GOAL_DIFFERENCE", "POINTS",
                        ] if c in grp_df.columns]
                        grp_df = grp_df[display_cols]

                        with col:
                            st.markdown(
                                f"<div class='section-title'>Group {grp}</div>",
                                unsafe_allow_html=True,
                            )
                            # Highlight top 2 (qualified)
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

            # Points comparison chart
            section("Points comparison across groups")
            if "POINTS" in gs_df.columns and "TEAM_NAME" in gs_df.columns:
                fig3 = px.bar(
                    gs_df.sort_values("POINTS", ascending=False),
                    x="TEAM_NAME", y="POINTS",
                    color="GROUP_NAME" if "GROUP_NAME" in gs_df.columns else None,
                    labels={"TEAM_NAME": "", "POINTS": "Points", "GROUP_NAME": "Group"},
                    title=f"{year_int} group stage points",
                )
                fig3.update_traces(marker_line_width=0)
                fig3.update_layout(**plotly_layout(height=380,
                                   xaxis=dict(tickangle=-45)))
                st.plotly_chart(fig3, use_container_width=True)

# ============================================================
# TAB 4 — KNOCKOUT
# ============================================================
with tabs[3]:
    if year_int is None:
        st.info("Select a specific tournament year to view knockout results.")
    else:
        ko_df = load_knockout(year_int)

        if ko_df.empty:
            st.warning(f"No knockout data for {year_int}.")
        else:
            section(f"{year_int} Knockout Stage")

            # Order stages logically
            stage_order = [
                "Round of 16", "Quarter-finals", "Semi-finals",
                "Third-place play-off", "Third place play-off",
                "Final",
            ]

            stages_present = ko_df["STAGE_NAME"].dropna().unique() if "STAGE_NAME" in ko_df.columns else []
            ordered_stages = [s for s in stage_order if s in stages_present]
            remaining      = [s for s in stages_present if s not in stage_order]
            ordered_stages = ordered_stages + remaining

            for stage in ordered_stages:
                stage_df = ko_df[ko_df["STAGE_NAME"] == stage].copy()

                display_cols = [c for c in [
                    "HOME_TEAM_NAME", "HOME_TEAM_SCORE", "AWAY_TEAM_SCORE", "AWAY_TEAM_NAME",
                    "EXTRA_TIME", "PENALTY_SHOOTOUT", "SCORE_PENALTIES",
                ] if c in stage_df.columns]

                section(stage)
                for _, row in stage_df[display_cols].iterrows():
                    ht  = row.get("HOME_TEAM_NAME", "")
                    hs  = row.get("HOME_TEAM_SCORE", "")
                    as_ = row.get("AWAY_TEAM_SCORE", "")
                    at  = row.get("AWAY_TEAM_NAME", "")
                    pen = row.get("SCORE_PENALTIES", "")

                    try:
                        winner = ht if int(hs) > int(as_) else at
                    except Exception:
                        winner = ""

                    pen_label = f"  *(pen. {pen})*" if pen and str(pen).strip() not in ("", "nan") else ""
                    score_label = f"{hs} – {as_}"

                    c1, c2, c3 = st.columns([3, 1, 3])
                    c1.markdown(
                        f"<div style='text-align:right; font-weight:{'800' if winner==ht else '400'}; "
                        f"color:{'#FFFFFF' if winner==ht else '#718096'}'>{ht}</div>",
                        unsafe_allow_html=True,
                    )
                    c2.markdown(
                        f"<div style='text-align:center; font-weight:700; color:#C8102E; font-size:1.1rem'>"
                        f"{score_label}{pen_label}</div>",
                        unsafe_allow_html=True,
                    )
                    c3.markdown(
                        f"<div style='font-weight:{'800' if winner==at else '400'}; "
                        f"color:{'#FFFFFF' if winner==at else '#718096'}'>{at}</div>",
                        unsafe_allow_html=True,
                    )

# ============================================================
# TAB 5 — TOP SCORERS
# ============================================================
with tabs[4]:
    section("Top goal scorers of all time")

    scorers_df = load_top_scorers()

    if scorers_df.empty:
        st.info("No scorer data available.")
    else:
        if year_int:
            # Per-tournament top scorers from goals table
            goals_year = load_goals(year_int)
            if not goals_year.empty and "PLAYER_NAME" in goals_year.columns:
                excl = goals_year["OWN_GOAL"].astype(str).str.lower().isin(["true", "1", "yes"]) if "OWN_GOAL" in goals_year.columns else pd.Series([False] * len(goals_year))
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
                        x="GOALS", y="PLAYER_NAME",
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

        # All-time chart (always shown)
        section("All-time top scorers")
        left2, right2 = st.columns([2, 3])
        with left2:
            styled_table(scorers_df.head(30), height=600)
        with right2:
            top20 = scorers_df.head(20).sort_values("GOALS")
            colors = [GOLD if i == len(top20) - 1 else SILVER if i == len(top20) - 2 else BRONZE if i == len(top20) - 3 else PRIMARY_COLOR
                      for i in range(len(top20))]
            fig5 = go.Figure(go.Bar(
                x=top20["GOALS"],
                y=top20["PLAYER_NAME"],
                orientation="h",
                marker_color=colors,
                text=top20["GOALS"],
                textposition="outside",
            ))
            fig5.update_layout(**plotly_layout(height=600, title="All-time top 20 scorers",
                               xaxis=dict(title="Career goals")))
            st.plotly_chart(fig5, use_container_width=True)

# ============================================================
# TAB 6 — TEAM STATS
# ============================================================
with tabs[5]:
    section("All-time team statistics")

    teams_df = load_top_scoring_teams()

    if teams_df.empty:
        st.info("No team statistics data available.")
    else:
        # Filter by year if selected
        if year_int:
            matches_yr = load_matches(year_int)
            if not matches_yr.empty:
                teams_in_year = set()
                for col in ("HOME_TEAM_NAME", "AWAY_TEAM_NAME"):
                    if col in matches_yr.columns:
                        teams_in_year.update(matches_yr[col].dropna().tolist())
                if "TEAM_NAME" in teams_df.columns:
                    teams_df = teams_df[teams_df["TEAM_NAME"].isin(teams_in_year)]

        # KPI row
        if not teams_df.empty:
            c1, c2, c3, c4 = st.columns(4)
            top_team = teams_df.iloc[0]["TEAM_NAME"] if "TEAM_NAME" in teams_df.columns else "—"
            top_goals = teams_df.iloc[0]["GOALS_FOR"] if "GOALS_FOR" in teams_df.columns else "—"
            c1.markdown(kpi(len(teams_df), "Teams"), unsafe_allow_html=True)
            c2.markdown(kpi(top_team, "Most goals scored"), unsafe_allow_html=True)
            c3.markdown(kpi(int(top_goals) if top_goals != "—" else "—", f"Goals ({top_team})"), unsafe_allow_html=True)
            if "WINS" in teams_df.columns:
                top_wins_team = teams_df.sort_values("WINS", ascending=False).iloc[0]
                c4.markdown(kpi(int(top_wins_team["WINS"]), f"Most wins ({top_wins_team['TEAM_NAME']})"), unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        left, right = st.columns([2, 3])

        with left:
            display_cols = [c for c in [
                "RANK", "TEAM_NAME", "PLAYED", "WINS", "DRAWS", "LOSSES",
                "GOALS_FOR", "GOALS_AGAINST", "GOAL_DIFFERENCE",
            ] if c in teams_df.columns]
            styled_table(teams_df[display_cols].head(30), height=600)

        with right:
            if "GOALS_FOR" in teams_df.columns and "TEAM_NAME" in teams_df.columns:
                top15 = teams_df.head(15).copy()

                fig6a = px.bar(
                    top15,
                    x="TEAM_NAME", y=["GOALS_FOR", "GOALS_AGAINST"] if "GOALS_AGAINST" in top15.columns else ["GOALS_FOR"],
                    barmode="group",
                    color_discrete_map={"GOALS_FOR": PRIMARY_COLOR, "GOALS_AGAINST": "#4A5568"},
                    labels={"TEAM_NAME": "", "value": "Goals", "variable": ""},
                    title="Goals scored vs conceded — top 15 teams",
                )
                fig6a.update_traces(marker_line_width=0)
                fig6a.update_layout(**plotly_layout(height=340,
                                    xaxis=dict(tickangle=-35)))
                st.plotly_chart(fig6a, use_container_width=True)

            if "WINS" in teams_df.columns:
                top10 = teams_df.head(10).copy()
                fig6b = px.bar(
                    top10,
                    x="TEAM_NAME",
                    y=["WINS", "DRAWS", "LOSSES"],
                    barmode="stack",
                    color_discrete_map={
                        "WINS":   PRIMARY_COLOR,
                        "DRAWS":  "#F6AD55",
                        "LOSSES": "#4A5568",
                    },
                    labels={"TEAM_NAME": "", "value": "Matches", "variable": "Result"},
                    title="Win / draw / loss breakdown — top 10 teams",
                )
                fig6b.update_traces(marker_line_width=0)
                fig6b.update_layout(**plotly_layout(height=290,
                                    xaxis=dict(tickangle=-30)))
                st.plotly_chart(fig6b, use_container_width=True)

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.divider()
st.markdown(
    "<div style='text-align:center; color:#4A5568; font-size:0.78rem; padding:0.5rem 0'>"
    "Data sources: "
    "<a href='https://github.com/jfjelstul/worldcup' style='color:#718096'>jfjelstul/worldcup</a>"
    " · "
    "<a href='https://github.com/openfootball/world-cup' style='color:#718096'>openfootball/world-cup</a>"
    "</div>",
    unsafe_allow_html=True,
)
