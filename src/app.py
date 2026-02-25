"""
app.py
──────
Streamlit dashboard — Seattle Sounders FC Analytics 2026.

Data sources:
  data/gold/sounders_timeseries.csv    — per-match player records
  data/processed/league_benchmark.json — MLS positional benchmarks
  data/processed/match_schedule.json   — season fixture list

Run:
  .venv/bin/streamlit run src/app.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from scipy.stats import norm

# ── src/ imports ──────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
from api_client import ESPNApiClient
from config import POSITION_GROUPS, POSITION_STATS, SOUNDERS_ID_MLS

# ── Paths ─────────────────────────────────────────────────────────────────────
_ROOT           = Path(__file__).parent.parent
TIMESERIES_PATH = _ROOT / "data" / "gold"      / "sounders_timeseries.csv"
BENCHMARK_PATH  = _ROOT / "data" / "processed" / "league_benchmark.json"
SCHEDULE_PATH   = _ROOT / "data" / "processed" / "match_schedule.json"
STANDINGS_PATH  = _ROOT / "data" / "processed" / "standings.json"

# ── Brand colours ─────────────────────────────────────────────────────────────
RAVE_GREEN    = "#5B8C1E"   # slightly lightened for dark-bg readability
CASCADE_SHALE = "#1C1A1B"
SOUNDER_BLUE  = "#003087"   # Pantone 286 — Sounders official secondary blue
CARD_BG       = "#252225"
BORDER        = "#3a373a"
GOLD          = "#F0B429"

# ── Position → primary KPI shown on player card ───────────────────────────────
PRIMARY_KPI: dict[str, str] = {
    "GK": "goalkeeper_saves",
    "CB": "defensive_clearances",
    "FB": "crosses_from_play_successful",
    "DM": "interceptions_sum",
    "CM": "assists",
    "AM": "chances",
    "FW": "xG",
}

KPI_LABEL: dict[str, str] = {
    "goalkeeper_saves":            "Saves",
    "defensive_clearances":        "Clearances",
    "crosses_from_play_successful":"Crosses",
    "interceptions_sum":           "Interceptions",
    "assists":                     "Assists",
    "chances":                     "Chances Created",
    "xG":                          "xG",
}

# Five stats shown in the player detail dialog, ordered by importance per position.
# All keys must be present in the gold CSV (ALL_STAT_KEYS in config.py).
PLAYER_DETAIL_STATS: dict[str, list[tuple[str, str]]] = {
    "GK": [
        ("goalkeeper_saves",             "Saves"),
        ("goals_conceded",               "Goals Conceded"),
        ("clean_sheets",                 "Clean Sheet"),
        ("passes_conversion_rate",       "Pass Accuracy"),
        ("ball_control_phases",          "Touches"),
    ],
    "CB": [
        ("defensive_clearances",         "Clearances"),
        ("passes_conversion_rate",       "Pass Accuracy"),
        ("ball_control_phases",          "Touches"),
        ("fouls_against_opponent",       "Fouls Won"),
        ("goals",                        "Goals"),
    ],
    "FB": [
        ("crosses_from_play_successful", "Crosses"),
        ("assists",                      "Assists"),
        ("passes_conversion_rate",       "Pass Accuracy"),
        ("defensive_clearances",         "Clearances"),
        ("ball_control_phases",          "Touches"),
    ],
    "DM": [
        ("ball_control_phases",          "Touches"),
        ("passes_conversion_rate",       "Pass Accuracy"),
        ("fouls_against_opponent",       "Fouls Won"),
        ("chances",                      "Chances Created"),
        ("assists",                      "Assists"),
    ],
    "CM": [
        ("passes_conversion_rate",       "Pass Accuracy"),
        ("ball_control_phases",          "Touches"),
        ("chances",                      "Chances Created"),
        ("assists",                      "Assists"),
        ("goals",                        "Goals"),
    ],
    "AM": [
        ("chances",                      "Chances Created"),
        ("assists",                      "Assists"),
        ("goals",                        "Goals"),
        ("shots_on_target",              "Shots on Target"),
        ("ball_control_phases",          "Touches"),
    ],
    "FW": [
        ("goals",                        "Goals"),
        ("xG",                           "xG"),
        ("shots_on_target",              "Shots on Target"),
        ("assists",                      "Assists"),
        ("chances",                      "Chances Created"),
    ],
}

# Human-readable labels for stat keys used in the Z-score weight tooltip
_WEIGHT_LABEL: dict[str, str] = {
    "goalkeeper_saves":             "Saves",
    "clean_sheets":                 "Clean Sheets",
    "goals_conceded":               "Goals Conceded",
    "defensive_clearances":         "Clearances",
    "passes_conversion_rate":       "Pass Acc",
    "ball_control_phases":          "Touches",
    "crosses_from_play_successful": "Crosses",
    "assists":                      "Assists",
    "fouls_against_opponent":       "Fouls Won",
    "chances":                      "Chances",
    "goals":                        "Goals",
    "shots_on_target":              "SOT",
    "xG":                           "xG",
}

STAT_CATEGORIES: dict[str, list[str]] = {
    "All":        [],
    "Attacking":  ["goals", "xG", "shots_on_target", "assists", "chances"],
    "Defensive":  ["defensive_clearances", "interceptions_sum",
                   "tackling_games_air_won", "goalkeeper_saves",
                   "goals_conceded", "fouls_against_opponent"],
    "Possession": ["passes_conversion_rate", "ball_control_phases",
                   "crosses_from_play_successful"],
}

# ─────────────────────────────────────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title = "Sounders Analytics 2026",
    page_icon  = "⚽",
    layout     = "wide",
    initial_sidebar_state = "expanded",
)

st.markdown(f"""
<style>
  .stApp {{ background-color:{CASCADE_SHALE}; color:#EBEBEB; }}
  section[data-testid="stSidebar"] {{ background-color:#161416; }}
  section[data-testid="stSidebar"] * {{ color:#EBEBEB !important; }}
  h1,h2,h3,h4 {{ color:{RAVE_GREEN} !important; }}

  /* player cards */
  .pc {{
    background:{CARD_BG}; border:1px solid {RAVE_GREEN};
    border-radius:10px; padding:14px 10px; text-align:center;
    height:160px; display:flex; flex-direction:column;
    justify-content:center; gap:4px;
  }}
  .pc-name  {{ font-weight:700; font-size:.92em; color:#fff; }}
  .pc-pos   {{ font-size:.72em; color:#888; letter-spacing:.04em; }}
  .pc-kpi   {{ font-size:1.55em; font-weight:700; color:{GOLD}; }}
  .pc-klbl  {{ font-size:.68em; color:#aaa; }}
  .b-g {{ background:{RAVE_GREEN}; color:#fff; border-radius:10px;
          padding:1px 7px; font-size:.72em; display:inline-block; }}
  .b-y {{ background:#8B6914; color:#fff; border-radius:10px;
          padding:1px 7px; font-size:.72em; display:inline-block; }}
  .b-r {{ background:#8B1A1A; color:#fff; border-radius:10px;
          padding:1px 7px; font-size:.72em; display:inline-block; }}

  /* metric cards */
  [data-testid="metric-container"] {{
    background:{CARD_BG}; border-radius:10px;
    padding:18px 20px; border-left:4px solid {RAVE_GREEN};
  }}

  /* section rule */
  .srule {{ border-top:1px solid {BORDER}; margin:24px 0 16px 0; }}

  /* no-data placeholder */
  .nodata {{
    background:{CARD_BG}; border:1px dashed {BORDER}; border-radius:10px;
    padding:40px; text-align:center; color:#666;
  }}

  /* Formation pitch panel */
  .fp-pitch {{
    background: linear-gradient(180deg, {SOUNDER_BLUE} 0%, #00205e 50%, {SOUNDER_BLUE} 100%);
    border:2px solid rgba(255,255,255,0.12);
    border-radius:12px;
    padding:18px 8px;
    display:flex;
    flex-direction:column;
    gap:14px;
    min-height:500px;
  }}
  .fp-row {{
    display:flex;
    justify-content:space-around;
    align-items:center;
    flex-wrap:wrap;
    gap:4px;
  }}
  .fp-player {{ display:flex; flex-direction:column; align-items:center; }}
  .fp-bubble {{
    width:58px; height:58px;
    border-radius:50%;
    border:2px solid #555;
    background:rgba(20,18,19,0.88);
    display:flex;
    flex-direction:column;
    align-items:center;
    justify-content:center;
    gap:1px;
  }}
  .fp-pos  {{ font-size:.54em; color:#aaa; letter-spacing:.05em; text-transform:uppercase; }}
  .fp-name {{ font-size:.58em; font-weight:700; color:#fff; text-align:center;
               line-height:1.15; padding:0 3px; }}
  .fp-kpi  {{ font-size:.64em; font-weight:700; color:{GOLD}; }}
  .fp-match-label {{
    font-size:.72em; color:#aaa; text-align:center;
    margin-bottom:4px; font-style:italic;
  }}
  .fp-icons {{
    font-size:.65em; min-height:.9em; text-align:center;
    line-height:1.3; margin-top:2px; letter-spacing:1px;
  }}

  /* Substitution timeline */
  .fp-timeline {{
    border-top:1px solid rgba(255,255,255,0.1);
    margin-top:6px; padding-top:8px;
    display:flex; flex-direction:column; gap:4px;
  }}
  .fp-sub-row {{
    display:flex; align-items:center; gap:6px;
    font-size:.68em; padding:2px 4px;
  }}
  .fp-sub-min  {{ color:{GOLD}; font-weight:700; min-width:28px; }}
  .fp-sub-in   {{ color:#7dcc7d; }}
  .fp-sub-out  {{ color:#cc7d7d; }}
  .fp-sub-sep  {{ color:#555; }}

  /* Formation player detail buttons */
  div[data-testid="stHorizontalBlock"] button[kind="secondary"] {{
    background: rgba(91,140,30,0.12);
    border: 1px solid {RAVE_GREEN};
    color: #ddd;
    font-size: .72em;
    padding: 3px 4px;
    height: 28px;
    border-radius: 6px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }}
  div[data-testid="stHorizontalBlock"] button[kind="secondary"]:hover {{
    background: rgba(91,140,30,0.30);
    color: #fff;
    border-color: {GOLD};
  }}

  /* Upcoming fixtures */
  .fx-card {{
    display:flex; align-items:center; gap:10px;
    padding:7px 10px; margin-bottom:5px;
    background:{CARD_BG}; border-radius:8px;
    border-left:3px solid {RAVE_GREEN};
  }}
  .fx-date {{
    font-size:.72em; color:{GOLD}; font-weight:700;
    min-width:48px; text-align:center; line-height:1.2;
  }}
  .fx-teams {{
    flex:1; font-size:.78em; color:#fff; line-height:1.3;
  }}
  .fx-comp {{
    font-size:.62em; color:#888; background:rgba(255,255,255,0.07);
    border-radius:4px; padding:2px 5px; white-space:nowrap;
  }}

  /* Western Conference standings */
  .st-tbl {{
    width:100%; border-collapse:collapse;
    font-size:.78em; margin-top:8px;
  }}
  .st-tbl th {{
    color:#aaa; font-weight:600; text-align:center;
    padding:4px 6px; border-bottom:1px solid {BORDER};
  }}
  .st-tbl th:first-child {{ text-align:left; }}
  .st-tbl td {{
    padding:5px 6px; text-align:center;
    border-bottom:1px solid rgba(255,255,255,0.04);
    color:#ddd;
  }}
  .st-tbl td:first-child {{ text-align:left; }}
  .st-row-sounders td {{
    background:rgba(91,140,30,0.22);
    color:#fff; font-weight:700;
    border-left:3px solid {RAVE_GREEN};
  }}
  .st-last-synced {{
    font-size:.65em; color:#555; text-align:right; margin-top:4px;
  }}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# Cached data loaders
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_timeseries() -> pd.DataFrame:
    if not TIMESERIES_PATH.exists():
        return pd.DataFrame()
    df = pd.read_csv(TIMESERIES_PATH, dtype={"match_id": str, "player_id": str})
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    skip = {"match_id", "timestamp", "player_id", "player_name",
            "position_raw", "position_group"}
    for col in df.columns:
        if col not in skip:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


@st.cache_data(ttl=3600)
def load_benchmark() -> dict:
    if not BENCHMARK_PATH.exists():
        return {}
    with open(BENCHMARK_PATH) as f:
        raw = json.load(f)
    return {g: {s: tuple(v) for s, v in stats.items()} for g, stats in raw.items()}


@st.cache_data(ttl=3600)
def load_schedule() -> list[dict]:
    if not SCHEDULE_PATH.exists():
        return []
    with open(SCHEDULE_PATH) as f:
        return json.load(f).get("matches", [])


@st.cache_data(ttl=3600)
def load_club_attacking_stats() -> list[dict]:
    """
    Fetch season attacking stats for all MLS clubs.
    Returns raw list of club dicts from stats-api.mlssoccer.com.
    Key fields: team_id, team_name, goals, shots_at_goal_sum,
                shots_on_target, xG, xG_efficiency.
    """
    try:
        from api_client import MLSApiClient
        from config import SEASON
        mls = MLSApiClient()
        sid = mls.get_season_id(SEASON)
        if not sid:
            return []
        return mls.get_club_stats(sid)
    except Exception:
        return []


@st.cache_data(ttl=3600)
def load_standings() -> dict:
    """Load Western Conference standings from JSON cache written by Airflow."""
    if not STANDINGS_PATH.exists():
        return {}
    with open(STANDINGS_PATH) as f:
        return json.load(f)


@st.cache_data(ttl=3600, show_spinner="Computing league Z-scores…")
def load_league_player_zscores(latest_match_id: str | None = None) -> pd.DataFrame:
    """
    Fetch all MLS player season stats and compute positional Z-scores
    for league-wide outlier analysis. Cached for 1 hour.

    Position classification strategy (in priority order):
      1. ESPN roster from latest_match_id  — reliable per-match position codes
         for all players who appeared (both teams, ~22 players).
         Codes are mapped through POSITION_MAP (e.g. LM → CM, CD-L → CB).
      2. infer_position_from_stats()       — stat-profile heuristic for everyone else.

    Returns columns: player_name, team_id, position_group,
                     composite_zscore, minutes, is_sfc
    """
    try:
        from api_client import MLSApiClient
        from analytics_engine import AnalyticsEngine, infer_position_from_stats
        from config import SEASON, POSITION_MAP

        # ── 1. Build ESPN position lookup from the latest match ───────────────
        # ESPN gives us reliable, match-specific position codes for both teams.
        # This covers ~22 players and takes priority over stat-based inference.
        espn_pos_lookup: dict[str, str] = {}   # player_id → position_group
        if latest_match_id:
            espn_data = load_espn_match_data(latest_match_id)
            for p in espn_data.get("roster", []):
                code = (p.get("pos_code") or "").upper()
                grp  = POSITION_MAP.get(code)
                if grp and p.get("player_id"):
                    espn_pos_lookup[p["player_id"]] = grp

        # ── 2. Fetch all MLS player season stats (paginated) ──────────────────
        mls = MLSApiClient()
        sid = mls.get_season_id(SEASON)
        if not sid:
            sid = mls.get_season_id(SEASON - 1)
        if not sid:
            return pd.DataFrame()

        all_players = mls.get_all_player_stats(sid)
        if not all_players:
            return pd.DataFrame()

        benchmark = load_benchmark()
        if not benchmark:
            return pd.DataFrame()

        # ── 3. Compute Z-scores with ESPN-first position classification ───────
        engine = AnalyticsEngine()
        rows = []
        for p in all_players:
            if p.minutes < 90:
                continue

            # ESPN position code takes priority; fall back to stat inference
            pos_group = espn_pos_lookup.get(p.player_id)
            if pos_group is None:
                pos_group = POSITION_MAP.get(p.position_raw or "", None)
            if pos_group is None:
                pos_group = infer_position_from_stats(p.stats, p.is_gk)

            z = engine.compute_zscore(p, benchmark)
            if z is None:
                continue
            rows.append({
                "player_name":      p.player_name,
                "team_id":          p.team_id,
                "position_group":   pos_group or "CM",
                "composite_zscore": round(z, 3),
                "minutes":          int(round(p.minutes)),
                "is_sfc":           p.team_id == SOUNDERS_ID_MLS,
            })

        return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


# ESPN position code → formation row (0 = attack/top, 5 = GK/bottom)
_ESPN_POS_ROW: dict[str, int] = {
    "F":  0, "CF": 0, "ST": 0, "FW": 0, "LW": 0, "RW": 0,
    "AM": 1, "AM-R": 1, "AM-L": 1, "CAM": 1, "SS": 1,
    "LM": 2, "RM":  2, "CM":  2, "MC": 2,
    "DM": 3, "CDM": 3, "DCM": 3,
    "RB": 4, "LB":  4, "CB":  4, "CD-R": 4, "CD-L": 4,
    "LCB": 4, "RCB": 4, "LWB": 4, "RWB": 4,
    "G":  5, "GK":  5,
}

# Left-to-right ordering within a formation row (lower = further left).
# ESPN formationPlace numbers do NOT reliably encode lateral position,
# so we derive side from the position code instead.
_POS_SIDE: dict[str, int] = {
    # Far left (0) — wide left players
    "LB":  0, "LWB": 0, "LM": 0, "LW": 0,
    # Left-center (1) — left-leaning central positions
    "LCB": 1, "CD-L": 1, "WB-L": 1, "AM-L": 1, "LAM": 1,
    # Center (2) — pure center positions
    "GK":  2, "G":   2,
    "CB":  2, "DM":  2, "CDM": 2, "DCM": 2,
    "CM":  2, "MC":  2, "AM":  2, "CAM": 2, "SS": 2,
    "CF":  2, "ST":  2, "FW":  2, "F":   2,
    # Right-center (3) — right-leaning central positions
    "RCB": 3, "CD-R": 3, "WB-R": 3, "AM-R": 3, "RAM": 3,
    # Far right (4) — wide right players
    "RB":  4, "RWB": 4, "RM": 4, "RW": 4,
}


def _pos_side(pos_code: str) -> int:
    """Left-to-right weight for a position code (0 = left, 2 = center, 4 = right)."""
    return _POS_SIDE.get((pos_code or "").upper(), 2)


def _parse_sub_minute(display: str) -> int:
    """'8'' → 8, '45'+13'' → 58, '90'+5'' → 95"""
    nums = re.findall(r"\d+", display)
    return sum(int(n) for n in nums) if nums else 90


@st.cache_data(ttl=3600)
def load_espn_match_data(match_id: str) -> dict:
    """Fetch Sounders roster, formation string, and sub events from ESPN."""
    empty: dict = {"roster": [], "formation": "", "sub_events": []}
    try:
        client  = ESPNApiClient()
        summary = client.get_match_summary(match_id)
        if not summary:
            return empty
        sounders = next(
            (t for t in summary.get("rosters", [])
             if "Seattle" in t.get("team", {}).get("displayName", "")),
            None,
        )
        if not sounders:
            return empty

        formation = sounders.get("formation", "")
        roster: list[dict] = []
        for p in sounders.get("roster", []):
            athlete   = p.get("athlete", {})
            pos       = (p.get("position") or {}).get("abbreviation", "")
            raw_stats = {s["name"]: s.get("value", 0)
                         for s in p.get("stats", []) if "name" in s}
            roster.append({
                "player_id":       str(athlete.get("id", "")),
                "name":            athlete.get("displayName", ""),
                "pos_code":        pos,
                "formation_place": int(p.get("formationPlace") or 99),
                "starter":         bool(p.get("starter", False)),
                "subbed_out":      bool(p.get("subbedOut", False)),
                "subbed_in":       bool(p.get("subbedIn", False)),
                # ESPN sometimes provides exact minutes — used as fallback
                # when the sub minute can't be matched from keyEvent text
                "espn_minutes":    int(raw_stats.get("minutesPlayed",
                                       raw_stats.get("minsPlayed", 0))),
                "yellow_cards":    int(raw_stats.get("yellowCards", 0)),
                "red_cards":       int(raw_stats.get("redCards", 0)),
                "goals":           int(raw_stats.get("totalGoals", 0)),
                "assists":         int(raw_stats.get("goalAssists", 0)),
            })

        # Parse Sounders substitution events from keyEvents
        sub_events: list[dict] = []
        for ev in summary.get("keyEvents", []):
            if ev.get("type", {}).get("text") != "Substitution":
                continue
            text = ev.get("text", "")
            if "Seattle" not in text or " replaces " not in text:
                continue
            display = ev.get("clock", {}).get("displayValue", "?")
            after_dot  = text.split(". ", 1)[-1] if ". " in text else text
            parts      = after_dot.split(" replaces ", 1)
            player_in  = parts[0].strip()
            player_out = parts[1].strip() if len(parts) > 1 else ""
            # Strip " because of..." injury suffix and trailing punctuation
            if " because" in player_out:
                player_out = player_out.split(" because")[0]
            player_out = player_out.rstrip(".").strip()
            sub_events.append({
                "minute":     _parse_sub_minute(display),
                "display":    display,
                "player_in":  player_in,
                "player_out": player_out,
            })

        return {"roster": roster, "formation": formation, "sub_events": sub_events}
    except Exception:
        return empty


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _pct_badge(z: float | None) -> str:
    if z is None or (isinstance(z, float) and np.isnan(z)):
        return '<span style="color:#666">—</span>'
    pct = int(norm.cdf(float(z)) * 100)
    cls = "b-g" if pct >= 75 else ("b-y" if pct >= 40 else "b-r")
    return f'<span class="{cls}">{pct}th pct</span>'


def _match_labels(df: pd.DataFrame, schedule: list[dict]) -> dict[str, str]:
    sched = {m["id"]: m for m in schedule}
    labels: dict[str, str] = {}
    for mid in sorted(df["match_id"].dropna().unique()):
        m = sched.get(str(mid))
        # Derive date string for display
        ts = df[df["match_id"] == mid]["timestamp"].min()
        date_str = ts.strftime("%b %-d") if pd.notna(ts) else "?"
        if m:
            opp_full = m.get("away", "") if "Seattle" in m.get("home", "") else m.get("home", "")
            # Shorten "Portland Timbers" → "Portland", "LA Galaxy" → "LA Galaxy" etc.
            opp = opp_full.replace(" FC", "").replace(" SC", "").strip() or "?"
            labels[mid] = f"{date_str} — {opp}"
        else:
            labels[mid] = date_str
    return labels


# ─────────────────────────────────────────────────────────────────────────────
# Section renderers
# ─────────────────────────────────────────────────────────────────────────────

def _p90(raw, matches: float) -> float | None:
    """Convert a counting stat to per-90-min rate using matches_played as denominator."""
    if raw is None or not matches:
        return None
    return float(raw) / float(matches)

# Formation row order: top (attack) → bottom (defense)
_FORMATION_ROWS: tuple[tuple[str, ...], ...] = (
    ("FW",),
    ("AM",),
    ("CM",),
    ("DM",),
    ("CB", "FB"),
    ("GK",),
)


def _z_border(z: float | None) -> str:
    try:
        zf = float(z)  # type: ignore[arg-type]
        if np.isnan(zf):
            return "#555"
        return GOLD if zf >= 1.5 else (RAVE_GREEN if zf >= 0 else "#7a2020")
    except (TypeError, ValueError):
        return "#555"


def _fp_bubble(
    name: str,
    pos: str,
    kpi: float | None,
    z: float | None,
    subbed_out: bool = False,
    yellow_cards: int = 0,
    red_cards: int = 0,
    goals: int = 0,
    assists: int = 0,
) -> str:
    last    = name.rsplit(" ", 1)[-1] if " " in name else name
    kpi_str = f"{kpi:.1f}" if kpi is not None else "—"
    border  = _z_border(z)
    icons   = "⚽" * goals + "🅰️" * assists + ("♻️" if subbed_out else "") + "🟨" * yellow_cards + "🟥" * red_cards
    icon_html = f'<div class="fp-icons">{icons}</div>' if icons else '<div class="fp-icons"></div>'
    return (
        f'<div class="fp-player">'
        f'<div class="fp-bubble" style="border-color:{border}">'
        f'<div class="fp-pos">{pos}</div>'
        f'<div class="fp-name">{last}</div>'
        f'<div class="fp-kpi">{kpi_str}</div>'
        f'</div>'
        f'{icon_html}'
        f'</div>'
    )


@st.dialog("Player Stats", width="small")
def _show_player_detail(
    name:      str,
    pos_group: str,
    stats:     dict,
    z:         float | None,
    velocity:  float | None,
    minutes:   float,
) -> None:
    """Modal: up to 5 position-appropriate stats for a single player."""
    # Header row: name + percentile badge
    c_name, c_badge = st.columns([3, 1])
    with c_name:
        st.markdown(f"#### {name}")
        st.caption(f"{pos_group}  ·  {int(minutes)}' played")
    with c_badge:
        if z is not None:
            pct   = int(norm.cdf(float(z)) * 100)
            color = RAVE_GREEN if pct >= 75 else (GOLD if pct >= 40 else "#8B1A1A")
            st.markdown(
                f'<div style="text-align:center;padding-top:14px">'
                f'<span style="background:{color};color:#fff;border-radius:10px;'
                f'padding:3px 9px;font-size:.78em;font-weight:700">{pct}th</span></div>',
                unsafe_allow_html=True,
            )

    st.divider()

    # Position-specific stat rows
    stat_list = PLAYER_DETAIL_STATS.get(pos_group, PLAYER_DETAIL_STATS["CM"])
    for stat_key, stat_label in stat_list:
        raw = stats.get(stat_key)
        try:
            val = float(raw) if raw is not None and not pd.isna(float(raw)) else None
        except (ValueError, TypeError):
            val = None

        sc, sv = st.columns([3, 1])
        with sc:
            st.markdown(
                f'<div style="color:#aaa;font-size:.88em;padding:3px 0">{stat_label}</div>',
                unsafe_allow_html=True,
            )
        with sv:
            if val is None:
                disp = "—"
                color = "#555"
            elif stat_key == "passes_conversion_rate":
                disp  = f"{val:.0%}"
                color = "#fff"
            elif stat_key == "clean_sheets":
                disp  = "Yes" if val > 0 else "No"
                color = RAVE_GREEN if val > 0 else "#aaa"
            elif stat_key == "xG":
                disp  = f"{val:.2f}"
                color = GOLD
            else:
                disp  = f"{val:.0f}"
                color = "#fff"
            st.markdown(
                f'<div style="text-align:right;font-weight:700;color:{color};'
                f'font-size:.95em;padding:3px 0">{disp}</div>',
                unsafe_allow_html=True,
            )

    # Form Velocity footer (only when at least VELOCITY_WINDOW matches stored)
    if velocity is not None:
        st.divider()
        arrow = "▲" if velocity > 0 else "▼"
        color = RAVE_GREEN if velocity > 0 else "#8B1A1A"
        st.markdown(
            f'<div style="text-align:center">'
            f'<div style="color:#888;font-size:.75em;margin-bottom:4px">Form Velocity</div>'
            f'<div style="font-size:1.3em;font-weight:700;color:{color}">'
            f'{arrow} {velocity:+.2f}</div>'
            f'<div style="color:#555;font-size:.7em">vs. last 3 qualifying appearances</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    # Score formula expander — shows position-specific stat weights
    with st.expander("Score formula"):
        weights = POSITION_STATS.get(pos_group, [])
        if weights:
            parts = []
            for stat, w in weights:
                lbl = _WEIGHT_LABEL.get(stat, stat)
                sign = "−" if w < 0 else "×"
                parts.append(f"**{lbl}** {sign}{abs(w):.0%}")
            st.markdown("  ·  ".join(parts))
        st.caption(
            "Composite Z-score = weighted average of per-stat Z-scores "
            "vs. all MLS players at this position with ≥90 min played."
        )


def render_formation_panel(df: pd.DataFrame, schedule: list[dict]) -> None:
    st.markdown(
        f'<h3 style="color:{RAVE_GREEN};margin:0 0 6px 0">Kick-off Lineup</h3>',
        unsafe_allow_html=True,
    )

    if df.empty:
        st.markdown(
            '<div class="nodata" style="min-height:460px">'
            'No match data yet<br><small>Populates after first match</small>'
            '</div>',
            unsafe_allow_html=True,
        )
        return

    latest_match = df.sort_values("timestamp")["match_id"].iloc[-1]
    match_df     = df[df["match_id"] == latest_match]

    labels = _match_labels(df, schedule)
    lbl    = labels.get(latest_match, f"Match {latest_match}")
    st.markdown(f'<div class="fp-match-label">{lbl}</div>', unsafe_allow_html=True)

    # CSV lookup: player_id → z-score and primary KPI
    csv_lookup: dict[str, dict] = {}
    for _, row in match_df.iterrows():
        pg      = str(row.get("position_group", ""))
        kpi_col = PRIMARY_KPI.get(pg, "")
        kpi_v   = row.get(kpi_col)
        z_v     = row.get("composite_zscore")
        csv_lookup[str(row["player_id"])] = {
            "z":   float(z_v)   if z_v  is not None and pd.notna(z_v)  else None,
            "kpi": float(kpi_v) if kpi_v is not None and pd.notna(kpi_v) else None,
        }

    # Full stats lookup for the player detail dialog
    _DIALOG_STAT_KEYS = [
        "assists", "ball_control_phases", "chances", "clean_sheets",
        "crosses_from_play_successful", "defensive_clearances",
        "fouls_against_opponent", "goalkeeper_saves", "goals",
        "goals_conceded", "passes_conversion_rate", "shots_on_target", "xG",
    ]
    player_stats_lookup: dict[str, dict] = {}
    for _, row in match_df.iterrows():
        pid = str(row["player_id"])
        player_stats_lookup[pid] = {
            "name":           str(row.get("player_name", "")),
            "position_group": str(row.get("position_group", "CM")),
            "minutes_played": float(row.get("minutes_played") or 0),
            "composite_zscore": row.get("composite_zscore"),
            "form_velocity":  row.get("form_velocity"),
            **{k: row.get(k) for k in _DIALOG_STAT_KEYS},
        }

    # Fetch ESPN match data (roster + sub events)
    espn = load_espn_match_data(str(latest_match))
    roster     = espn["roster"]
    sub_events = espn["sub_events"]
    starters   = [p for p in roster if p["starter"]]

    if not starters:
        # Fallback: position_group from CSV
        rows_html: list[str] = []
        for row_groups in _FORMATION_ROWS:
            bubbles = ""
            for group in row_groups:
                for _, row in match_df[match_df["position_group"] == group].iterrows():
                    csv = csv_lookup.get(str(row["player_id"]), {})
                    bubbles += _fp_bubble(str(row["player_name"]), group,
                                         csv.get("kpi"), csv.get("z"))
            if bubbles:
                rows_html.append(f'<div class="fp-row">{bubbles}</div>')
        st.markdown(f'<div class="fp-pitch">{"".join(rows_html)}</div>',
                    unsafe_allow_html=True)
        # Fallback player buttons
        st.markdown(
            '<div style="font-size:.7em;color:#555;text-align:center;margin:6px 0 4px">tap for stats</div>',
            unsafe_allow_html=True,
        )
        for row_groups in _FORMATION_ROWS:
            row_pids = []
            for group in row_groups:
                grp_rows = match_df[
                    (match_df["position_group"] == group) &
                    (match_df["minutes_played"] > 0)
                ]
                row_pids.extend(str(r["player_id"]) for _, r in grp_rows.iterrows())
            if not row_pids:
                continue
            btn_cols = st.columns(len(row_pids))
            for pid, col in zip(row_pids, btn_cols):
                pdata = player_stats_lookup.get(pid, {})
                short = pdata.get("name", "?").rsplit(" ", 1)[-1]
                with col:
                    if st.button(short, key=f"fpb_{pid}", use_container_width=True):
                        _show_player_detail(
                            name      = pdata.get("name", "?"),
                            pos_group = pdata.get("position_group", "CM"),
                            stats     = pdata,
                            z         = csv_lookup.get(pid, {}).get("z"),
                            velocity  = pdata.get("form_velocity"),
                            minutes   = pdata.get("minutes_played", 0),
                        )
        # Fallback bench: 0-minute players from CSV
        bench_df = match_df[match_df["minutes_played"] == 0]
        if not bench_df.empty:
            st.markdown(
                '<div style="color:#555;font-size:.7em;letter-spacing:.12em;'
                'text-align:center;border-top:1px solid #2a2a2a;'
                'margin:10px 0 5px;padding-top:8px">BENCH</div>',
                unsafe_allow_html=True,
            )
            bench_pids = [str(r["player_id"]) for _, r in bench_df.iterrows()]
            for chunk_start in range(0, len(bench_pids), 5):
                chunk  = bench_pids[chunk_start:chunk_start + 5]
                b_cols = st.columns(len(chunk))
                for pid, col in zip(chunk, b_cols):
                    pdata = player_stats_lookup.get(pid, {})
                    short = pdata.get("name", "?").rsplit(" ", 1)[-1]
                    with col:
                        if st.button(short, key=f"bench_{pid}", use_container_width=True):
                            _show_player_detail(
                                name      = pdata.get("name", "?"),
                                pos_group = pdata.get("position_group", "CM"),
                                stats     = pdata,
                                z         = csv_lookup.get(pid, {}).get("z"),
                                velocity  = pdata.get("form_velocity"),
                                minutes   = pdata.get("minutes_played", 0),
                            )
        return

    # ── Compute actual minutes for each player from sub events ────────────────
    minute_subbed_off: dict[str, int] = {}   # player_name → minute left
    minute_subbed_on:  dict[str, int] = {}   # player_name → minute came on
    for ev in sub_events:
        minute_subbed_off[ev["player_out"]] = ev["minute"]
        minute_subbed_on[ev["player_in"]]   = ev["minute"]

    by_name = {p["name"]: p for p in roster}

    def _minutes(name: str) -> int:
        p = by_name.get(name)
        if p is None:
            return 0
        if p["starter"]:
            return minute_subbed_off.get(name, 90)
        if p["subbed_in"]:
            minute_on = minute_subbed_on.get(name)
            if minute_on is not None:
                return 90 - minute_on
            # Sub minute wasn't found in keyEvents — ESPN displayName likely
            # differs from the name in the event text (e.g. accent stripping,
            # alias vs. full name).  Use ESPN's own minutesPlayed stat if
            # present; otherwise fall back to 30 as a conservative estimate
            # for an untracked late sub rather than silently returning 0.
            espn_min = p.get("espn_minutes", 0)
            return espn_min if espn_min > 0 else 30
        return 0

    # Override CSV minutes_played with ESPN sub-event minutes (accurate per-match).
    # The CSV gets minutes from the MLS Stats API which returns season totals or
    # stale per-match figures — ESPN sub events are the source of truth.
    for p in roster:
        pid = p["player_id"]
        espn_min = float(_minutes(p["name"]))
        if pid in player_stats_lookup:
            player_stats_lookup[pid]["minutes_played"] = espn_min
        else:
            player_stats_lookup[pid] = {
                "name":             p["name"],
                "position_group":   "CM",
                "minutes_played":   espn_min,
                "composite_zscore": None,
                "form_velocity":    None,
                **{k: None for k in _DIALOG_STAT_KEYS},
            }

    # ── Build formation slots: formation_place → player with most minutes ─────
    # Start: each slot gets the starter
    slots: dict[int, dict] = {p["formation_place"]: p for p in starters}

    # For each sub event, check if the sub played more than the starter they replaced
    for ev in sub_events:
        starter = by_name.get(ev["player_out"])
        sub     = by_name.get(ev["player_in"])
        if starter is None or sub is None:
            continue
        fp = starter["formation_place"]
        if _minutes(sub["name"]) > _minutes(starter["name"]):
            # Show the sub at this slot; inherit starter's pos_code for row grouping
            slots[fp] = {**sub, "_pos_for_row": starter["pos_code"]}

    # ── Render formation rows ─────────────────────────────────────────────────
    row_buckets: dict[int, list[dict]] = {}
    for fp, p in slots.items():
        pos_for_row = p.get("_pos_for_row", p["pos_code"])
        row_idx     = _ESPN_POS_ROW.get(pos_for_row, 2)
        row_buckets.setdefault(row_idx, []).append({**p, "_fp": fp,
                                                    "_row_pos": pos_for_row})

    pitch_rows: list[str] = []
    for row_idx in sorted(row_buckets):
        row_players = sorted(row_buckets[row_idx],
                             key=lambda p: (_pos_side(p.get("_row_pos", p.get("pos_code", ""))),
                                            p["_fp"]))
        bubbles = ""
        for p in row_players:
            csv      = csv_lookup.get(p["player_id"], {})
            disp_pos = p["pos_code"] if p["pos_code"] not in ("SUB", "") else p["_row_pos"]
            bubbles += _fp_bubble(
                name         = p["name"],
                pos          = disp_pos,
                kpi          = csv.get("kpi"),
                z            = csv.get("z"),
                subbed_out   = p.get("subbed_out", False),
                yellow_cards = p.get("yellow_cards", 0),
                red_cards    = p.get("red_cards", 0),
                goals        = p.get("goals", 0),
                assists      = p.get("assists", 0),
            )
        pitch_rows.append(f'<div class="fp-row">{bubbles}</div>')

    # ── Substitution timeline ─────────────────────────────────────────────────
    timeline_html = ""
    if sub_events:
        rows_t = ""
        for ev in sub_events:
            pin  = ev["player_in"].rsplit(" ", 1)[-1]
            pout = ev["player_out"].rsplit(" ", 1)[-1]
            rows_t += (
                f'<div class="fp-sub-row">'
                f'<span class="fp-sub-min">{ev["display"]}</span>'
                f'<span class="fp-sub-in">↑ {pin}</span>'
                f'<span class="fp-sub-sep">/</span>'
                f'<span class="fp-sub-out">↓ {pout}</span>'
                f'</div>'
            )
        timeline_html = f'<div class="fp-timeline">{rows_t}</div>'

    st.markdown(
        f'<div class="fp-pitch">{"".join(pitch_rows)}{timeline_html}</div>',
        unsafe_allow_html=True,
    )

    # ── Player detail buttons — formation-matched layout ─────────────────────
    st.markdown(
        '<div style="font-size:.7em;color:#555;text-align:center;margin:6px 0 4px">tap for stats</div>',
        unsafe_allow_html=True,
    )
    for row_idx in sorted(row_buckets):
        row_players = sorted(row_buckets[row_idx],
                             key=lambda p: (_pos_side(p.get("_row_pos", p.get("pos_code", ""))),
                                            p["_fp"]))
        n = len(row_players)
        if n == 1:
            all_cols = st.columns([1.5, 1, 1.5])
            btn_cols = [all_cols[1]]
        elif n == 3:
            all_cols = st.columns([0.5, 1, 1, 1, 0.5])
            btn_cols = list(all_cols[1:4])
        else:
            btn_cols = st.columns(n)

        for p, col in zip(row_players, btn_cols):
            pid   = p["player_id"]
            pdata = player_stats_lookup.get(pid, {})
            short = p["name"].rsplit(" ", 1)[-1]
            with col:
                if st.button(short, key=f"fpb_{pid}", use_container_width=True):
                    _show_player_detail(
                        name      = p["name"],
                        pos_group = pdata.get("position_group", "CM"),
                        stats     = pdata,
                        z         = csv_lookup.get(pid, {}).get("z"),
                        velocity  = pdata.get("form_velocity"),
                        minutes   = pdata.get("minutes_played", 0),
                    )

    # ── Bench ─────────────────────────────────────────────────────────────────
    # Bench = everyone not holding a formation slot, regardless of ESPN starter flag.
    # Starters subbed off early (fewer minutes than their replacement) drop here too.
    slot_ids = {p["player_id"] for players in row_buckets.values() for p in players}
    bench = [p for p in roster if p["player_id"] not in slot_ids]
    if bench:
        # Subs who came on first (sorted by minute), then unused subs by name
        bench.sort(key=lambda p: (
            0 if p["name"] in minute_subbed_on else 1,
            minute_subbed_on.get(p["name"], 999),
            p["name"],
        ))
        st.markdown(
            '<div style="color:#555;font-size:.7em;letter-spacing:.12em;'
            'text-align:center;border-top:1px solid #2a2a2a;'
            'margin:10px 0 5px;padding-top:8px">BENCH</div>',
            unsafe_allow_html=True,
        )
        for chunk_start in range(0, len(bench), 5):
            chunk  = bench[chunk_start:chunk_start + 5]
            b_cols = st.columns(len(chunk))
            for p, col in zip(chunk, b_cols):
                pid     = p["player_id"]
                pdata   = player_stats_lookup.get(pid, {})
                short   = p["name"].rsplit(" ", 1)[-1]
                label   = short
                with col:
                    if st.button(label, key=f"bench_{pid}", use_container_width=True):
                        _show_player_detail(
                            name      = p["name"],
                            pos_group = pdata.get("position_group", "CM"),
                            stats     = pdata,
                            z         = csv_lookup.get(pid, {}).get("z"),
                            velocity  = pdata.get("form_velocity"),
                            minutes   = pdata.get("minutes_played", 0),
                        )


def render_attacking_snapshot() -> None:
    """
    4 metric cards — Sounders' attacking stats vs the MLS club table:
    xG  |  Goals  |  Shots on Target  |  Shot Accuracy (SHT%)

    Data source: stats-api.mlssoccer.com/statistics/clubs (club season totals).
    League rank is computed across all clubs with data for that match week.
    """
    st.markdown('<div class="srule"></div>', unsafe_allow_html=True)
    st.subheader("Attacking Snapshot")

    clubs = load_club_attacking_stats()
    if not clubs:
        st.info("Club stats not yet available from MLS API.", icon="📊")
        return

    sfc = next((c for c in clubs if c.get("team_id") == SOUNDERS_ID_MLS), None)
    if sfc is None:
        st.info("Sounders not yet in the MLS club stats table.", icon="📊")
        return

    n = len(clubs)

    def _rank(val: float | None, all_vals: list[float]) -> str:
        """1-based rank among all clubs (higher = better)."""
        if val is None:
            return "—"
        rank = sum(1 for v in all_vals if v > val) + 1
        return f"#{rank} of {n}"

    mp_sfc  = float(sfc.get("matches_played") or 1)
    xg_val  = _p90(sfc.get("xG"),              mp_sfc)
    g_val   = _p90(sfc.get("goals"),            mp_sfc)
    sot_val = _p90(sfc.get("shots_on_target"),  mp_sfc)
    sht_tot = float(sfc.get("shots_at_goal_sum") or 0)
    sht_pct = (float(sfc.get("shots_on_target") or 0) / sht_tot * 100) if sht_tot else None

    def _mp(c): return float(c.get("matches_played") or 1)
    all_xg  = [float(c.get("xG") or 0)            / _mp(c) for c in clubs]
    all_g   = [float(c.get("goals") or 0)          / _mp(c) for c in clubs]
    all_sot = [float(c.get("shots_on_target") or 0)/ _mp(c) for c in clubs]
    all_sht = [
        (float(c.get("shots_on_target") or 0) / float(c.get("shots_at_goal_sum") or 1) * 100)
        if c.get("shots_at_goal_sum") else 0
        for c in clubs
    ]

    st.caption("Counting stats per 90 min")
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.metric(
            label       = "xG p90",
            value       = f"{xg_val:.2f}" if xg_val is not None else "—",
            delta       = _rank(xg_val, all_xg),
            delta_color = "off",
            help        = "Expected goals per 90 min",
        )
    with c2:
        st.metric(
            label       = "Goals p90",
            value       = f"{g_val:.2f}" if g_val is not None else "—",
            delta       = _rank(g_val, all_g),
            delta_color = "off",
        )
    with c3:
        st.metric(
            label       = "SOT p90",
            value       = f"{sot_val:.2f}" if sot_val is not None else "—",
            delta       = _rank(sot_val, all_sot),
            delta_color = "off",
            help        = "Shots on target per 90 min",
        )
    with c4:
        st.metric(
            label       = "SHT%",
            value       = f"{sht_pct:.1f}%" if sht_pct is not None else "—",
            delta       = _rank(sht_pct, all_sht),
            delta_color = "off",
            help        = "Shot accuracy — shots on target ÷ total shots",
        )


def render_creation_snapshot() -> None:
    """4 metric cards — Sounders' chance-creation stats vs the MLS club table."""
    st.markdown('<div class="srule"></div>', unsafe_allow_html=True)
    st.subheader("Creation Snapshot")

    clubs = load_club_attacking_stats()
    if not clubs:
        st.info("Club stats not yet available from MLS API.", icon="📊")
        return

    sfc = next((c for c in clubs if c.get("team_id") == SOUNDERS_ID_MLS), None)
    if sfc is None:
        st.info("Sounders not yet in the MLS club stats table.", icon="📊")
        return

    n = len(clubs)

    def _rank(val: float | None, all_vals: list[float]) -> str:
        if val is None:
            return "—"
        rank = sum(1 for v in all_vals if v > val) + 1
        return f"#{rank} of {n}"

    def _mp(c): return float(c.get("matches_played") or 1)
    mp_sfc       = float(sfc.get("matches_played") or 1)
    chances_val  = _p90(sfc.get("chances"),                      mp_sfc)
    assists_val  = _p90(sfc.get("assists"),                      mp_sfc)
    crosses_val  = _p90(sfc.get("crosses_from_play_successful"), mp_sfc)
    pass_acc_val = sfc.get("passes_conversion_rate")  # rate — no p90

    all_chances  = [float(c.get("chances") or 0)                      / _mp(c) for c in clubs]
    all_assists  = [float(c.get("assists") or 0)                       / _mp(c) for c in clubs]
    all_crosses  = [float(c.get("crosses_from_play_successful") or 0)  / _mp(c) for c in clubs]
    all_pass_acc = [float(c.get("passes_conversion_rate") or 0)                 for c in clubs]

    st.caption("Counting stats per 90 min")
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.metric(
            label       = "Chances p90",
            value       = f"{chances_val:.2f}" if chances_val is not None else "—",
            delta       = _rank(chances_val, all_chances),
            delta_color = "off",
            help        = "Chances created per 90 min",
        )
    with c2:
        st.metric(
            label       = "Assists p90",
            value       = f"{assists_val:.2f}" if assists_val is not None else "—",
            delta       = _rank(assists_val, all_assists),
            delta_color = "off",
        )
    with c3:
        st.metric(
            label       = "Crosses p90",
            value       = f"{crosses_val:.2f}" if crosses_val is not None else "—",
            delta       = _rank(crosses_val, all_crosses),
            delta_color = "off",
            help        = "Successful open-play crosses per 90 min",
        )
    with c4:
        st.metric(
            label       = "Pass Acc",
            value       = f"{float(pass_acc_val):.0%}" if pass_acc_val is not None else "—",
            delta       = _rank(float(pass_acc_val) if pass_acc_val is not None else None, all_pass_acc),
            delta_color = "off",
            help        = "Pass accuracy — successful passes ÷ total passes",
        )


def render_defensive_snapshot() -> None:
    """4 metric cards — Sounders' defensive stats vs the MLS club table."""
    st.markdown('<div class="srule"></div>', unsafe_allow_html=True)
    st.subheader("Defensive Snapshot")

    clubs = load_club_attacking_stats()
    if not clubs:
        st.info("Club stats not yet available from MLS API.", icon="📊")
        return

    sfc = next((c for c in clubs if c.get("team_id") == SOUNDERS_ID_MLS), None)
    if sfc is None:
        st.info("Sounders not yet in the MLS club stats table.", icon="📊")
        return

    n = len(clubs)

    def _rank(val: float | None, all_vals: list[float], lower_is_better: bool = False) -> str:
        if val is None:
            return "—"
        if lower_is_better:
            rank = sum(1 for v in all_vals if v < val) + 1
        else:
            rank = sum(1 for v in all_vals if v > val) + 1
        return f"#{rank} of {n}"

    def _mp(c): return float(c.get("matches_played") or 1)
    mp_sfc    = float(sfc.get("matches_played") or 1)
    yel_raw   = sfc.get("cards_yellow")
    red_raw   = sfc.get("cards_red")
    book_raw  = (float(yel_raw or 0) + float(red_raw or 0)) or None

    sog_val   = _p90(sfc.get("shots_on_goal_suffered"), mp_sfc)
    clr_val   = _p90(sfc.get("defensive_clearances"),   mp_sfc)
    int_val   = _p90(sfc.get("interceptions_sum"),      mp_sfc)
    book_val  = _p90(book_raw,                           mp_sfc)
    duels_val = _p90(sfc.get("tackling_games_air_won"),  mp_sfc)

    all_sog   = [float(c.get("shots_on_goal_suffered") or 0) / _mp(c) for c in clubs]
    all_clr   = [float(c.get("defensive_clearances")   or 0) / _mp(c) for c in clubs]
    all_int   = [float(c.get("interceptions_sum")      or 0) / _mp(c) for c in clubs]
    all_book  = [(float((c.get("cards_yellow") or 0) + (c.get("cards_red") or 0))) / _mp(c) for c in clubs]
    all_duels = [float(c.get("tackling_games_air_won") or 0) / _mp(c) for c in clubs]

    st.caption("Counting stats per 90 min")
    c1, c2, c3, c4, c5 = st.columns(5)

    with c1:
        st.metric(
            label       = "Shots Faced p90",
            value       = f"{sog_val:.2f}" if sog_val is not None else "—",
            delta       = _rank(sog_val, all_sog, lower_is_better=True),
            delta_color = "off",
            help        = "Shots on goal suffered per 90 min — lower is better",
        )
    with c2:
        st.metric(
            label       = "Clearances p90",
            value       = f"{clr_val:.2f}" if clr_val is not None else "—",
            delta       = _rank(clr_val, all_clr),
            delta_color = "off",
            help        = "Defensive clearances per 90 min",
        )
    with c3:
        st.metric(
            label       = "Interceptions p90",
            value       = f"{int_val:.2f}" if int_val is not None else "—",
            delta       = _rank(int_val, all_int),
            delta_color = "off",
            help        = "Total interceptions per 90 min",
        )
    with c4:
        st.metric(
            label       = "Bookings p90",
            value       = f"{book_val:.2f}" if book_val is not None else "—",
            delta       = _rank(book_val, all_book, lower_is_better=True),
            delta_color = "off",
            help        = f"Yellow + Red cards per 90 min  ({int(yel_raw or 0)}Y / {int(red_raw or 0)}R season total) — lower is better",
        )
    with c5:
        st.metric(
            label       = "Duels Won p90",
            value       = f"{duels_val:.2f}" if duels_val is not None else "—",
            delta       = _rank(duels_val, all_duels),
            delta_color = "off",
            help        = "Aerial duels won per 90 min",
        )


# Maps raw ESPN/MLS position codes → formation slot IDs used in render_starting_xi.
# Bypasses position_group inference so left/right wingers (AM-L, AM-R, LW, RW)
# land in the correct column regardless of how infer_position_from_stats() bucketed them.
_RAW_TO_SLOT: dict[str, str] = {
    "G":   "GK",  "GK":  "GK",
    "LB":  "LB",  "LWB": "LB",  "WB-L": "LB",
    "CD-L":"LCB", "LCB": "LCB",
    "CD-R":"RCB", "RCB": "RCB",
    "RB":  "RB",  "RWB": "RB",  "WB-R": "RB",
    "LM":  "LM",
    "DM":  "CAM", "CDM": "CAM", "DCM": "CAM",
    "CM":  "CAM", "MC":  "CAM",
    "AM":  "CAM", "CAM": "CAM", "SS":   "CAM",
    "RM":  "RM",
    "AM-L":"LW",  "LAM": "LW",  "LW":   "LW",
    "AM-R":"RW",  "RAM": "RW",  "RW":   "RW",
    "CF":  "ST",  "ST":  "ST",  "FW":   "ST",  "F": "ST",
    # broad single-letter ESPN fallbacks
    "D":   "LCB", "M":   "CAM",
}

# Formation slot → (position_group fallback, side fallback) used when no raw-code match
_SLOT_FALLBACK: dict[str, tuple[str, str]] = {
    "LW":  ("FW", "L"), "ST":  ("FW", "C"), "RW":  ("FW", "R"),
    "LM":  ("CM", "L"), "CAM": ("AM", "C"), "RM":  ("CM", "R"),
    "LB":  ("FB", "L"), "LCB": ("CB", "L"), "RCB": ("CB", "R"), "RB": ("FB", "R"),
    "GK":  ("GK", "C"),
}


def render_starting_xi(df: pd.DataFrame) -> None:
    st.markdown('<div class="srule"></div>', unsafe_allow_html=True)
    st.subheader("Current Starting XI  —  Most Minutes by Position")

    if df.empty:
        st.markdown('<div class="nodata">No match data yet — season starts Feb 21</div>',
                    unsafe_allow_html=True)
        return

    active = df[df["minutes_played"] > 0].copy()
    if active.empty:
        st.markdown('<div class="nodata">No minutes data yet</div>',
                    unsafe_allow_html=True)
        return

    # Sum minutes per (player, position_group, position_raw)
    season_min = (
        active
        .groupby(["player_id", "player_name", "position_group", "position_raw"],
                 as_index=False)["minutes_played"].sum()
        .rename(columns={"minutes_played": "minutes"})
    )
    # Tag each player with their formation slot from position_raw (primary)
    # and positional side from raw code (fallback path)
    season_min["slot"] = season_min["position_raw"].apply(
        lambda p: _RAW_TO_SLOT.get(str(p or "").upper())
    )
    season_min["side"] = season_min["position_raw"].apply(
        lambda p: "R" if str(p or "").upper() in {"RB","RWB","RCB","RM","RW","CD-R","WB-R","AM-R","RAM"} or str(p or "").upper().endswith("-R")
                  else ("L" if str(p or "").upper() in {"LB","LWB","LCB","LM","LW","CD-L","WB-L","AM-L","LAM"} or str(p or "").upper().endswith("-L")
                        else "C")
    )

    # Latest match stats per player (zscore + KPI values)
    kpi_cols = [c for c in set(PRIMARY_KPI.values()) if c in active.columns]
    latest = (
        active.sort_values("timestamp")
              .groupby("player_id", as_index=False).last()
              [["player_id", "composite_zscore"] + kpi_cols]
    )
    pool = season_min.merge(latest, on="player_id", how="left")

    assigned: set[str] = set()

    def _pick(slot_id: str) -> dict | None:
        """
        Assign the best unassigned player to a slot.
        1. Exact slot match from _RAW_TO_SLOT (most reliable — uses position_raw directly)
        2. position_group + side fallback (catches codes not in _RAW_TO_SLOT)
        3. position_group any side
        """
        def _top(mask) -> dict | None:
            cands = pool[mask & ~pool["player_id"].isin(assigned)]
            if cands.empty:
                return None
            row = cands.nlargest(1, "minutes").iloc[0]
            assigned.add(row["player_id"])
            return row.to_dict()

        # 1. Direct slot match
        p = _top(pool["slot"] == slot_id)
        if p:
            return p
        # 2+3. Fallback via position_group + side
        grp, side = _SLOT_FALLBACK.get(slot_id, ("CM", "C"))
        return (
            _top((pool["position_group"] == grp) & (pool["side"] == side))
            or _top(pool["position_group"] == grp)
        )

    # Formation rows displayed top → bottom (attack at top, GK at bottom).
    # Each entry: (slot_id, display_label)
    ROWS: list[list[tuple[str, str]]] = [
        [("LW", "LW"),  ("ST", "ST"),  ("RW", "RW")],
        [("LM", "LM"),  ("CAM", "CAM"), ("RM", "RM")],
        [("LB", "LB"),  ("LCB", "LCB"), ("RCB", "RCB"), ("RB", "RB")],
        [("GK", "GK")],
    ]

    def _card(col, p: dict | None, slot_lbl: str) -> None:
        with col:
            if p is None:
                st.markdown(
                    f'<div class="pc"><div class="pc-pos">{slot_lbl}</div>'
                    f'<div class="pc-name" style="color:#555">—</div></div>',
                    unsafe_allow_html=True,
                )
                return
            grp     = p.get("position_group") or slot_lbl
            kpi_col = PRIMARY_KPI.get(grp, "")
            raw_kpi = p.get(kpi_col) if kpi_col else None
            try:
                kpi_val = float(raw_kpi) if raw_kpi is not None and not pd.isna(float(raw_kpi)) else None
            except (ValueError, TypeError):
                kpi_val = None
            kpi_str = f"{kpi_val:.2f}" if kpi_val is not None else "—"
            badge   = _pct_badge(p.get("composite_zscore"))
            st.markdown(
                f'<div class="pc">'
                f'  <div class="pc-pos">{slot_lbl}</div>'
                f'  <div class="pc-name">{p.get("player_name", "?")}</div>'
                f'  <div class="pc-kpi">{kpi_str}</div>'
                f'  <div class="pc-klbl">{KPI_LABEL.get(kpi_col, kpi_col)}</div>'
                f'  {badge}'
                f'</div>',
                unsafe_allow_html=True,
            )

    for row_slots in ROWS:
        n = len(row_slots)
        if n == 4:
            cols = st.columns(4)
        elif n == 3:
            all_cols = st.columns([0.5, 1, 1, 1, 0.5])
            cols = [all_cols[1], all_cols[2], all_cols[3]]
        else:  # GK row
            all_cols = st.columns([1.5, 1, 1.5])
            cols = [all_cols[1]]

        for col, (slot_id, slot_lbl) in zip(cols, row_slots):
            player = _pick(slot_id)
            _card(col, player, slot_lbl)


def render_trends(df: pd.DataFrame) -> None:
    st.markdown('<div class="srule"></div>', unsafe_allow_html=True)
    st.subheader("Team Form  —  Rolling 3-Match Average Z-Score")

    if df.empty or "composite_zscore" not in df.columns:
        st.markdown('<div class="nodata">No match data yet</div>',
                    unsafe_allow_html=True)
        return

    # Team average Z-score per match, ordered by timestamp
    match_avg = (
        df.groupby("match_id")
          .agg(avg_z=("composite_zscore", "mean"),
               ts   =("timestamp",        "min"))
          .reset_index()
          .sort_values("ts")
    )

    if len(match_avg) < 1:
        st.markdown('<div class="nodata">Need at least one match to plot trends</div>',
                    unsafe_allow_html=True)
        return

    match_avg["rolling3"] = match_avg["avg_z"].rolling(3, min_periods=1).mean()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x    = match_avg["ts"],
        y    = match_avg["avg_z"],
        mode = "markers",
        name = "Match Z-Score",
        marker = dict(color=RAVE_GREEN, size=8, opacity=0.6),
    ))
    fig.add_trace(go.Scatter(
        x    = match_avg["ts"],
        y    = match_avg["rolling3"],
        mode = "lines+markers",
        name = "3-Match Rolling Avg",
        line = dict(color=GOLD, width=2.5),
        marker = dict(size=6),
    ))
    fig.add_hline(y=0, line_dash="dot", line_color="#555",
                  annotation_text="League avg", annotation_position="right")

    fig.update_layout(
        paper_bgcolor = CASCADE_SHALE,
        plot_bgcolor  = CARD_BG,
        font          = dict(color="#EBEBEB"),
        legend        = dict(bgcolor=CARD_BG, bordercolor=BORDER),
        margin        = dict(l=10, r=10, t=10, b=10),
        yaxis_title   = "Composite Z-Score",
        xaxis_title   = "",
        hovermode     = "x unified",
    )
    st.plotly_chart(fig, use_container_width=True)


def render_outliers(df: pd.DataFrame) -> None:
    st.markdown('<div class="srule"></div>', unsafe_allow_html=True)
    st.subheader("League Outliers — ±1σ & ±2σ by Position")

    latest_mid = (
        str(df.sort_values("timestamp")["match_id"].iloc[-1])
        if not df.empty else None
    )
    league_df = load_league_player_zscores(latest_mid)
    if league_df.empty:
        st.info("League player Z-scores not yet available from MLS API.", icon="📊")
        return

    def _band(z: float) -> str:
        if z >=  2.0: return "+2σ"
        if z >=  1.0: return "+1σ"
        if z >= -1.0: return "avg"
        if z >= -2.0: return "−1σ"
        return "−2σ"

    league_df = league_df.copy()
    league_df["band"] = league_df["composite_zscore"].apply(_band)
    outliers = league_df[league_df["band"] != "avg"]

    # Tab per position group; label shows outlier count
    tab_labels = [
        f"{pos} ({len(outliers[outliers['position_group'] == pos])})"
        for pos in POSITION_GROUPS
    ]
    pos_tabs = st.tabs(tab_labels)

    for tab, pos in zip(pos_tabs, POSITION_GROUPS):
        with tab:
            pos_df = (
                outliers[outliers["position_group"] == pos]
                .copy()
                .sort_values("composite_zscore", ascending=False)
                .reset_index(drop=True)
            )
            if pos_df.empty:
                st.caption(f"No ±1σ outliers in {pos} yet.")
                continue

            # Star prefix for Sounders players
            pos_df["Player"] = pos_df.apply(
                lambda r: f"★ {r['player_name']}" if r["is_sfc"] else r["player_name"],
                axis=1,
            )
            display = pos_df[["Player", "team_id", "composite_zscore", "band", "minutes"]].rename(
                columns={
                    "team_id":          "Team",
                    "composite_zscore": "Z",
                    "band":             "σ",
                    "minutes":          "Min",
                }
            )

            above = display[display["Z"] >= 1.0]
            below = display[display["Z"] <= -1.0]

            if not above.empty:
                st.caption("▲ Above average")
                st.dataframe(
                    above.style
                        .format({"Z": "{:+.3f}", "Min": "{:.0f}"})
                        .background_gradient(subset=["Z"], cmap="RdYlGn", vmin=-3.0, vmax=3.0),
                    use_container_width=True,
                    hide_index=True,
                    height=min(320, (len(above) + 1) * 35 + 3),
                )
            if not below.empty:
                st.caption("▼ Below average")
                st.dataframe(
                    below.style
                        .format({"Z": "{:+.3f}", "Min": "{:.0f}"})
                        .background_gradient(subset=["Z"], cmap="RdYlGn", vmin=-3.0, vmax=3.0),
                    use_container_width=True,
                    hide_index=True,
                    height=min(320, (len(below) + 1) * 35 + 3),
                )


# ─────────────────────────────────────────────────────────────────────────────
# Upcoming Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def _fetch_upcoming_fixtures() -> list[dict]:
    """
    Load upcoming Sounders fixtures.  Prefers the cached match_schedule.json
    (written by Airflow Monday sync); falls back to a live ESPN scoreboard
    scan if the cache is absent or stale (older than 7 days).
    """
    from datetime import datetime, timezone

    now_utc = datetime.now(timezone.utc)

    # Try the cached schedule file first
    cached_schedule = load_schedule()
    if cached_schedule:
        upcoming = [
            m for m in cached_schedule
            if m.get("kickoff_utc", m.get("date_local", "")) > now_utc.isoformat()[:10]
        ]
        if upcoming:
            return upcoming

    # Live fallback
    try:
        return ESPNApiClient().get_upcoming_sounders_fixtures(n_weeks=10)
    except Exception:
        return []


def render_next_fixtures(n: int = 3) -> None:
    """
    Show the next *n* upcoming Sounders matches (all competitions).
    """
    st.markdown('<div class="srule"></div>', unsafe_allow_html=True)
    st.markdown(
        f'<div style="font-size:.85em; font-weight:700; color:{RAVE_GREEN}; '
        f'letter-spacing:.04em; margin-bottom:6px;">UPCOMING FIXTURES</div>',
        unsafe_allow_html=True,
    )

    fixtures = _fetch_upcoming_fixtures()
    if not fixtures:
        st.markdown(
            '<div class="nodata" style="padding:14px; font-size:.8em;">'
            'No upcoming fixtures found</div>',
            unsafe_allow_html=True,
        )
        return

    from datetime import datetime, timezone
    now_utc  = datetime.now(timezone.utc).isoformat()
    upcoming = [f for f in fixtures if f.get("kickoff_utc", f.get("date_local","")) > now_utc[:10]]

    shown = 0
    for m in upcoming:
        if shown >= n:
            break
        # Parse date for display
        raw_dt = m.get("kickoff_utc", m.get("date_local", ""))
        try:
            dt = datetime.fromisoformat(raw_dt.replace("Z", "+00:00"))
            month_day = dt.strftime("%-d %b").upper()
            weekday   = dt.strftime("%a").upper()
        except Exception:
            month_day = raw_dt[:10]
            weekday   = ""

        home = m.get("home", "?")
        away = m.get("away", "?")
        home = home.replace("Seattle Sounders FC", "SSFC")
        away = away.replace("Seattle Sounders FC", "SSFC")
        comp = m.get("competition", "MLS")

        st.markdown(
            f'<div class="fx-card">'
            f'  <div class="fx-date">{weekday}<br>{month_day}</div>'
            f'  <div class="fx-teams">{home}<br><span style="color:#888">vs</span> {away}</div>'
            f'  <div class="fx-comp">{comp}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
        shown += 1

    if shown == 0:
        st.markdown(
            '<div class="nodata" style="padding:14px; font-size:.8em;">'
            'No upcoming fixtures scheduled</div>',
            unsafe_allow_html=True,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Western Conference Standings
# ─────────────────────────────────────────────────────────────────────────────

def render_standings_table() -> None:
    """
    Render a compact Western Conference standings table focused on the
    Sounders: 2 teams above, Sounders, 2 teams below.

    Data comes from data/processed/standings.json, written by the Airflow
    DAG every Monday at 10 AM EST.  Falls back to a live ESPN fetch when
    the cache is absent (e.g. first run before Airflow has executed).
    """
    st.markdown('<div class="srule"></div>', unsafe_allow_html=True)
    st.markdown(
        f'<div style="font-size:.85em; font-weight:700; color:{RAVE_GREEN}; '
        f'letter-spacing:.04em; margin-bottom:4px;">WESTERN CONFERENCE</div>',
        unsafe_allow_html=True,
    )

    payload = load_standings()
    rows    = payload.get("standings", [])

    # Live fallback when the cache hasn't been primed yet
    if not rows:
        try:
            client = ESPNApiClient()
            rows   = client.get_western_standings()
        except Exception:
            rows = []

    if not rows:
        st.markdown(
            '<div class="nodata" style="padding:16px; font-size:.8em;">'
            'Standings unavailable — trigger <em>sounders_schedule_sync</em></div>',
            unsafe_allow_html=True,
        )
        return

    # Find Sounders row index
    sounders_idx = next(
        (i for i, r in enumerate(rows)
         if r.get("team_id") == "9726" or "Seattle" in r.get("short_name", "")),
        None,
    )

    if sounders_idx is None:
        # Sounders not in table (early season edge case) — show top 5
        window = rows[:5]
    else:
        lo = max(0, sounders_idx - 2)
        hi = min(len(rows), sounders_idx + 3)
        window = rows[lo:hi]

    # Build HTML table
    header = (
        "<table class='st-tbl'>"
        "<thead><tr>"
        "<th>#</th><th style='text-align:left'>Club</th>"
        "<th>Pts</th><th>GP</th><th>W</th><th>D</th><th>L</th><th>GD</th>"
        "</tr></thead><tbody>"
    )
    body_rows = []
    for r in window:
        is_sfc   = (r.get("team_id") == "9726" or "Seattle" in r.get("short_name", ""))
        row_cls  = " class='st-row-sounders'" if is_sfc else ""
        gd_str   = str(r.get("gd", "0"))
        if gd_str.lstrip("-").isdigit():
            gd_val = int(gd_str)
            gd_fmt = f"+{gd_val}" if gd_val > 0 else str(gd_val)
        else:
            gd_fmt = gd_str
        body_rows.append(
            f"<tr{row_cls}>"
            f"<td>{r['rank']}</td>"
            f"<td>{r['short_name']}</td>"
            f"<td>{r['pts']}</td>"
            f"<td>{r['gp']}</td>"
            f"<td>{r['w']}</td>"
            f"<td>{r['d']}</td>"
            f"<td>{r['l']}</td>"
            f"<td>{gd_fmt}</td>"
            f"</tr>"
        )
    footer = "</tbody></table>"

    st.markdown(header + "".join(body_rows) + footer, unsafe_allow_html=True)

    # Last-synced timestamp (from cache file, or omit if live fetch)
    last_synced = payload.get("last_synced", "")
    if last_synced:
        try:
            from datetime import datetime, timezone
            ts  = datetime.fromisoformat(last_synced)
            fmt = ts.strftime("Updated %b %-d · %H:%M UTC")
        except Exception:
            fmt = last_synced[:16]
        st.markdown(f'<div class="st-last-synced">{fmt}</div>', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────

def build_sidebar(df: pd.DataFrame, schedule: list[dict]) -> pd.DataFrame:
    st.sidebar.markdown(
        f'<h2 style="color:{RAVE_GREEN};margin-top:0">⚽ Sounders 2026</h2>',
        unsafe_allow_html=True,
    )
    st.sidebar.markdown("---")

    if df.empty:
        st.sidebar.info("No match data yet.")
        return df

    labels = _match_labels(df, schedule)
    all_ids = list(labels.keys())

    # ── Matchweek filter ──────────────────────────────────────────────────────
    st.sidebar.markdown("**Matchweek**")
    selected_ids = st.sidebar.multiselect(
        label      = "Select matches",
        options    = all_ids,
        default    = all_ids,
        format_func= lambda mid: labels.get(mid, mid),
        label_visibility = "collapsed",
    )

    # ── Opponent filter ───────────────────────────────────────────────────────
    sched_map = {m["id"]: m for m in schedule}
    opponents = sorted({
        (m.get("away") if "Seattle" in m.get("home", "") else m.get("home", "Unknown"))
        for mid in all_ids
        if (m := sched_map.get(mid))
    })
    if opponents:
        st.sidebar.markdown("**Opponent**")
        sel_opp = st.sidebar.multiselect(
            "Opponent", options=opponents, default=opponents,
            label_visibility="collapsed",
        )
        # Filter match ids by opponent
        filtered_ids = []
        for mid in (selected_ids or all_ids):
            m = sched_map.get(mid)
            if m is None:
                filtered_ids.append(mid)
            else:
                opp = m.get("away") if "Seattle" in m.get("home", "") else m.get("home", "")
                if opp in sel_opp:
                    filtered_ids.append(mid)
        selected_ids = filtered_ids

    # ── Statistical category ──────────────────────────────────────────────────
    st.sidebar.markdown("**Statistical Category**")
    category = st.sidebar.radio(
        "Category", options=list(STAT_CATEGORIES.keys()),
        label_visibility="collapsed",
    )

    st.sidebar.markdown("---")
    if selected_ids:
        st.sidebar.caption(f"{len(selected_ids)} of {len(all_ids)} matches selected")

    return df[df["match_id"].isin(selected_ids)] if selected_ids else df.head(0)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    df       = load_timeseries()
    schedule = load_schedule()

    # Header
    st.markdown(
        f'<h1 style="margin-bottom:0">Seattle Sounders FC  —  Analytics 2026</h1>',
        unsafe_allow_html=True,
    )
    if df.empty:
        st.caption("Season begins Feb 21, 2026.  Data will populate after the first match.")
    else:
        n_matches = df["match_id"].nunique()
        n_players = df["player_id"].nunique()
        st.caption(f"{n_matches} match{'es' if n_matches != 1 else ''} · "
                   f"{n_players} players tracked")

    df_filtered = build_sidebar(df, schedule)
    active_df   = df_filtered if not df.empty else df

    # ── Two-column layout: formation (left 1/3) | analytics (right 2/3) ──────
    col_left, col_right = st.columns([1, 2])

    with col_left:
        render_formation_panel(active_df, schedule)
        render_next_fixtures(n=3)
        render_standings_table()

    with col_right:
        render_attacking_snapshot()
        render_creation_snapshot()
        render_defensive_snapshot()
        render_trends(active_df)
        render_outliers(active_df)


if __name__ == "__main__":
    main()
