"""
Central configuration for the Sounders Analytics Engine.
Adjust constants here rather than touching logic modules.

API ENDPOINTS CONFIRMED 2026-02-17 via Playwright network interception:
  Season list:    https://stats-api.mlssoccer.com/competitions/{comp}/seasons
  Player stats:   https://sportapi.mlssoccer.com/api/stats/players/competition/{comp}/season/{season}/order/goals/desc
  Club stats:     https://stats-api.mlssoccer.com/statistics/clubs/competitions/{comp}/seasons/{season}
  ESPN schedule:  https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1/teams/{team_id}/schedule
  ESPN summary:   https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1/summary?event={event_id}
"""

# ── Season / Competition ─────────────────────────────────────────────────────
SEASON           = 2026
COMPETITION_MLS  = "MLS-COM-000001"     # MLS Regular Season competition ID (Opta/Stats API)

# Season IDs by year (populated from /competitions/{comp}/seasons endpoint)
# Will be refreshed dynamically at runtime; fallback values provided here.
SEASON_ID_BY_YEAR: dict[int, str] = {
    2026: "MLS-SEA-0001KA",
    2025: "MLS-SEA-0001K9",
    2024: "MLS-SEA-0001K8",
}

# ── Team identifiers ─────────────────────────────────────────────────────────
SOUNDERS_ID_MLS  = "MLS-CLU-00000S"    # confirmed via club stats API 2026-02-17
SOUNDERS_ID_ESPN = "9726"              # confirmed via ESPN teams probe 2026-02-16

# ── API base URLs ─────────────────────────────────────────────────────────────
MLS_STATS_BASE   = "https://stats-api.mlssoccer.com"      # season/club stats
MLS_SPORT_BASE   = "https://sportapi.mlssoccer.com/api"   # player stats (paginated)
ESPN_SITE_BASE   = "https://site.api.espn.com/apis/site/v2/sports/soccer"
ESPN_LEAGUE      = "usa.1"            # MLS league code on ESPN

# Required headers for MLS APIs (no token needed, but Referer is checked)
MLS_HEADERS: dict[str, str] = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer":    "https://www.mlssoccer.com/",
    "Origin":     "https://www.mlssoccer.com",
}

# ── HTTP ──────────────────────────────────────────────────────────────────────
REQUEST_TIMEOUT  = 20       # seconds
REQUEST_RETRIES  = 3
RETRY_BACKOFF    = 2.0      # seconds between retries
MLS_PAGE_SIZE    = 50       # players per page for paginated endpoints

# ── Analytics thresholds ──────────────────────────────────────────────────────
UNDERPERFORMER_Z_THRESHOLD = -1.0
MIN_MINUTES_FOR_VELOCITY   = 30    # minutes on-pitch required to qualify for velocity window
VELOCITY_WINDOW            = 3     # number of qualifying appearances for Form Velocity

# ── Benchmark cache ───────────────────────────────────────────────────────────
BENCHMARK_MAX_AGE_HOURS = 24

# ── Position groups ───────────────────────────────────────────────────────────
POSITION_GROUPS = ["GK", "CB", "FB", "DM", "CM", "AM", "FW"]

# Maps raw API position codes → canonical group labels.
# MLS API uses 'goal_keeper': True/False rather than position strings,
# so these codes come mainly from ESPN and mock data.
POSITION_MAP: dict[str, str] = {
    # ESPN / common position codes
    "GK":  "GK", "G":   "GK",
    # Center-backs — ESPN uses CD-L / CD-R for left/right center-back
    "CB":  "CB", "LCB": "CB", "RCB": "CB",
    "CD-L": "CB", "CD-R": "CB",   # confirmed ESPN codes 2026-02-23
    # Full-backs / Wing-backs
    "LB":  "FB", "RB":  "FB", "LWB": "FB", "RWB": "FB",
    "WB-L": "FB", "WB-R": "FB",   # ESPN wide-back variants
    # Defensive / central midfield
    "DM":  "DM", "CDM": "DM", "DCM": "DM",
    "CM":  "CM", "MC":  "CM",
    "LM":  "CM", "RM":  "CM",   # wide central midfielders (ESPN uses LM/RM for box-to-box)
    # Attacking midfield / forwards
    "CAM": "AM", "AM":  "AM", "SS":  "AM",
    # Wide forwards / wingers — ESPN uses AM-L/AM-R for wide attackers
    "AM-L": "FW", "AM-R": "FW", "LAM": "FW", "RAM": "FW",
    "LW":  "FW", "RW":  "FW", "CF":  "FW", "ST": "FW", "FW": "FW",
    # Generic ESPN single-letter codes (broad fallback)
    "D":   "CB",
    "M":   "CM",
    "F":   "FW",
}

# ── Per-position stat weights for composite Z-score ───────────────────────────
# Keys are EXACT field names from the MLS Stats API (confirmed 2026-02-21).
# Weights must reflect the relative importance of each stat for that position.
# Negative weights = lower is better (e.g. goals_conceded for GKs).
#
# MLS API field coverage audit (2026-02-21, n=20 Sounders players):
#   goalkeeper_saves           = saves made (GK only)               — 1/20
#   clean_sheets               = shutouts (GK only)                 — 1/20
#   goals_conceded             = goals allowed (GK, negative weight) — 1/20
#   defensive_clearances       = clearances (CB/FB only in practice) — 4/20
#   crosses_from_play_successful = accurate crosses                  — varies
#   fouls_against_opponent     = foul/duel proxy for DM/CM          — 6/20
#   passes_conversion_rate     = pass completion % (0.0–1.0)        — 16/20 ✓
#   ball_control_phases        = touch/control volume               — 16/20 ✓
#   assists                    = goal assists                       — varies
#   chances                    = open-play chances created          — 6/20
#   goals                      = goals scored                       — varies
#   shots_on_target            = shots on target                    — varies
#   xG                         = expected goals                     — 9/20
#
# REMOVED (absent from MLS API — confirmed 0/20 coverage):
#   interceptions_sum          ← NOT in MLS Stats API
#   tackling_games_air_won     ← NOT in MLS Stats API

POSITION_STATS: dict[str, list[tuple[str, float]]] = {
    "GK": [
        ("goalkeeper_saves",         0.45),
        ("clean_sheets",             0.35),
        ("goals_conceded",          -0.20),   # fewer is better
    ],
    "CB": [
        ("defensive_clearances",     0.45),   # primary CB metric
        ("passes_conversion_rate",   0.35),   # build-out quality
        ("ball_control_phases",      0.20),   # involvement / composure
    ],
    "FB": [
        ("crosses_from_play_successful", 0.35),  # attacking width
        ("passes_conversion_rate",       0.30),  # distribution accuracy
        ("assists",                      0.20),  # direct output
        ("ball_control_phases",          0.15),  # involvement in build
    ],
    "DM": [
        ("passes_conversion_rate",   0.40),   # screen + distribute
        ("ball_control_phases",      0.35),   # high-touch anchor
        ("fouls_against_opponent",   0.25),   # defensive action proxy
    ],
    "CM": [
        ("passes_conversion_rate",   0.30),   # distribution engine
        ("ball_control_phases",      0.25),   # involvement volume
        ("chances",                  0.25),   # chance creation
        ("assists",                  0.20),   # direct output
    ],
    "AM": [
        ("assists",                  0.30),
        ("chances",                  0.30),
        ("goals",                    0.20),
        ("shots_on_target",          0.20),
    ],
    "FW": [
        ("goals",                    0.35),
        ("xG",                       0.25),
        ("shots_on_target",          0.20),
        ("assists",                  0.20),
    ],
}

# Flat set of all stat keys used across all positions (for CSV column list)
ALL_STAT_KEYS: list[str] = sorted({
    stat
    for stats in POSITION_STATS.values()
    for stat, _ in stats
})

# ── Per-90 normalization ───────────────────────────────────────────────────────
# Stats that are already rates or proportions — do NOT divide by minutes/90.
# Everything else in POSITION_STATS is a counting stat and must be normalized.
RATE_STATS: frozenset[str] = frozenset({
    "passes_conversion_rate",   # already 0.0–1.0 proportion
    "clean_sheets",             # binary per-match event, not a count
    "maximum_speed",            # peak value (km/h) — not cumulative, no per-90
})

# Stats reported directly from ESPN per-match box scores.
# These reflect single-match totals, so compute_zscore normalizes them using
# match minutes (player.minutes).  All other counting stats come from the MLS
# API as season aggregates and must be normalized using season_minutes so the
# per-90 rate matches how the benchmark was built.
ESPN_DIRECT_STATS: frozenset[str] = frozenset({
    "goals",
    "assists",
    "goalkeeper_saves",
    "goals_conceded",
    "shots_on_target",
    "fouls_against_opponent",
})

# Minimum season minutes for a player to be included in the benchmark.
# Filters out cameo appearances whose per-90 projections would be inflated.
MIN_BENCHMARK_MINUTES: int = 90
