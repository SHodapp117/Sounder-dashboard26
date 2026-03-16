"""
mock_data.py
────────────
Generates realistic synthetic match data for end-to-end testing
before live API data is available.

Uses real 2026 Sounders roster names and ESPN position codes, with
stats drawn from distributions calibrated to typical MLS match values.
"""

import random
from api_client import PlayerStat

random.seed(42)  # reproducible mock runs

# ── Sounders 2026 roster (name, raw_position, ESPN_id) ──────────────────────
SOUNDERS_ROSTER = [
    ("Stefan Frei",       "GK",  "s001"),
    ("Jackson Ragen",     "CB",  "s002"),
    ("Yeimar Gomez",      "CB",  "s003"),
    ("Nouhou Tolo",       "LB",  "s004"),
    ("Alex Roldan",       "RB",  "s005"),
    ("João Paulo",        "CDM", "s006"),
    ("Danny Leyva",       "CDM", "s007"),
    ("Albert Rusnák",     "CM",  "s008"),
    ("Josh Atencio",      "CM",  "s009"),
    ("Jordan Morris",     "LW",  "s010"),
    ("Raúl Ruidíaz",      "ST",  "s011"),
    ("Obed Vargas",       "CAM", "s012"),
    ("Leo Chu",           "LW",  "s013"),
    ("Andrew Thomas",     "GK",  "s014"),   # backup GK
]

# ── Stat distributions per position: (stat_key, mean, std, min_clamp, max_clamp)
# Keys MUST match exact MLS Stats API field names (confirmed 2026-02-17).
# Values calibrated to realistic single-season MLS numbers (prorated to 1 match).
POSITION_STAT_DISTRIBUTIONS: dict[str, list[tuple]] = {
    "GK": [
        ("goalkeeper_saves",         3.5,  1.5,  0,   10),
        ("clean_sheets",             0.3,  0.46, 0,   1),
        ("goals_conceded",           1.1,  0.9,  0,   5),
    ],
    "CB": [
        ("defensive_clearances",     4.0,  2.0,  0,   12),
        ("interceptions_sum",        1.8,  1.0,  0,   6),
        ("tackling_games_air_won",   3.0,  1.5,  0,   9),
        ("passes_conversion_rate",   0.82, 0.08, 0.5, 1.0),
    ],
    "FB": [
        ("crosses_from_play_successful", 2.0, 1.2, 0,   7),
        ("interceptions_sum",            1.2, 0.8, 0,   4),
        ("passes_conversion_rate",       0.79, 0.09, 0.5, 1.0),
        ("tackling_games_air_won",       1.0, 0.8, 0,   4),
        ("assists",                      0.15, 0.35, 0, 2),
    ],
    "DM": [
        ("interceptions_sum",        2.2,  1.0,  0,   7),
        ("passes_conversion_rate",   0.85, 0.07, 0.6, 1.0),
        ("fouls_against_opponent",   2.5,  1.2,  0,   8),
        ("ball_control_phases",      45.0, 15.0, 10,  120),
        ("tackling_games_air_won",   1.5,  0.9,  0,   5),
    ],
    "CM": [
        ("passes_conversion_rate",   0.83, 0.08, 0.5, 1.0),
        ("assists",                  0.20, 0.40, 0,   2),
        ("chances",                  1.0,  0.7,  0,   4),
        ("interceptions_sum",        1.2,  0.8,  0,   4),
        ("crosses_from_play_successful", 0.8, 0.6, 0, 3),
    ],
    "AM": [
        ("assists",                  0.25, 0.43, 0,   2),
        ("chances",                  1.5,  0.9,  0,   5),
        ("goals",                    0.20, 0.40, 0,   2),
        ("shots_on_target",          1.0,  0.8,  0,   4),
    ],
    "FW": [
        ("goals",                    0.40, 0.60, 0,   3),
        ("xG",                       0.35, 0.25, 0.0, 2.0),
        ("shots_on_target",          1.5,  1.0,  0,   5),
        ("assists",                  0.15, 0.35, 0,   2),
    ],
}

# Map position codes → groups (mirrors config.POSITION_MAP)
_POS_GROUP = {
    "GK": "GK",
    "CB": "CB", "LCB": "CB", "RCB": "CB",
    "LB": "FB", "RB": "FB", "LWB": "FB", "RWB": "FB",
    "CDM": "DM", "DM": "DM",
    "CM": "CM", "MC": "CM",
    "CAM": "AM", "AM": "AM",
    "LW": "FW", "RW": "FW", "ST": "FW", "CF": "FW", "FW": "FW",
}


def _sample_stats(group: str, seed_offset: int = 0) -> dict:
    """Generate one realistic set of stats for a given position group."""
    dists = POSITION_STAT_DISTRIBUTIONS.get(group, [])
    stats: dict[str, float] = {}
    for stat, mean, std, lo, hi in dists:
        val = random.gauss(mean, std)
        val = max(lo, min(hi, val))
        # Round counts to integers; keep rates as floats
        if lo == 0 and hi > 1 and isinstance(mean, int):
            val = round(val)
        else:
            val = round(val, 3)
        stats[stat] = val
    return stats


def generate_mock_sounders_match(match_id: str = "MOCK-001") -> list[PlayerStat]:
    """
    Generate a single-match PlayerStat list for the Sounders squad.
    Starting XI plays 90 min; 2–3 subs play 30–60 min.
    """
    players: list[PlayerStat] = []
    starters = SOUNDERS_ROSTER[:11]
    subs = SOUNDERS_ROSTER[11:]

    for name, pos, pid in starters:
        group = _POS_GROUP.get(pos, "CM")
        players.append(PlayerStat(
            player_id    = pid,
            player_name  = name,
            team_id      = "MLS-CLU-00000S",
            position_raw = pos,
            is_gk        = (pos == "GK"),
            minutes      = random.uniform(70, 90),
            stats        = _sample_stats(group),
            match_id     = match_id,
            source       = "mock",
        ))

    for name, pos, pid in subs:
        group = _POS_GROUP.get(pos, "CM")
        players.append(PlayerStat(
            player_id    = pid,
            player_name  = name,
            team_id      = "MLS-CLU-00000S",
            position_raw = pos,
            is_gk        = (pos == "GK"),
            minutes      = random.uniform(10, 45),
            stats        = _sample_stats(group),
            match_id     = match_id,
            source       = "mock",
        ))

    return players


def generate_mock_league_players(players_per_group: int = 25) -> list[PlayerStat]:
    """
    Generate a full mock MLS player pool for building the league benchmark.
    Creates `players_per_group` synthetic players per position group.
    """
    players: list[PlayerStat] = []
    team_names = [
        "atlanta-united-fc", "austin-fc", "cf-montreal", "charlotte-fc",
        "chicago-fire-fc", "colorado-rapids", "columbus-crew", "dc-united",
        "fc-cincinnati", "fc-dallas", "houston-dynamo-fc", "inter-miami-cf",
        "la-galaxy", "lafc", "minnesota-united-fc", "nashville-sc",
        "new-england-revolution", "new-york-city-fc", "new-york-red-bulls",
        "orlando-city-sc", "philadelphia-union", "portland-timbers",
        "real-salt-lake", "san-jose-earthquakes", "seattle-sounders-fc",
        "sporting-kansas-city", "toronto-fc", "vancouver-whitecaps",
    ]

    pid_counter = 1000
    for group in POSITION_STAT_DISTRIBUTIONS.keys():
        for i in range(players_per_group):
            team = random.choice(team_names)
            # Reverse-map group to a sample raw position code
            pos_code = {
                "GK": "GK", "CB": "CB", "FB": "LB",
                "DM": "CDM", "CM": "CM", "AM": "CAM", "FW": "ST",
            }[group]
            # Season-total minutes (mirrors what the real MLS API returns).
            # 20–30 appearances × 90 min = 1800–2700 season minutes.
            season_minutes = random.uniform(1800, 2700)
            # Per-90 stats scaled to season totals so build_benchmark()
            # normalises them back to per-90 correctly.
            per90_stats = _sample_stats(group, seed_offset=pid_counter)
            scale       = season_minutes / 90.0
            season_stats = {
                k: round(v * scale, 3) if k not in ("passes_conversion_rate", "clean_sheets") else v
                for k, v in per90_stats.items()
            }
            players.append(PlayerStat(
                player_id    = f"mock_{pid_counter}",
                player_name  = f"Player {pid_counter}",
                team_id      = team,
                position_raw = pos_code,
                is_gk        = (group == "GK"),
                minutes      = season_minutes,
                stats        = season_stats,
                match_id     = None,
                source       = "mock",
            ))
            pid_counter += 1

    return players
