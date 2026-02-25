"""
analytics_engine.py
───────────────────
Positional Z-score calculation and Form Velocity for all Sounders players.

Pipeline per match:
  1. build_benchmark(all_mls_players)
       → for each position group, compute (μ, σ) per stat
  2. compute_zscore(player, benchmark)
       → weighted composite Z-score using per-position stat weights
  3. compute_form_velocity(player_id, current_zscore, history_df)
       → current Z-score minus rolling average of last N qualifying matches
          (qualifying = minutes_played >= MIN_MINUTES_FOR_VELOCITY)
  4. process_match(...)
       → full pipeline; returns list[PlayerInsight]
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pydantic import BaseModel

from config import (
    POSITION_MAP,
    POSITION_STATS,
    POSITION_GROUPS,
    MIN_MINUTES_FOR_VELOCITY,
    VELOCITY_WINDOW,
    RATE_STATS,
    MIN_BENCHMARK_MINUTES,
)
from api_client import PlayerStat


def infer_position_from_stats(stats: dict, is_gk: bool) -> str | None:
    """
    Infer a broad positional group from a player's season stat profile.

    Used when the data source (MLS Stats API) does not supply a position code.
    This is intentionally heuristic — it will be imprecise for versatile players
    but produces a usable benchmark grouping for all 1000+ MLS players.

    Logic (uses only stats confirmed present in MLS API as of 2026-02-21):
      GK  → goal_keeper flag
      FW  → xG data present + high attack score, low clearances
      AM  → high chance-creation + assists, moderate attack
      CB  → clearances data present + dominant defensive involvement
      FB  → high crossing output, low goal threat
      DM  → high ball_control_phases + good pass %, low attack
      CM  → default for all remaining midfielders

    NOTE: interceptions_sum and tackling_games_air_won are NOT in the MLS API
    and are intentionally absent from all score calculations here.
    """
    if is_gk:
        return "GK"

    def _n(key: str) -> float:
        v = stats.get(key)
        return float(v) if v is not None else 0.0

    goals      = _n("goals")
    xG         = _n("xG")
    shots      = _n("shots_on_target")
    assists    = _n("assists")
    chances    = _n("chances")
    clearances = _n("defensive_clearances")
    crosses    = _n("crosses_from_play_successful")
    fouls      = _n("fouls_against_opponent")
    ball_ctrl  = _n("ball_control_phases")
    pass_rate  = _n("passes_conversion_rate")

    # Presence flags — stat absent entirely means MLS API didn't report it
    has_xG         = stats.get("xG") is not None
    has_clearances = stats.get("defensive_clearances") is not None

    attack_score   = goals * 2 + (xG if has_xG else 0) + shots * 0.5 + assists
    creation_score = assists * 1.5 + chances * 0.5 + crosses * 0.3

    # ── FW: xG data present (MLS only reports xG for shooters) + high output
    if has_xG and attack_score >= 3 and clearances < 4:
        return "FW"

    # ── AM: creative with a goal threat but not a pure striker
    if creation_score >= 3 and attack_score >= 2:
        return "AM"

    # ── CB: clearance data present (MLS reports this for genuine defenders)
    if has_clearances and clearances >= 6 and attack_score < 3:
        return "CB"

    # ── FB: wide — crosses + moderate attack, not a striker
    if crosses >= 3 and attack_score < 4:
        return "FB"

    # ── DM: high-touch defensive anchor with good passing, low attack
    if ball_ctrl >= 50 and pass_rate >= 0.78 and attack_score < 3:
        return "DM"

    # ── FW (secondary): decent scorer without dominant xG data
    if attack_score >= 5:
        return "FW"

    # ── CM: box-to-box / central (default for most midfielders)
    if ball_ctrl >= 20 or pass_rate >= 0.75:
        return "CM"

    # ── CB fallback: any player with some clearance involvement
    if has_clearances and clearances >= 2:
        return "CB"

    return "CM"   # final fallback


# ── Output model ──────────────────────────────────────────────────────────────

class PlayerInsight(BaseModel):
    player_id:        str
    player_name:      str
    position_raw:     str | None
    position_group:   str | None
    minutes_played:   float
    composite_zscore: float | None
    form_velocity:    float | None
    match_id:         str
    raw_stats:        dict   # individual stat values used in the Z-score


# ── Engine ────────────────────────────────────────────────────────────────────

class AnalyticsEngine:
    """
    Stateless within a single call; benchmark is passed as a dict so
    callers control caching/persistence (see storage_manager.py).
    """

    # ── Benchmark builder ─────────────────────────────────────────────────

    def build_benchmark(self, all_players: list[PlayerStat]) -> dict[str, dict[str, tuple[float, float]]]:
        """
        Compute per-group, per-stat (mean, std) from the full MLS player pool.

        Returns:
            {
              "FW": {"goals": (0.38, 0.52), "shots_on_target": (1.4, 0.9), ...},
              "CB": {...},
              ...
            }

        Only players with minutes_played > 0 are included to exclude
        players who appeared on the bench but did not play.
        """
        # Bucket players by canonical position group.
        # When position_raw is None (MLS API only gives goal_keeper flag),
        # infer the group from the player's stat profile.
        buckets: dict[str, list[PlayerStat]] = {g: [] for g in POSITION_GROUPS}
        for player in all_players:
            if player.minutes < MIN_BENCHMARK_MINUTES:
                continue   # filter cameos whose per-90 projections would be inflated
            group = POSITION_MAP.get(player.position_raw or "", None)
            if group is None:
                group = infer_position_from_stats(player.stats, player.is_gk)
            if group and group in buckets:
                buckets[group].append(player)

        benchmark: dict[str, dict[str, tuple[float, float]]] = {}

        for group, players in buckets.items():
            if not players:
                continue
            stat_keys = [s[0] for s in POSITION_STATS.get(group, [])]
            benchmark[group] = {}
            for stat in stat_keys:
                # Normalize counting stats to per-90 so the benchmark means are
                # comparable to per-match ESPN stats used in compute_zscore().
                values = []
                for p in players:
                    raw   = float(p.stats.get(stat) or 0)
                    scale = p.minutes / 90.0
                    values.append(raw / scale if stat not in RATE_STATS else raw)
                mu    = float(np.mean(values))
                sigma = float(np.std(values, ddof=1)) if len(values) > 1 else 1.0
                if sigma == 0:
                    sigma = 1.0  # guard: prevent division by zero
                benchmark[group][stat] = (round(mu, 6), round(sigma, 6))

        return benchmark

    # ── Z-score ───────────────────────────────────────────────────────────

    def compute_zscore(
        self,
        player: PlayerStat,
        benchmark: dict[str, dict[str, tuple[float, float]]],
    ) -> float | None:
        """
        Compute the weighted composite positional Z-score for one player.

        For each stat in the player's position group:
            z_stat = (player_value - group_mean) / group_std
        Final score = sum(weight_i * z_stat_i)

        Negative weights are supported (e.g. goals_against for GKs).
        Returns None if the player's position cannot be mapped.
        """
        pos_group = POSITION_MAP.get(player.position_raw or "", None)
        # Fall back to stat inference if the mapped group isn't in the benchmark
        # (e.g. ESPN returns LB/RB/AM but benchmark only covers GK/CM/FW)
        if pos_group is None or pos_group not in benchmark:
            pos_group = infer_position_from_stats(player.stats, player.is_gk)
        if not pos_group or pos_group not in benchmark:
            return None

        group_bm    = benchmark[pos_group]
        stat_weights = POSITION_STATS.get(pos_group, [])

        weighted_z   = 0.0
        covered_weight = 0.0

        # Normalize match minutes to per-90 scale (floor at 1.0 to guard
        # against 0-minute rows that would cause division by zero).
        scale = max(player.minutes / 90.0, 1.0)

        for stat, weight in stat_weights:
            if stat not in group_bm:
                continue
            # Skip stats absent from this player's data — treat as "not reported"
            # rather than zero.  ESPN box scores omit pass%, interceptions, xG, etc.
            # Coercing those absences to 0 would unfairly tank the composite score.
            if stat not in player.stats or player.stats[stat] is None:
                continue
            mu, sigma = group_bm[stat]
            raw       = float(player.stats[stat] or 0)
            # Apply the same normalization used when building the benchmark
            value     = raw / scale if stat not in RATE_STATS else raw
            z_stat    = (value - mu) / sigma
            weighted_z     += weight * z_stat
            covered_weight += abs(weight)

        if covered_weight == 0:
            return None

        # Normalise by covered weight so partial stat coverage still makes sense
        return round(weighted_z / covered_weight, 4)

    # ── Form Velocity ─────────────────────────────────────────────────────

    def compute_form_velocity(
        self,
        player_id:     str,
        current_zscore: float,
        history:       pd.DataFrame,
    ) -> float | None:
        """
        Form Velocity = current Z-score − rolling average of last
        VELOCITY_WINDOW qualifying appearances.

        A match qualifies only if minutes_played >= MIN_MINUTES_FOR_VELOCITY,
        which filters out brief substitute cameos that would skew the average.

        Returns None when the player has fewer than VELOCITY_WINDOW
        qualifying appearances in the history.
        """
        if history.empty or "player_id" not in history.columns:
            return None

        qualifying = (
            history[
                (history["player_id"].astype(str) == str(player_id)) &
                (pd.to_numeric(history["minutes_played"], errors="coerce")
                    .fillna(0) >= MIN_MINUTES_FOR_VELOCITY)
            ]
            .sort_values("timestamp", ascending=False)
            .head(VELOCITY_WINDOW)
        )

        if len(qualifying) < VELOCITY_WINDOW:
            return None

        rolling_avg = pd.to_numeric(qualifying["composite_zscore"], errors="coerce").mean()
        if pd.isna(rolling_avg):
            return None

        return round(current_zscore - rolling_avg, 4)

    # ── Full match pipeline ───────────────────────────────────────────────

    def process_match(
        self,
        all_mls_players:  list[PlayerStat],
        sounders_players: list[PlayerStat],
        match_id:         str,
        history:          pd.DataFrame,
        benchmark:        dict | None = None,
    ) -> list[PlayerInsight]:
        """
        Run the complete analytics pipeline for a single Sounders match.

        Args:
            all_mls_players:  Full MLS player pool (used to build benchmark
                              if one is not supplied).
            sounders_players: Player stats for Sounders players in this match.
            match_id:         Unique identifier for the match.
            history:          Existing time-series DataFrame (for Form Velocity).
            benchmark:        Pre-built benchmark dict; rebuilt if None.

        Returns:
            List of PlayerInsight objects, one per Sounders player.
        """
        bm = benchmark if benchmark else self.build_benchmark(all_mls_players)

        insights: list[PlayerInsight] = []

        for player in sounders_players:
            pos_group = POSITION_MAP.get(player.position_raw or "", None)
            if pos_group is None:
                pos_group = infer_position_from_stats(player.stats, player.is_gk)

            # Skip Z-score for players who didn't take the field — they have no
            # meaningful match stats and their presence would skew comparisons.
            if player.minutes == 0:
                insights.append(PlayerInsight(
                    player_id        = player.player_id,
                    player_name      = player.player_name,
                    position_raw     = player.position_raw,
                    position_group   = pos_group,
                    minutes_played   = player.minutes,
                    composite_zscore = None,
                    form_velocity    = None,
                    match_id         = match_id,
                    raw_stats        = player.stats,
                ))
                continue

            zscore   = self.compute_zscore(player, bm)
            velocity = None

            if zscore is not None:
                velocity = self.compute_form_velocity(player.player_id, zscore, history)

            insights.append(PlayerInsight(
                player_id        = player.player_id,
                player_name      = player.player_name,
                position_raw     = player.position_raw,
                position_group   = pos_group,
                minutes_played   = player.minutes,
                composite_zscore = zscore,
                form_velocity    = velocity,
                match_id         = match_id,
                raw_stats        = player.stats,
            ))

        return insights
