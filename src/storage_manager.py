"""
storage_manager.py
──────────────────
Handles all read/write operations for the time-series gold CSV and the
league benchmark cache.

Gold CSV  →  /data/gold/sounders_timeseries.csv
Benchmark →  /data/processed/league_benchmark.json

Append-only guarantee: match_already_stored() is checked before any write
so re-running run_update.py for the same match is always safe.

Benchmark cache: auto-refreshes if the file is older than BENCHMARK_MAX_AGE_HOURS.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from analytics_engine import PlayerInsight
from config import BENCHMARK_MAX_AGE_HOURS, ALL_STAT_KEYS

# ── Paths ─────────────────────────────────────────────────────────────────────

_ROOT            = Path(__file__).parent.parent
GOLD_DIR         = _ROOT / "data" / "gold"
PROCESSED_DIR    = _ROOT / "data" / "processed"
TIMESERIES_PATH  = GOLD_DIR / "sounders_timeseries.csv"
BENCHMARK_PATH   = PROCESSED_DIR / "league_benchmark.json"

# ── Column schema ─────────────────────────────────────────────────────────────
# Stat columns are derived from config.ALL_STAT_KEYS (real MLS API field names).
# This ensures the CSV schema stays in sync with the analytics engine config.

_STAT_COLUMNS: list[str] = ALL_STAT_KEYS

CORE_COLUMNS: list[str] = [
    "match_id", "timestamp", "player_id", "player_name",
    "position_raw", "position_group", "minutes_played",
    "composite_zscore", "form_velocity",
    *_STAT_COLUMNS,
]


# ═════════════════════════════════════════════════════════════════════════════

class StorageManager:

    # ── Gold CSV operations ───────────────────────────────────────────────

    def load_history(self) -> pd.DataFrame:
        """
        Load the full time-series CSV.
        Returns an empty DataFrame with the correct schema if the file
        does not exist yet.
        """
        if not TIMESERIES_PATH.exists():
            return pd.DataFrame(columns=CORE_COLUMNS)

        df = pd.read_csv(TIMESERIES_PATH, dtype={"match_id": str, "player_id": str})

        # Backfill any columns added after the file was first created
        for col in CORE_COLUMNS:
            if col not in df.columns:
                df[col] = None

        return df[CORE_COLUMNS]

    def match_already_stored(self, match_id: str) -> bool:
        """
        Return True if this match_id already has rows in the gold CSV.
        Prevents duplicate appends when run_update.py is called twice
        for the same match.
        """
        if not TIMESERIES_PATH.exists():
            return False
        try:
            df = pd.read_csv(TIMESERIES_PATH, usecols=["match_id"], dtype=str)
            return str(match_id) in df["match_id"].values
        except Exception:
            return False

    def append_matchday(
        self,
        insights: list[PlayerInsight],
        match_id: str,
    ) -> pd.DataFrame:
        """
        Append one row per Sounders player for the given match to the
        gold CSV.  Idempotent — silently skips if already stored.

        Returns the newly written DataFrame (empty if skipped).
        """
        if self.match_already_stored(match_id):
            print(f"[Storage] Match {match_id} already in gold CSV — skipping.")
            return pd.DataFrame()

        timestamp = datetime.now(timezone.utc).isoformat()
        rows: list[dict[str, Any]] = []

        for insight in insights:
            row: dict[str, Any] = {
                "match_id":        str(insight.match_id),
                "timestamp":       timestamp,
                "player_id":       str(insight.player_id),
                "player_name":     insight.player_name,
                "position_raw":    insight.position_raw,
                "position_group":  insight.position_group,
                "minutes_played":  insight.minutes_played,
                "composite_zscore": insight.composite_zscore,
                "form_velocity":   insight.form_velocity,
            }
            for stat in _STAT_COLUMNS:
                row[stat] = insight.raw_stats.get(stat)
            rows.append(row)

        new_df = pd.DataFrame(rows, columns=CORE_COLUMNS)

        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        write_header = not TIMESERIES_PATH.exists()

        new_df.to_csv(
            TIMESERIES_PATH,
            mode   = "w" if write_header else "a",
            header = write_header,
            index  = False,
        )

        print(f"[Storage] Appended {len(new_df)} player rows for match {match_id}.")
        return new_df

    # ── Benchmark cache operations ────────────────────────────────────────

    def benchmark_is_stale(self) -> bool:
        """
        Return True if the cached benchmark does not exist or is older
        than BENCHMARK_MAX_AGE_HOURS.
        """
        if not BENCHMARK_PATH.exists():
            return True
        mtime = datetime.fromtimestamp(BENCHMARK_PATH.stat().st_mtime, tz=timezone.utc)
        age   = datetime.now(timezone.utc) - mtime
        return age > timedelta(hours=BENCHMARK_MAX_AGE_HOURS)

    def load_benchmark(self) -> dict | None:
        """
        Load the cached benchmark.  Returns None if not present or stale
        (caller should rebuild and re-save).
        """
        if self.benchmark_is_stale():
            return None
        try:
            with open(BENCHMARK_PATH) as f:
                return json.load(f)
        except Exception as exc:
            print(f"[Storage] Could not read benchmark cache: {exc}")
            return None

    def save_benchmark(self, benchmark: dict) -> None:
        """
        Persist benchmark to /data/processed/league_benchmark.json.
        The file modification time acts as the cache timestamp.
        """
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        # Convert tuple values (mean, std) to lists for JSON serialisation
        serialisable = {
            group: {
                stat: list(values)
                for stat, values in stats.items()
            }
            for group, stats in benchmark.items()
        }
        with open(BENCHMARK_PATH, "w") as f:
            json.dump(serialisable, f, indent=2)
        print(f"[Storage] Benchmark saved → {BENCHMARK_PATH}")

    def load_or_rebuild_benchmark(
        self,
        rebuild_fn,  # callable() → dict  (e.g. engine.build_benchmark(players))
    ) -> dict:
        """
        Return the cached benchmark if fresh; otherwise call rebuild_fn()
        to generate a new one, save it, and return it.

        Usage:
            bm = storage.load_or_rebuild_benchmark(
                lambda: engine.build_benchmark(all_mls_players)
            )
        """
        cached = self.load_benchmark()
        if cached is not None:
            age_mins = int(
                (datetime.now(timezone.utc) -
                 datetime.fromtimestamp(BENCHMARK_PATH.stat().st_mtime, tz=timezone.utc)
                ).total_seconds() / 60
            )
            print(f"[Storage] Using cached benchmark ({age_mins} min old).")
            # Re-convert lists back to tuples so callers get consistent types
            return {
                group: {stat: tuple(v) for stat, v in stats.items()}
                for group, stats in cached.items()
            }

        print("[Storage] Benchmark cache missing or stale — rebuilding…")
        benchmark = rebuild_fn()
        self.save_benchmark(benchmark)
        return benchmark
