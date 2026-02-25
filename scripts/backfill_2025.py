#!/usr/bin/env python3
"""
scripts/backfill_2025.py
────────────────────────
One-time backfill of all 2025 Sounders matches into the gold CSV.

This seeds Form Velocity history so it fires from the very first 2026
match rather than waiting 3 matches to accumulate a baseline.

Run from the project root:
    .venv/bin/python scripts/backfill_2025.py

Flags:
    --dry-run   Fetch and compute analytics but do NOT write to the CSV
    --limit N   Process only the first N matches (useful for testing)
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ── Make src/ importable ──────────────────────────────────────────────────────
_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT / "src"))

from api_client       import SoundersDataClient
from analytics_engine import AnalyticsEngine
from storage_manager  import StorageManager


def main(dry_run: bool, limit: int | None) -> None:
    client  = SoundersDataClient()
    engine  = AnalyticsEngine()
    storage = StorageManager()

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   Sounders 2025 Season Backfill                         ║")
    if dry_run:
        print("║   DRY RUN — no CSV writes                               ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    # ── Step 1: fetch 2025 schedule ───────────────────────────────────────────
    print("[1/4] Fetching 2025 Sounders schedule from ESPN…")
    events = client.get_sounders_schedule(season=2025)
    print(f"      {len(events)} total events found.")

    # Filter to completed matches only
    completed = []
    for event in events:
        comps  = event.get("competitions", [{}])[0]
        status = comps.get("status", {}).get("type", {})
        if not status.get("completed", False):
            continue
        teams = comps.get("competitors", [])
        names = [t.get("team", {}).get("displayName", "?") for t in teams]
        raw_date = event.get("date", "")
        try:
            kickoff = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
        except ValueError:
            kickoff = datetime.min.replace(tzinfo=timezone.utc)
        completed.append({
            "id":      str(event.get("id", "")),
            "date":    raw_date[:10],
            "home":    names[0] if names else "?",
            "away":    names[1] if len(names) > 1 else "?",
            "kickoff": kickoff,
        })

    # Sort chronologically (oldest first) so Form Velocity accumulates correctly
    completed.sort(key=lambda m: m["kickoff"])
    if limit:
        completed = completed[:limit]

    print(f"      {len(completed)} completed matches to process.")
    if not completed:
        print("      Nothing to do.")
        return

    # ── Step 2: build 2025 MLS benchmark (once) ───────────────────────────────
    print("\n[2/4] Building 2025 MLS benchmark…")
    all_mls = client.get_all_mls_player_stats()
    if not all_mls:
        print("      [WARN] No MLS player data — Z-scores will be unreliable.")
    benchmark = engine.build_benchmark(all_mls)
    print(f"      Benchmark covers {len(benchmark)} position groups.")

    # ── Step 3: process each match ────────────────────────────────────────────
    print(f"\n[3/4] Processing {len(completed)} matches…")
    processed = 0
    skipped   = 0
    failed    = 0

    for i, match in enumerate(completed, 1):
        match_id = match["id"]
        label    = f"{match['date']}  {match['home']} vs {match['away']}"
        prefix   = f"  [{i:>2}/{len(completed)}]"

        # Idempotency check
        if storage.match_already_stored(match_id):
            print(f"{prefix} SKIP  {label}  (already stored)")
            skipped += 1
            continue

        # Fetch match stats
        sounders_players = client.get_player_stats_for_match(match_id)
        if not sounders_players:
            print(f"{prefix} FAIL  {label}  (no ESPN data)")
            failed += 1
            time.sleep(0.5)
            continue

        # Load the history accumulated so far (grows each iteration)
        history = storage.load_history()

        # Compute analytics
        insights = engine.process_match(
            all_mls_players  = [],           # benchmark already built
            sounders_players = sounders_players,
            match_id         = match_id,
            history          = history,
            benchmark        = benchmark,
        )

        n_scored   = sum(1 for ins in insights if ins.composite_zscore is not None)
        n_velocity = sum(1 for ins in insights if ins.form_velocity is not None)

        if dry_run:
            print(f"{prefix} DRY   {label}  "
                  f"({len(insights)} players, {n_scored} scored, {n_velocity} with velocity)")
        else:
            storage.append_matchday(insights, match_id)
            print(f"{prefix} OK    {label}  "
                  f"({len(insights)} players, {n_scored} scored, {n_velocity} with velocity)")
            processed += 1

        # Be polite to ESPN's API
        time.sleep(1.0)

    # ── Step 4: summary ───────────────────────────────────────────────────────
    print(f"\n[4/4] Done.")
    print(f"      Processed : {processed}")
    print(f"      Skipped   : {skipped}  (already in CSV)")
    print(f"      Failed    : {failed}   (no ESPN data)")
    if not dry_run:
        history = storage.load_history()
        print(f"      Gold CSV  : {len(history)} total rows")
        vel_rows = history["form_velocity"].notna().sum() if "form_velocity" in history else 0
        print(f"      Velocity  : {vel_rows} rows with Form Velocity populated")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill 2025 Sounders season into gold CSV"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute but do not write to CSV")
    parser.add_argument("--limit", type=int, default=None,
                        help="Process only the first N matches")
    args = parser.parse_args()
    main(dry_run=args.dry_run, limit=args.limit)
