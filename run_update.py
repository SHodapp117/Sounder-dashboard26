#!/usr/bin/env python3
"""
run_update.py
─────────────
Main entry point for the Seattle Sounders Analytics Engine.

Usage:
    python run_update.py               # interactive mode
    python run_update.py --mock        # use synthetic data (no API calls)
    python run_update.py --probe       # test API connectivity only

Pipeline:
  1. Select match (interactive prompt or --mock flag)
  2. Load existing time-series history (for Form Velocity)
  3. Fetch / cache Global League Benchmark
  4. Fetch Sounders player stats for the selected match
  5. Compute positional Z-scores and Form Velocity
  6. Append results to /data/gold/sounders_timeseries.csv
  7. Print Match Report
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

# ── Make /src importable ──────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent / "src"))

from api_client      import SoundersDataClient
from analytics_engine import AnalyticsEngine, PlayerInsight
from storage_manager  import StorageManager
from config           import UNDERPERFORMER_Z_THRESHOLD


# ═════════════════════════════════════════════════════════════════════════════
# Display helpers
# ═════════════════════════════════════════════════════════════════════════════

def _banner() -> None:
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   Seattle Sounders FC  —  Analytics Engine  2026        ║")
    print(f"║   {datetime.now().strftime('%Y-%m-%d  %H:%M')}                                       ║")
    print("╚══════════════════════════════════════════════════════════╝")


def print_match_report(
    insights:   list[PlayerInsight],
    match_info: dict,
    is_mock:    bool = False,
) -> None:
    """Print a formatted Match Report to the console."""

    scored = [i for i in insights if i.composite_zscore is not None]
    scored.sort(key=lambda x: x.composite_zscore, reverse=True)

    top3          = scored[:3]
    underperformers = [i for i in scored if i.composite_zscore < UNDERPERFORMER_Z_THRESHOLD]

    mock_tag = " [MOCK]" if is_mock else ""
    date_str = str(match_info.get("date", ""))[:10]
    matchup  = f"{match_info.get('home','?')} vs {match_info.get('away','?')}"
    id_str   = str(match_info.get("id", "N/A"))

    W = 58  # inner box width (between │ characters)

    def _row(text: str) -> str:
        return f"│  {text:<{W - 2}}│"

    print()
    print("┌" + "─" * W + "┐")
    print(_row(f"MATCH REPORT{mock_tag}"))
    print(_row(matchup[:W - 2]))
    print(_row(f"{date_str}   ID: {id_str}"))
    div = "├" + "─" * W + "┤"

    # ── Top 3 performers ─────────────────────────────────────────────────
    print(div)
    print(_row("TOP 3 PERFORMERS  (Composite Positional Z-Score)"))
    print(_row("─" * (W - 2)))
    for rank, p in enumerate(top3, 1):
        z   = p.composite_zscore
        pos = f"[{p.position_group or '?':3s}]"
        vel_str = (f"  ▲{p.form_velocity:+.2f}" if p.form_velocity and p.form_velocity > 0
                   else f"  ▼{p.form_velocity:+.2f}" if p.form_velocity is not None
                   else "  Vel --")
        print(_row(f"{rank}. {p.player_name:<20} {pos}  Z={z:+.2f}{vel_str}"))

    # ── Underperformers ───────────────────────────────────────────────────
    print(div)
    print(_row(f"UNDERPERFORMERS  (Z < {UNDERPERFORMER_Z_THRESHOLD})"))
    print(_row("─" * (W - 2)))

    if not underperformers:
        print(_row("None — solid team performance across all positions."))
    else:
        for p in underperformers:
            z   = p.composite_zscore
            pos = f"[{p.position_group or '?':3s}]"
            vel_str = (f"  {p.form_velocity:+.2f}" if p.form_velocity is not None else "  --")
            print(_row(f"⚠  {p.player_name:<20} {pos}  Z={z:+.2f}{vel_str}"))

    # ── Full squad ranking ─────────────────────────────────────────────────
    print(div)
    print(_row("FULL SQUAD RANKING"))
    print(_row("─" * (W - 2)))
    for p in scored:
        z       = p.composite_zscore
        pos     = f"[{p.position_group or '?':3s}]"
        min_str = f"{p.minutes_played:.0f}'"
        print(_row(f"  {p.player_name:<21} {pos}  Z={z:+.2f}   {min_str}"))

    did_not_play = [i for i in insights if i.composite_zscore is None and i.minutes_played == 0]
    no_data      = [i for i in insights if i.composite_zscore is None and i.minutes_played > 0]

    if did_not_play:
        print(_row("─" * (W - 2)))
        print(_row("DID NOT PLAY:"))
        for p in did_not_play:
            print(_row(f"  {p.player_name:<21} pos={p.position_raw or '?'}"))

    if no_data:
        print(_row("─" * (W - 2)))
        print(_row("No Z-score (insufficient match data):"))
        for p in no_data:
            print(_row(f"  {p.player_name:<21} pos={p.position_raw or '?'}"))

    print("└" + "─" * W + "┘")
    print()


# ═════════════════════════════════════════════════════════════════════════════
# Interactive match selection
# ═════════════════════════════════════════════════════════════════════════════

def select_match(client: SoundersDataClient) -> dict | None:
    """
    Fetch completed Sounders matches and prompt the user to select one.
    Always offers [M] Mock data and [Q] Quit as options.
    Returns a match_info dict or None to abort.
    """
    print("\n[Step 1] Fetching Sounders schedule from ESPN…")
    schedule = client.get_sounders_schedule()

    # get_sounders_schedule() returns pre-parsed dicts:
    # {id, date, kickoff_utc, home, away, competition, status}
    completed: list[dict] = []
    for event in schedule:
        if event.get("status") != "completed":
            continue
        completed.append({
            "id":   str(event.get("id", "")),
            "date": str(event.get("date", ""))[:10],
            "home": event.get("home", "?"),
            "away": event.get("away", "?"),
        })

    print()
    print("─" * 58)
    print("  SELECT MATCH TO PROCESS")
    print("─" * 58)

    if completed:
        for idx, m in enumerate(completed, 1):
            print(f"  [{idx}]  {m['date']}  {m['home']}  vs  {m['away']}")
    else:
        print("  (No completed 2026 matches found — season starting soon)")

    print(f"  [M]  Use mock data  (synthetic test run, no CSV write)")
    print(f"  [Q]  Quit")
    print("─" * 58)

    while True:
        raw = input("  Your choice: ").strip().upper()

        if raw == "Q":
            print("  Exiting.")
            return None

        if raw == "M":
            return {
                "id":       "MOCK-001",
                "date":     datetime.now().strftime("%Y-%m-%d"),
                "home":     "Seattle Sounders FC",
                "away":     "Portland Timbers",
                "use_mock": True,
            }

        try:
            idx = int(raw) - 1
            if 0 <= idx < len(completed):
                return completed[idx]
        except ValueError:
            # Allow typing a raw match/event ID directly
            if raw:
                confirm = input(f"  Process match ID '{raw}'? [y/N]: ").strip().lower()
                if confirm == "y":
                    return {"id": raw, "date": "unknown", "home": "?", "away": "?"}

        print("  Invalid choice — try again.")


# ═════════════════════════════════════════════════════════════════════════════
# Main pipeline
# ═════════════════════════════════════════════════════════════════════════════

def main(args: argparse.Namespace) -> None:
    _banner()

    client  = SoundersDataClient()
    engine  = AnalyticsEngine()
    storage = StorageManager()

    # ── Probe-only mode ───────────────────────────────────────────────────
    if args.probe:
        client.probe_all()
        return

    # ── Match selection ───────────────────────────────────────────────────
    if args.mock:
        match_info = {
            "id":       "MOCK-001",
            "date":     datetime.now().strftime("%Y-%m-%d"),
            "home":     "Seattle Sounders FC",
            "away":     "Portland Timbers",
            "use_mock": True,
        }
        print(f"[Step 1] Mock mode — using synthetic match data.")
    elif args.match_id:
        match_info = {
            "id":   args.match_id,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "home": "Seattle Sounders FC",
            "away": "?",
        }
        print(f"[Step 1] Processing match ID {args.match_id} (non-interactive).")
    else:
        match_info = select_match(client)
        if match_info is None:
            return

    match_id = match_info["id"]
    use_mock = match_info.get("use_mock", False)

    # ── Idempotency check ─────────────────────────────────────────────────
    if not use_mock and storage.match_already_stored(match_id):
        print(f"\n[Run] Match {match_id} is already stored. Nothing to do.")
        print("      Delete the relevant rows from the gold CSV to re-process.\n")
        return

    # ── Load history (Form Velocity baseline) ────────────────────────────
    print("\n[Step 2] Loading historical time-series…")
    history = storage.load_history()
    print(f"         {len(history)} existing rows loaded.")

    # ── Benchmark ─────────────────────────────────────────────────────────
    print("\n[Step 3] Updating Global League Benchmark…")

    if use_mock:
        from mock_data import generate_mock_league_players
        all_mls_players = generate_mock_league_players()
        print(f"         [MOCK] {len(all_mls_players)} synthetic MLS players generated.")
        benchmark = engine.build_benchmark(all_mls_players)
        print(f"         Benchmark built for {len(benchmark)} position groups.")
    else:
        all_mls_players_raw = client.get_all_mls_player_stats()
        all_mls_players     = all_mls_players_raw or []

        if not all_mls_players:
            print("         [WARN] No league data available — Z-scores will be relative "
                  "only to mock/previous data.")

        benchmark = storage.load_or_rebuild_benchmark(
            lambda: engine.build_benchmark(all_mls_players)
        )
        print(f"         Benchmark covers {len(benchmark)} position groups.")

    # ── Fetch Sounders player stats ───────────────────────────────────────
    print(f"\n[Step 4] Fetching player stats for match {match_id}…")

    if use_mock:
        from mock_data import generate_mock_sounders_match
        sounders_players = generate_mock_sounders_match(match_id)
        print(f"         [MOCK] {len(sounders_players)} synthetic Sounders player records.")
    else:
        sounders_players = client.get_player_stats_for_match(match_id)
        if not sounders_players:
            print(f"         [ERROR] Could not retrieve stats for match {match_id}. Aborting.")
            return
        print(f"         {len(sounders_players)} player records retrieved.")

    # ── Compute analytics ─────────────────────────────────────────────────
    print(f"\n[Step 5] Computing Sounders analytics…")
    insights = engine.process_match(
        all_mls_players  = all_mls_players,
        sounders_players = sounders_players,
        match_id         = match_id,
        history          = history,
        benchmark        = benchmark,
    )

    scored = sum(1 for i in insights if i.composite_zscore is not None)
    print(f"         Z-scores computed for {scored}/{len(insights)} players.")

    # ── Persist to gold CSV ───────────────────────────────────────────────
    print(f"\n[Step 6] Appending to gold time-series…")
    if use_mock:
        print("         [MOCK] Skipping CSV write — run without --mock to persist.")
    else:
        storage.append_matchday(insights, match_id)

    # ── Match Report ──────────────────────────────────────────────────────
    print_match_report(insights, match_info, is_mock=use_mock)


# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Seattle Sounders Analytics Engine — match update runner"
    )
    parser.add_argument(
        "--mock",  action="store_true",
        help="Use synthetic data (no API calls, no CSV write)"
    )
    parser.add_argument(
        "--probe", action="store_true",
        help="Test API connectivity and exit"
    )
    parser.add_argument(
        "--match-id", dest="match_id", default=None,
        help="Process a specific ESPN match ID non-interactively (e.g. --match-id 761453)"
    )
    main(parser.parse_args())
