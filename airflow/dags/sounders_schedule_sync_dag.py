"""
sounders_schedule_sync_dag.py
──────────────────────────────
Fetches the full Sounders season schedule from ESPN every Monday and
writes it to data/processed/match_schedule.json.

The analytics DAG reads this file to know exactly when matches are
scheduled, so it can target the pipeline ~24–48h after each kickoff
without polling ESPN every single day.

Trigger manually after first install to prime the schedule before
the next Monday automatic run.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Make src/ importable ──────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).parent.parent.parent
_SRC = str(_PROJECT_ROOT / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

log = logging.getLogger(__name__)

SCHEDULE_PATH  = _PROJECT_ROOT / "data" / "processed" / "match_schedule.json"
STANDINGS_PATH = _PROJECT_ROOT / "data" / "processed" / "standings.json"


# ═════════════════════════════════════════════════════════════════════════════
# Task
# ═════════════════════════════════════════════════════════════════════════════

def _sync_schedule(**context) -> None:
    """
    Fetch the Sounders season schedule from ESPN and write it to
    data/processed/match_schedule.json.

    Each entry records the match ID, kickoff time (UTC), local date,
    and team names.  Matches older than 14 days are dropped to keep the
    file focused on current and upcoming fixtures.
    """
    from api_client import SoundersDataClient

    client   = SoundersDataClient()
    now_utc  = datetime.now(timezone.utc)
    cutoff   = now_utc - timedelta(days=14)

    log.info("Scanning upcoming Sounders fixtures across all competitions…")
    schedule = client.get_sounders_schedule()

    # get_sounders_schedule() now returns pre-parsed dicts from the scoreboard
    # scan: {id, date, kickoff_utc, home, away, competition, status}
    matches = []
    for event in schedule:
        try:
            kickoff_utc = datetime.fromisoformat(
                event["kickoff_utc"].replace("Z", "+00:00")
            )
        except (KeyError, ValueError) as exc:
            log.warning("Skipping event with bad kickoff_utc: %s — %s", event, exc)
            continue

        # Drop matches more than 14 days in the past
        if kickoff_utc < cutoff:
            continue

        matches.append({
            "id":          event.get("id", ""),
            "kickoff_utc": kickoff_utc.isoformat(),
            "date_local":  event.get("date", kickoff_utc.date().isoformat()),
            "home":        event.get("home", "?"),
            "away":        event.get("away", "?"),
            "competition": event.get("competition", "MLS"),
        })

    matches.sort(key=lambda m: m["kickoff_utc"])

    payload = {
        "season":      2026,
        "last_synced": now_utc.isoformat(),
        "match_count": len(matches),
        "matches":     matches,
    }

    SCHEDULE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SCHEDULE_PATH, "w") as f:
        json.dump(payload, f, indent=2)

    log.info(
        "Schedule synced: %d matches written to %s",
        len(matches), SCHEDULE_PATH,
    )

    # Log upcoming matches for easy inspection in the Airflow UI
    upcoming = [m for m in matches if m["kickoff_utc"] > now_utc.isoformat()][:5]
    if upcoming:
        log.info("Next %d upcoming Sounders matches:", len(upcoming))
        for m in upcoming:
            log.info("  %s  %s vs %s  [%s]  (ID: %s)",
                     m["date_local"], m["home"], m["away"],
                     m.get("competition", "MLS"), m["id"])


def _sync_standings(**context) -> None:
    """
    Fetch Western Conference standings from ESPN and cache to
    data/processed/standings.json.

    Runs every Monday at 10 AM EST (15:00 UTC) so the standings widget
    in the dashboard always reflects the latest table without hitting the
    API on every page load.
    """
    from api_client import ESPNApiClient

    client   = ESPNApiClient()
    now_utc  = datetime.now(timezone.utc)

    log.info("Fetching Western Conference standings from ESPN…")
    rows = client.get_western_standings()

    if not rows:
        log.warning("No standings data returned — skipping write.")
        return

    payload = {
        "last_synced":   now_utc.isoformat(),
        "conference":    "Western",
        "entry_count":   len(rows),
        "standings":     rows,
    }

    STANDINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(STANDINGS_PATH, "w") as f:
        json.dump(payload, f, indent=2)

    log.info(
        "Standings synced: %d teams written to %s",
        len(rows), STANDINGS_PATH,
    )

    # Log top 5 for easy inspection in the Airflow UI
    for row in rows[:5]:
        log.info(
            "  #%d  %-20s  Pts=%d  GP=%d  W=%d  D=%d  L=%d  GD=%s",
            row["rank"], row["short_name"],
            row["pts"], row["gp"], row["w"], row["d"], row["l"], row["gd"],
        )


# ═════════════════════════════════════════════════════════════════════════════
# DAG definition
# ═════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id       = "sounders_schedule_sync",
    description  = "Weekly sync of Sounders match schedule and Western standings from ESPN",
    schedule     = "0 15 * * 1",   # every Monday at 10 AM EST (15:00 UTC)
    start_date   = datetime(2026, 2, 17),
    catchup      = False,
    default_args = {
        "owner":       "sounders",
        "retries":     2,
        "retry_delay": timedelta(minutes=5),
    },
    tags         = ["sounders", "schedule", "standings", "mls"],
) as dag:

    t_schedule = PythonOperator(
        task_id         = "sync_schedule",
        python_callable = _sync_schedule,
    )

    t_standings = PythonOperator(
        task_id         = "sync_standings",
        python_callable = _sync_standings,
    )

    # Run schedule first, then standings (both are fast; sequential for clarity)
    t_schedule >> t_standings
