"""
sounders_analytics_dag.py
──────────────────────────
Airflow DAG: runs the Sounders Analytics Engine automatically,
~24 hours after each MLS match.

Schedule: noon daily.  The first task checks the local match schedule
(written by sounders_schedule_sync) for a match that kicked off 24–48h
ago.  On non-match days this short-circuits instantly without any API
calls.  Only when a match is in-window does it verify completion via
ESPN and proceed with the full pipeline.

Pipeline:
  find_unprocessed_match  →  build_benchmark  →  run_analytics  →  send_report
"""

from __future__ import annotations

import json
import logging
import os
import smtplib
import sys
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

# ── Make src/ importable inside Airflow worker processes ─────────────────────
_PROJECT_ROOT = Path(__file__).parent.parent.parent   # dags/ → airflow/ → project/
_SRC = str(_PROJECT_ROOT / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

_SCHEDULE_PATH = _PROJECT_ROOT / "data" / "processed" / "match_schedule.json"

log = logging.getLogger(__name__)


# ═════════════════════════════════════════════════════════════════════════════
# Task functions
# ═════════════════════════════════════════════════════════════════════════════

def _find_unprocessed_match(**context) -> bool:
    """
    ShortCircuitOperator task.

    Step 1 — local schedule check (fast, no API call):
      Reads data/processed/match_schedule.json (written by the
      sounders_schedule_sync DAG) and checks whether any match kicked
      off between 24h and 48h ago.  On non-match days this returns False
      in milliseconds.

    Step 2 — ESPN verification (only on match days):
      Confirms the match is marked completed by ESPN, then pushes
      match_info to XCom and returns True so downstream tasks run.

    Fallback: if match_schedule.json doesn't exist yet (sync hasn't run),
      falls back to polling ESPN directly so nothing breaks on first install.
    """
    from api_client import SoundersDataClient
    from storage_manager import StorageManager

    storage = StorageManager()
    now_utc = datetime.now(timezone.utc)
    window_start = now_utc - timedelta(hours=48)
    window_end   = now_utc - timedelta(hours=24)

    # ── Step 1: check local schedule file ────────────────────────────────────
    schedule_candidates = []

    if _SCHEDULE_PATH.exists():
        try:
            with open(_SCHEDULE_PATH) as f:
                saved = json.load(f)
            last_synced = saved.get("last_synced", "unknown")
            log.info("Using match schedule (last synced: %s)", last_synced)

            for match in saved.get("matches", []):
                try:
                    kickoff = datetime.fromisoformat(match["kickoff_utc"])
                except (KeyError, ValueError):
                    continue
                if window_start <= kickoff <= window_end:
                    match_id = match.get("id", "")
                    if match_id and not storage.match_already_stored(match_id):
                        schedule_candidates.append(match)

            if not schedule_candidates:
                log.info(
                    "No Sounders match in the 24–48h window — skipping. "
                    "(window: %s → %s)",
                    window_start.strftime("%Y-%m-%d %H:%M UTC"),
                    window_end.strftime("%Y-%m-%d %H:%M UTC"),
                )
                return False

        except Exception as exc:
            log.warning("Could not read match_schedule.json (%s) — falling back to ESPN poll.", exc)
            schedule_candidates = []   # triggers fallback below

    else:
        log.warning(
            "match_schedule.json not found — falling back to ESPN poll. "
            "Trigger sounders_schedule_sync to prime the schedule."
        )

    # ── Step 2: verify completion via ESPN ───────────────────────────────────
    client   = SoundersDataClient()
    espn_raw = client.get_sounders_schedule()

    # Build a lookup of ESPN completion status keyed by event ID
    espn_status: dict[str, dict] = {}
    for event in espn_raw:
        eid   = str(event.get("id", ""))
        comps = event.get("competitions", [{}])[0]
        teams = comps.get("competitors", [])
        names = [t.get("team", {}).get("displayName", "?") for t in teams]
        espn_status[eid] = {
            "completed": comps.get("status", {}).get("type", {}).get("completed", False),
            "home":      names[0] if names else "?",
            "away":      names[1] if len(names) > 1 else "?",
            "raw_date":  event.get("date", "")[:10],
        }

    # If we fell back (no schedule file), search all ESPN events in the window
    if not schedule_candidates:
        for event in espn_raw:
            raw_date = event.get("date", "")
            try:
                kickoff = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
            except ValueError:
                continue
            if window_start <= kickoff <= window_end:
                match_id = str(event.get("id", ""))
                if match_id and not storage.match_already_stored(match_id):
                    comps = event.get("competitions", [{}])[0]
                    teams = comps.get("competitors", [])
                    names = [t.get("team", {}).get("displayName", "?") for t in teams]
                    schedule_candidates.append({
                        "id":          match_id,
                        "kickoff_utc": kickoff.isoformat(),
                        "date_local":  raw_date[:10],
                        "home":        names[0] if names else "?",
                        "away":        names[1] if len(names) > 1 else "?",
                    })

    # Pick the most recent candidate that ESPN confirms is completed
    schedule_candidates.sort(key=lambda m: m["kickoff_utc"], reverse=True)
    for candidate in schedule_candidates:
        match_id = candidate["id"]
        info     = espn_status.get(match_id, {})
        if not info.get("completed", False):
            log.info("Match %s not yet marked complete by ESPN — skipping.", match_id)
            continue

        match_info = {
            "id":   match_id,
            "date": candidate.get("date_local") or info.get("raw_date", ""),
            "home": candidate.get("home", info.get("home", "?")),
            "away": candidate.get("away", info.get("away", "?")),
        }
        log.info(
            "Found unprocessed match: %s  %s vs %s  (ID: %s)",
            match_info["date"], match_info["home"], match_info["away"], match_id,
        )
        context["ti"].xcom_push(key="match_info", value=match_info)
        return True

    log.info("No completed unprocessed Sounders match found in the 24–48h window.")
    return False


def _build_benchmark(**context) -> None:
    """
    Fetch all MLS player season stats and rebuild the positional benchmark
    if the cached version is older than 24 hours.
    Uses the existing StorageManager.load_or_rebuild_benchmark() logic.
    """
    from api_client import SoundersDataClient
    from analytics_engine import AnalyticsEngine
    from storage_manager import StorageManager

    client  = SoundersDataClient()
    engine  = AnalyticsEngine()
    storage = StorageManager()

    all_players = client.get_all_mls_player_stats() or []
    if not all_players:
        log.warning("No MLS player data returned — benchmark will be empty or cached.")

    storage.load_or_rebuild_benchmark(
        lambda: engine.build_benchmark(all_players)
    )
    log.info("Benchmark ready.")


def _run_analytics(**context) -> None:
    """
    Fetch Sounders per-match stats from ESPN, run the Z-score / Form Velocity
    pipeline, append to the gold CSV, and push the formatted report to XCom.
    """
    from api_client import SoundersDataClient
    from analytics_engine import AnalyticsEngine
    from storage_manager import StorageManager

    ti         = context["ti"]
    match_info = ti.xcom_pull(key="match_info", task_ids="find_unprocessed_match")
    match_id   = match_info["id"]

    client  = SoundersDataClient()
    engine  = AnalyticsEngine()
    storage = StorageManager()

    # Fetch per-match Sounders stats (ESPN boxscore)
    sounders_players = client.get_player_stats_for_match(match_id)
    if not sounders_players:
        raise RuntimeError(
            f"No player stats returned for match {match_id}. "
            "Check ESPN API or match ID."
        )
    log.info("%d Sounders player records fetched.", len(sounders_players))

    # Load history and benchmark
    history   = storage.load_history()
    benchmark = storage.load_benchmark()
    if benchmark is None:
        log.warning("Benchmark cache missing — rebuilding inline.")
        all_players = client.get_all_mls_player_stats() or []
        benchmark   = engine.build_benchmark(all_players)

    # Compute analytics
    insights = engine.process_match(
        all_mls_players  = [],           # benchmark already built
        sounders_players = sounders_players,
        match_id         = match_id,
        history          = history,
        benchmark        = benchmark,
    )
    log.info(
        "Z-scores computed for %d/%d players.",
        sum(1 for i in insights if i.composite_zscore is not None),
        len(insights),
    )

    # Append to gold CSV
    storage.append_matchday(insights, match_id)

    # Format report
    report_text = _format_report(insights, match_info)
    log.info("\n%s", report_text)
    ti.xcom_push(key="report_text", value=report_text)


def _send_report(**context) -> None:
    """
    Log the report and email it using SMTP credentials from the .env file.
    If SMTP credentials are not configured, logs a warning and skips email.
    """
    from dotenv import load_dotenv

    # Load .env from project root
    env_path = Path(__file__).parent.parent.parent / ".env"
    load_dotenv(env_path)

    ti          = context["ti"]
    match_info  = ti.xcom_pull(key="match_info", task_ids="find_unprocessed_match")
    report_text = ti.xcom_pull(key="report_text", task_ids="run_analytics")

    log.info("=== MATCH REPORT ===\n%s", report_text)

    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASSWORD")
    to_addr   = os.getenv("REPORT_EMAIL_TO")

    if not all([smtp_host, smtp_user, smtp_pass, to_addr]):
        log.warning(
            "SMTP credentials not fully configured in .env — skipping email. "
            "Set SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, REPORT_EMAIL_TO."
        )
        return

    subject = (
        f"Sounders Analytics — {match_info['home']} vs {match_info['away']} "
        f"({match_info['date']})"
    )
    msg = MIMEText(report_text, "plain")
    msg["Subject"] = subject
    msg["From"]    = smtp_user
    msg["To"]      = to_addr

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        log.info("Report emailed to %s.", to_addr)
    except Exception as exc:
        log.error("Failed to send email: %s", exc)
        raise


# ═════════════════════════════════════════════════════════════════════════════
# Report formatter (returns string instead of printing)
# ═════════════════════════════════════════════════════════════════════════════

def _format_report(insights, match_info: dict) -> str:
    from config import UNDERPERFORMER_Z_THRESHOLD

    scored = [i for i in insights if i.composite_zscore is not None]
    scored.sort(key=lambda x: x.composite_zscore, reverse=True)

    top3            = scored[:3]
    underperformers = [i for i in scored if i.composite_zscore < UNDERPERFORMER_Z_THRESHOLD]

    W        = 58
    lines    = []
    date_str = str(match_info.get("date", ""))[:10]
    matchup  = f"{match_info.get('home','?')} vs {match_info.get('away','?')}"
    id_str   = str(match_info.get("id", "N/A"))

    def _row(text: str) -> str:
        return f"│  {text:<{W - 2}}│"

    lines.append("┌" + "─" * W + "┐")
    lines.append(_row("MATCH REPORT"))
    lines.append(_row(matchup[:W - 2]))
    lines.append(_row(f"{date_str}   ID: {id_str}"))
    div = "├" + "─" * W + "┤"

    lines.append(div)
    lines.append(_row("TOP 3 PERFORMERS  (Composite Positional Z-Score)"))
    lines.append(_row("─" * (W - 2)))
    for rank, p in enumerate(top3, 1):
        z   = p.composite_zscore
        pos = f"[{p.position_group or '?':3s}]"
        vel_str = (f"  ▲{p.form_velocity:+.2f}" if p.form_velocity and p.form_velocity > 0
                   else f"  ▼{p.form_velocity:+.2f}" if p.form_velocity is not None
                   else "  Vel --")
        lines.append(_row(f"{rank}. {p.player_name:<20} {pos}  Z={z:+.2f}{vel_str}"))

    lines.append(div)
    lines.append(_row(f"UNDERPERFORMERS  (Z < {UNDERPERFORMER_Z_THRESHOLD})"))
    lines.append(_row("─" * (W - 2)))
    if not underperformers:
        lines.append(_row("None — solid team performance across all positions."))
    else:
        for p in underperformers:
            z   = p.composite_zscore
            pos = f"[{p.position_group or '?':3s}]"
            vel_str = f"  {p.form_velocity:+.2f}" if p.form_velocity is not None else "  --"
            lines.append(_row(f"⚠  {p.player_name:<20} {pos}  Z={z:+.2f}{vel_str}"))

    lines.append(div)
    lines.append(_row("FULL SQUAD RANKING"))
    lines.append(_row("─" * (W - 2)))
    for p in scored:
        z       = p.composite_zscore
        pos     = f"[{p.position_group or '?':3s}]"
        min_str = f"{p.minutes_played:.0f}'"
        lines.append(_row(f"  {p.player_name:<21} {pos}  Z={z:+.2f}   {min_str}"))

    unscored = [i for i in insights if i.composite_zscore is None]
    if unscored:
        lines.append(_row("─" * (W - 2)))
        lines.append(_row("No Z-score (position unmapped):"))
        for p in unscored:
            lines.append(_row(f"  {p.player_name:<21} pos={p.position_raw or '?'}"))

    lines.append("└" + "─" * W + "┘")
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# DAG definition
# ═════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id          = "sounders_match_analytics",
    description     = "Auto-process Sounders match stats ~24h after each game",
    schedule        = "0 12 * * *",   # noon daily
    start_date      = datetime(2026, 2, 21),
    catchup         = False,
    default_args    = {
        "owner":          "sounders",
        "retries":        1,
        "retry_delay":    timedelta(minutes=10),
    },
    tags            = ["sounders", "analytics", "mls"],
) as dag:

    find_match = ShortCircuitOperator(
        task_id         = "find_unprocessed_match",
        python_callable = _find_unprocessed_match,
    )

    build_bm = PythonOperator(
        task_id         = "build_benchmark",
        python_callable = _build_benchmark,
    )

    run_analytics = PythonOperator(
        task_id         = "run_analytics",
        python_callable = _run_analytics,
    )

    send_report = PythonOperator(
        task_id         = "send_report",
        python_callable = _send_report,
    )

    find_match >> build_bm >> run_analytics >> send_report
