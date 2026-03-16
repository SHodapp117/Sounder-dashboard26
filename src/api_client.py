"""
api_client.py
─────────────
Data-fetching layer for the Sounders Analytics Engine.

Sources (in priority order):
  1. MLS Stats API  — stats-api.mlssoccer.com  (season-aggregate player stats)
                    — sportapi.mlssoccer.com    (paginated per-player data)
     No auth token required; Referer header is checked by the server.
     Endpoints confirmed 2026-02-17 via Playwright network interception.

  2. ESPN Site API  — site.api.espn.com         (schedule, match summaries)
     Fully public; no auth or special headers required.

Architecture:
  MLSApiClient      → season stats, benchmark data
  ESPNApiClient     → schedule, per-match player stats (box score)
  SoundersDataClient → composite façade used by run_update.py
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import httpx
from pydantic import BaseModel, Field

from config import (
    COMPETITION_MLS,
    SEASON,
    SEASON_ID_BY_YEAR,
    SOUNDERS_ID_MLS,
    SOUNDERS_ID_ESPN,
    MLS_STATS_BASE,
    MLS_SPORT_BASE,
    MLS_HEADERS,
    ESPN_SITE_BASE,
    ESPN_LEAGUE,
    REQUEST_TIMEOUT,
    REQUEST_RETRIES,
    RETRY_BACKOFF,
    MLS_PAGE_SIZE,
)

RAW_DATA_DIR = Path(__file__).parent.parent / "data" / "raw"


# ═══════════════════════════════════════════════════════════════════════════
# Pydantic models
# ═══════════════════════════════════════════════════════════════════════════

class PlayerStat(BaseModel):
    player_id:      str
    player_name:    str
    team_id:        str = ""
    position_raw:   str | None = None   # position code ("GK", "CB", etc.)
    is_gk:          bool = False         # from MLS goal_keeper field
    minutes:        float = 0.0          # match minutes (from ESPN)
    season_minutes: float = 0.0          # total season minutes (from MLS API)
    stats:          dict[str, Any] = Field(default_factory=dict)
    match_id:       str | None = None
    source:         str = "unknown"      # "mls" | "espn" | "mock"


class MatchInfo(BaseModel):
    match_id:  str
    date:      str
    home_team: str
    away_team: str
    source:    str = "unknown"


# ═══════════════════════════════════════════════════════════════════════════
# Shared HTTP helper
# ═══════════════════════════════════════════════════════════════════════════

def _get(
    url: str,
    params:  dict | None = None,
    headers: dict | None = None,
) -> dict | list | None:
    """Synchronous GET with retry. Returns parsed JSON or None on failure."""
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
                r = client.get(url, params=params, headers=headers, follow_redirects=True)

            if r.status_code == 200:
                return r.json()
            if r.status_code in (401, 403):
                print(f"  [AUTH] {url} → {r.status_code}")
                return None
            if r.status_code == 404:
                print(f"  [404] {url}")
                return None
            print(f"  [HTTP {r.status_code}] {url} (attempt {attempt})")

        except httpx.TimeoutException:
            print(f"  [TIMEOUT] {url} (attempt {attempt})")
        except httpx.RequestError as exc:
            print(f"  [NET ERR] {url}: {exc} (attempt {attempt})")

        if attempt < REQUEST_RETRIES:
            time.sleep(RETRY_BACKOFF)

    return None


def _dump_raw(filename: str, data: Any) -> None:
    """Save a raw API response to /data/raw/ for inspection."""
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DATA_DIR / filename
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ═══════════════════════════════════════════════════════════════════════════
# MLS Stats API Client
# ═══════════════════════════════════════════════════════════════════════════

class MLSApiClient:
    """
    Wraps the two confirmed MLS data endpoints:

    stats-api.mlssoccer.com  → season metadata, club stats
    sportapi.mlssoccer.com   → paginated player stats (all or by club)

    No authentication needed — just the Referer header.
    """

    def _get(self, url: str, params: dict | None = None) -> Any:
        return _get(url, params=params, headers=MLS_HEADERS)

    # ── Season resolution ──────────────────────────────────────────────────

    def get_season_id(self, year: int = SEASON) -> str | None:
        """
        Return the Opta season_id string for a given year (e.g. 'MLS-SEA-0001KA').
        Fetches the seasons list from the API; falls back to the hard-coded
        SEASON_ID_BY_YEAR dict if the network call fails.
        """
        data = self._get(f"{MLS_STATS_BASE}/competitions/{COMPETITION_MLS}/seasons")
        if data and "seasons" in data:
            for entry in data["seasons"]:
                if entry.get("season") == year:
                    return entry["season_id"]
        # Hard-coded fallback
        return SEASON_ID_BY_YEAR.get(year)

    # ── Player stats (paginated) ───────────────────────────────────────────

    def _fetch_player_page(
        self,
        season_id: str,
        page: int = 1,
        club_id: str | None = None,
    ) -> list[dict]:
        """Fetch one page of player stats from sportapi."""
        params: dict[str, Any] = {
            "pageSize": MLS_PAGE_SIZE,
            "page":     page,
        }
        if club_id:
            params["clubId"] = club_id

        url = (
            f"{MLS_SPORT_BASE}/stats/players"
            f"/competition/{COMPETITION_MLS}"
            f"/season/{season_id}"
            f"/order/goals/desc"
        )
        data = self._get(url, params=params)
        if isinstance(data, list):
            return data
        return []

    def get_all_player_stats(self, season_id: str) -> list[PlayerStat]:
        """
        Fetch ALL MLS player season stats (paginated).
        Used to build the Global League Benchmark.
        Returns a list of PlayerStat objects.
        """
        all_rows: list[dict] = []
        page = 1
        while True:
            batch = self._fetch_player_page(season_id, page=page)
            if not batch:
                break
            all_rows.extend(batch)
            if len(batch) < MLS_PAGE_SIZE:
                break   # last page
            page += 1

        print(f"  [MLS] {len(all_rows)} players fetched across {page} page(s).")
        if all_rows:
            _dump_raw(f"mls_players_all_{season_id}.json", all_rows)

        return [self._parse_player(row) for row in all_rows]

    def get_sounders_player_stats(self, season_id: str) -> list[PlayerStat]:
        """
        Fetch season stats for Sounders players only.
        Returns a list of PlayerStat objects.
        """
        rows: list[dict] = []
        page = 1
        while True:
            batch = self._fetch_player_page(season_id, page=page, club_id=SOUNDERS_ID_MLS)
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < MLS_PAGE_SIZE:
                break
            page += 1

        print(f"  [MLS] {len(rows)} Sounders players fetched.")
        if rows:
            _dump_raw(f"mls_sounders_{season_id}.json", rows)

        return [self._parse_player(row) for row in rows]

    # ── Club stats (team-level attacking / season totals) ──────────────────

    def get_club_stats(self, season_id: str) -> list[dict]:
        """
        Fetch season stats for every MLS club from stats-api.mlssoccer.com.

        Key fields returned:
            team_id, team_name, goals, shots_at_goal_sum, shots_on_target,
            xG, xG_efficiency, possession_ratio, clean_sheets, …

        Returns raw list of club dicts (one per team).
        """
        data = self._get(
            f"{MLS_STATS_BASE}/statistics/clubs"
            f"/competitions/{COMPETITION_MLS}"
            f"/seasons/{season_id}"
        )
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            # Response wraps results in "team_statistics" (confirmed 2026-02-23)
            return data.get("team_statistics", data.get("clubs", []))
        return []

    # ── Internal parser ───────────────────────────────────────────────────

    @staticmethod
    def _parse_player(row: dict) -> PlayerStat:
        """
        Convert one MLS API player row into a PlayerStat object.

        Position inference: the MLS API does not return a position code
        directly. It uses 'goal_keeper': True/False. For field players,
        we set position_raw to None and rely on the ESPN roster or
        mock_data to supply a code where needed. The analytics engine's
        POSITION_MAP will be extended as richer position data is found.
        """
        is_gk   = bool(row.get("goal_keeper", False))
        pos_raw = "GK" if is_gk else None

        # Minutes: MLS returns "HH:MM:SS" playing_time; convert to float
        playing_time = row.get("playing_time") or row.get("normalized_player_minutes", 0)
        if isinstance(playing_time, str) and ":" in playing_time:
            parts   = playing_time.split(":")
            minutes = float(parts[0]) * 60 + float(parts[1]) + float(parts[2]) / 60
        else:
            minutes = float(playing_time or 0)

        # Name logic: prefer alias (often the player's known display name),
        # fall back to "first last". Alias sometimes duplicates first name
        # (e.g., "Nouhou" → alias="Nouhou", first="Nouhou") so guard against that.
        first = row.get("player_first_name", "") or ""
        last  = row.get("player_last_name", "") or ""
        alias = row.get("player_alias") or ""
        if alias and alias != first and alias != last and not alias.startswith(first + " " + first):
            name = alias
        else:
            name = f"{first} {last}".strip()
        name = name or "Unknown"

        return PlayerStat(
            player_id    = str(row.get("player_id", "unknown")),
            player_name  = name,
            team_id      = str(row.get("team_id", "")),
            position_raw = pos_raw,
            is_gk        = is_gk,
            minutes      = minutes,
            stats        = {k: v for k, v in row.items()
                            if k not in ("player_id", "player_first_name",
                                         "player_last_name", "player_alias",
                                         "team_id", "team_short_name",
                                         "team_three_letter_code",
                                         "playing_time", "goal_keeper",
                                         "normalized_player_minutes",
                                         "competition_id", "season",
                                         "season_id", "advanced_stats",
                                         "xg_rankings")},
            match_id     = None,
            source       = "mls",
        )

    # ── Connectivity probe ─────────────────────────────────────────────────

    def probe(self) -> dict[str, bool]:
        """Test MLS API connectivity and print a status table."""
        print("\n─── MLS API Probe ──────────────────────────────────────────────")
        season_id = self.get_season_id(SEASON)
        season_id_prev = self.get_season_id(SEASON - 1)

        tests: dict[str, str] = {
            "seasons":           f"{MLS_STATS_BASE}/competitions/{COMPETITION_MLS}/seasons",
            f"players_{SEASON}": (
                f"{MLS_SPORT_BASE}/stats/players/competition/{COMPETITION_MLS}"
                f"/season/{season_id}/order/goals/desc"
            ) if season_id else "N/A",
            f"players_{SEASON-1}": (
                f"{MLS_SPORT_BASE}/stats/players/competition/{COMPETITION_MLS}"
                f"/season/{season_id_prev}/order/goals/desc"
            ) if season_id_prev else "N/A",
        }

        results: dict[str, bool] = {}
        for name, url in tests.items():
            if url == "N/A":
                print(f"  SKIP  {name} (no season_id)")
                results[name] = False
                continue
            data = self._get(url, params={"pageSize": 1, "page": 1})
            ok   = data is not None and (isinstance(data, list) and len(data) > 0
                                         or isinstance(data, dict))
            results[name] = ok
            print(f"  {'OK  ' if ok else 'FAIL'}  {url[:90]}")

        print("────────────────────────────────────────────────────────────────\n")
        return results


# ═══════════════════════════════════════════════════════════════════════════
# ESPN Site API Client
# ═══════════════════════════════════════════════════════════════════════════

class ESPNApiClient:
    """
    Public ESPN API — no auth required.
    Used for: schedule, roster, per-match box scores.
    """

    def _url(self, *parts: str) -> str:
        return f"{ESPN_SITE_BASE}/{ESPN_LEAGUE}/{'/'.join(parts)}"

    # ── Schedule ──────────────────────────────────────────────────────────

    def get_team_schedule(self, team_id: str = SOUNDERS_ID_ESPN, season: int = SEASON) -> list[dict]:
        """Return all events (matches) for the given team and season."""
        data = _get(self._url("teams", team_id, "schedule"), params={"season": season})
        if data:
            _dump_raw(f"espn_schedule_{team_id}.json", data)
            return data.get("events", [])
        return []

    def get_completed_matches(self, team_id: str = SOUNDERS_ID_ESPN) -> list[dict]:
        """Return only completed matches, most recent first."""
        events = self.get_team_schedule(team_id)
        completed = [
            e for e in events
            if e.get("competitions", [{}])[0]
                .get("status", {}).get("type", {}).get("completed", False)
        ]
        completed.sort(key=lambda e: e.get("date", ""), reverse=True)
        return completed

    # Competition leagues to scan for Sounders fixtures
    _FIXTURE_LEAGUES: list[tuple[str, str]] = [
        ("MLS",           "usa.1"),
        ("US Open Cup",   "usa.open"),
        ("CCL",           "concacaf.leagues.cup"),
    ]

    def get_upcoming_sounders_fixtures(
        self,
        team_id: str = SOUNDERS_ID_ESPN,
        n_weeks: int = 12,
    ) -> list[dict]:
        """
        Scan ESPN scoreboards for all competitions over the next *n_weeks* weeks
        and return Sounders fixtures as a list of dicts, sorted by date:

          {id, date, kickoff_utc, home, away, competition, status}

        Uses scoreboard (not team schedule) so it catches fixtures across all
        competitions as soon as ESPN publishes them.
        """
        from datetime import datetime, timedelta, timezone

        now     = datetime.now(timezone.utc)
        results: list[dict] = []
        seen:    set[str]   = set()

        for comp_label, league in self._FIXTURE_LEAGUES:
            scoreboard_url = (
                f"{ESPN_SITE_BASE}/{league}/scoreboard"
            )
            for week_offset in range(n_weeks):
                # Use a 7-day range so we don't miss matches that fall
                # on days other than the anchor day.
                week_start = now + timedelta(weeks=week_offset)
                week_end   = week_start + timedelta(days=6)
                date_str   = (
                    f"{week_start.strftime('%Y%m%d')}"
                    f"-{week_end.strftime('%Y%m%d')}"
                )
                data = _get(scoreboard_url, params={"dates": date_str})
                if not data:
                    continue
                for event in data.get("events", []):
                    event_id = str(event.get("id", ""))
                    if event_id in seen:
                        continue
                    comp = event.get("competitions", [{}])[0]
                    competitors = comp.get("competitors", [])
                    team_ids = [str(t.get("team", {}).get("id", "")) for t in competitors]
                    if team_id not in team_ids:
                        continue
                    seen.add(event_id)
                    names = [t.get("team", {}).get("displayName", "?") for t in competitors]
                    status = comp.get("status", {}).get("type", {}).get("name", "")
                    results.append({
                        "id":           event_id,
                        "date":         event.get("date", "")[:10],
                        "kickoff_utc":  event.get("date", ""),
                        "home":         names[0] if names else "?",
                        "away":         names[1] if len(names) > 1 else "?",
                        "competition":  comp_label,
                        "status":       status,
                    })

        results.sort(key=lambda x: x["kickoff_utc"])
        return results

    def get_scoreboard(self) -> list[dict]:
        """Return today's MLS scoreboard events."""
        data = _get(self._url("scoreboard"))
        if data:
            _dump_raw("espn_scoreboard.json", data)
            return data.get("events", [])
        return []

    # ── Match summary (per-match player stats) ────────────────────────────

    def get_match_summary(self, event_id: str) -> dict | None:
        """
        Fetch ESPN match summary — includes boxscore with per-player stats.
        This is the primary source for per-match individual statistics.
        """
        data = _get(self._url("summary"), params={"event": event_id})
        if data:
            _dump_raw(f"espn_summary_{event_id}.json", data)
        return data

    # Map ESPN roster stat names → MLS API / analytics engine stat names
    _ESPN_STAT_MAP: dict[str, str] = {
        "totalGoals":    "goals",
        "goalAssists":   "assists",
        "saves":         "goalkeeper_saves",
        "goalsConceded": "goals_conceded",
        "shotsOnTarget": "shots_on_target",
        "totalShots":    "total_shots",
        "shotsFaced":    "shots_faced",
        "foulsCommitted":"fouls_against_opponent",
        "yellowCards":   "yellow_cards",
        "redCards":      "red_cards",
        "ownGoals":      "own_goals",
        "appearances":   "appearances",
        "subIns":        "sub_ins",
    }

    def parse_match_player_stats(
        self,
        summary: dict,
        event_id: str,
        team_id:  str = SOUNDERS_ID_ESPN,
    ) -> list[PlayerStat]:
        """
        Extract Sounders player stats from an ESPN match summary response.

        ESPN embeds player stats inside:
          rosters[{team}] → roster[{player}] → stats[{stat: name/value}]

        Position code lives at player_entry['position'], not athlete['position'].
        Stat names are normalized to match the MLS API / analytics engine schema.
        """
        players: list[PlayerStat] = []
        try:
            for team_entry in summary.get("rosters", []):
                block_team_id = str(team_entry.get("team", {}).get("id", ""))
                if block_team_id != str(team_id):
                    continue

                for player_entry in team_entry.get("roster", []):
                    athlete   = player_entry.get("athlete", {})
                    # Position lives at the player_entry level, not athlete level
                    pos_code  = (player_entry.get("position") or {}).get("abbreviation")
                    starter    = player_entry.get("starter", False)
                    subbed_in  = player_entry.get("subbedIn", False)
                    subbed_out = player_entry.get("subbedOut", False)

                    # Normalize ESPN stat names to match MLS API / analytics schema
                    stat_dict: dict[str, float] = {
                        self._ESPN_STAT_MAP.get(s["name"], s["name"]): float(s["value"])
                        for s in player_entry.get("stats", [])
                        if "name" in s and "value" in s
                    }

                    # Estimate minutes (ESPN roster doesn't include exact sub times).
                    # Starters subbed out are estimated at 65 min (MLS sub average).
                    # Subs coming on are estimated at 30 min.
                    if starter and not subbed_out:
                        minutes = 90.0
                    elif starter and subbed_out:
                        minutes = 65.0
                    elif subbed_in:
                        minutes = 30.0
                    else:
                        minutes = 0.0

                    players.append(PlayerStat(
                        player_id    = str(athlete.get("id", "unknown")),
                        player_name  = athlete.get("displayName", "Unknown"),
                        team_id      = block_team_id,
                        position_raw = pos_code,
                        is_gk        = (pos_code in ("GK", "G", "Goalkeeper")),
                        minutes      = minutes,
                        stats        = stat_dict,
                        match_id     = event_id,
                        source       = "espn",
                    ))
        except Exception as exc:
            print(f"  [PARSE ERR] ESPN summary parse: {exc}")
        return players

    # ── Standings ─────────────────────────────────────────────────────────

    _ESPN_STANDINGS_URL = (
        "https://site.api.espn.com/apis/v2/sports/soccer/usa.1/standings"
    )

    def get_western_standings(self) -> list[dict]:
        """
        Fetch Western Conference standings from ESPN.

        Returns a list of dicts sorted by rank:
          {rank, team_id, short_name, abbrev, pts, gp, w, d, l, gd}
        """
        data = _get(self._ESPN_STANDINGS_URL)
        if not data:
            return []

        for conf in data.get("children", []):
            if "West" not in conf.get("name", ""):
                continue
            entries = conf.get("standings", {}).get("entries", [])
            result  = []
            for e in entries:
                team  = e.get("team", {})
                stats = {s["name"]: s for s in e.get("stats", [])}
                result.append({
                    "rank":       int(stats.get("rank",            {}).get("value", 99)),
                    "team_id":    str(team.get("id", "")),
                    "short_name": team.get("shortDisplayName", "?"),
                    "abbrev":     team.get("abbreviation", "?"),
                    "pts":        int(stats.get("points",           {}).get("value", 0)),
                    "gp":         int(stats.get("gamesPlayed",      {}).get("value", 0)),
                    "w":          int(stats.get("wins",             {}).get("value", 0)),
                    "d":          int(stats.get("ties",             {}).get("value", 0)),
                    "l":          int(stats.get("losses",           {}).get("value", 0)),
                    "gd":         stats.get("pointDifferential",    {}).get("displayValue", "0"),
                })
            return sorted(result, key=lambda x: x["rank"])

        return []

    # ── Probe ─────────────────────────────────────────────────────────────

    def probe(self) -> dict[str, bool]:
        print("\n─── ESPN API Probe ─────────────────────────────────────────────")
        tests = {
            "scoreboard":   self._url("scoreboard"),
            "teams":        self._url("teams"),
            "sfc_schedule": self._url("teams", SOUNDERS_ID_ESPN, "schedule"),
        }
        results: dict[str, bool] = {}
        for name, url in tests.items():
            data = _get(url, params={"season": SEASON} if "schedule" in url else None)
            ok   = data is not None
            results[name] = ok
            print(f"  {'OK  ' if ok else 'FAIL'}  {url}")
        print("────────────────────────────────────────────────────────────────\n")
        return results


# ═══════════════════════════════════════════════════════════════════════════
# Composite Sounders Client  (façade used by run_update.py)
# ═══════════════════════════════════════════════════════════════════════════

class SoundersDataClient:
    """
    High-level client used by run_update.py.

    Data strategy:
      Benchmark (all MLS players)  → MLS Stats API (season aggregates, paginated)
      Sounders season stats         → MLS Stats API (filtered by clubId)
      Per-match player stats        → ESPN match summary (box score)
      Schedule / completed matches  → ESPN schedule endpoint
    """

    def __init__(self) -> None:
        self.mls      = MLSApiClient()
        self.espn     = ESPNApiClient()
        self._season_id: str | None = None
        from player_registry import PlayerRegistry
        self.registry = PlayerRegistry()

    def _get_season_id(self) -> str | None:
        if self._season_id is None:
            self._season_id = self.mls.get_season_id(SEASON)
            if self._season_id:
                print(f"  [MLS] 2026 season_id = {self._season_id}")
            else:
                print(f"  [MLS] Could not resolve 2026 season_id — data may not be loaded yet.")
        return self._season_id

    # ── Probes ────────────────────────────────────────────────────────────

    def probe_all(self) -> dict[str, dict[str, bool]]:
        return {"mls": self.mls.probe(), "espn": self.espn.probe()}

    # ── League benchmark ──────────────────────────────────────────────────

    def get_all_mls_player_stats(self) -> list[PlayerStat]:
        """
        Return season-aggregate stats for every MLS player.
        Used to build the Global League Benchmark for Z-score normalization.

        Falls back to 2025 data if 2026 season data isn't populated yet.
        """
        print("[Client] Fetching all MLS player stats for benchmark…")
        season_id = self._get_season_id()

        if season_id:
            players = self.mls.get_all_player_stats(season_id)
            if players:
                return players
            print(f"  [MLS] 2026 season returned no players — trying 2025 as fallback.")

        # 2026 data not yet populated — use 2025 to validate the pipeline
        season_id_2025 = self.mls.get_season_id(SEASON - 1)
        if season_id_2025:
            print(f"  [MLS] Using 2025 benchmark (season_id={season_id_2025}).")
            return self.mls.get_all_player_stats(season_id_2025)

        print("  [MLS] No player stats available from MLS API.")
        return []

    # ── Sounders schedule ─────────────────────────────────────────────────

    def get_sounders_schedule(self, season: int = SEASON) -> list[dict]:
        """
        Return Sounders fixtures from ESPN across all competitions.

        Uses a scoreboard scan (multiple competitions × upcoming weeks) so
        the returned list covers MLS, CCL, US Open Cup, etc. as soon as
        ESPN publishes them — not just the team-schedule endpoint which
        often only shows 1–2 matches at a time early in the season.
        """
        print(f"[Client] Scanning upcoming Sounders fixtures across all competitions…")
        return self.espn.get_upcoming_sounders_fixtures()

    def get_completed_sounders_matches(self) -> list[dict]:
        """Return completed Sounders matches, most recent first."""
        return self.espn.get_completed_matches()

    # ── Per-match player stats ────────────────────────────────────────────

    def get_player_stats_for_match(
        self,
        match_id: str,
    ) -> list[PlayerStat] | None:
        """
        Fetch per-match player stats, merging two sources:

          1. ESPN match summary  — minutes, position, goals, assists, saves,
                                   shots on target, cards.
          2. MLS Stats API       — passes_conversion_rate, xG, chances,
                                   interceptions, crosses, etc. (season
                                   aggregates; after match 1 these equal
                                   the per-match values).

        ESPN wins on any stat it reports directly; MLS API fills the gaps.
        The merge key is the ESPN athlete ID, resolved via PlayerRegistry
        (name-matching runs once per new ID, then cached in player_registry.json).
        """
        from player_registry import _norm

        print(f"[Client] Fetching match summary for event {match_id} (ESPN)…")
        summary = self.espn.get_match_summary(match_id)
        if not summary:
            print(f"  [ESPN] No summary returned for event {match_id}.")
            return None

        players = self.espn.parse_match_player_stats(summary, match_id)
        if not players:
            print(f"  [ESPN] No Sounders player stats parsed from summary.")
            return None

        print(f"  [ESPN] {len(players)} Sounders player records.")

        # ── Merge MLS API season stats ─────────────────────────────────────
        print(f"[Client] Fetching MLS API season stats to supplement ESPN data…")
        season_id   = self.mls.get_season_id(SEASON)
        mls_players = self.mls.get_sounders_player_stats(season_id) if season_id else []

        if mls_players:
            mls_by_id:   dict[str, PlayerStat] = {p.player_id:           p for p in mls_players}
            mls_by_name: dict[str, PlayerStat] = {_norm(p.player_name):  p for p in mls_players}

            merged = 0
            for player in players:
                # ── Position hygiene ──────────────────────────────────────
                # ESPN labels all bench players "SUB" regardless of role.
                # For starters, store the real position code so it's
                # available for future matches when they come on as a sub.
                if player.position_raw and player.position_raw not in ("SUB", "BE"):
                    self.registry.update_position(player.player_id, player.position_raw)
                elif player.position_raw in ("SUB", "BE", None):
                    stored = self.registry.get_position(player.player_id)
                    if stored:
                        player.position_raw = stored

                # Don't supplement bench players (0 min) with season stats —
                # their MLS aggregate numbers don't reflect this match.
                if player.minutes == 0:
                    continue
                mls_player = self.registry.resolve(
                    player.player_id, player.player_name, mls_by_id, mls_by_name,
                    position_raw=player.position_raw,
                )
                if not mls_player:
                    continue  # message already printed by registry
                # Carry over season minutes so compute_zscore can normalize
                # MLS counting stats by total season minutes (not match minutes).
                player.season_minutes = mls_player.minutes
                # ESPN takes precedence: only fill stats that ESPN left absent
                for k, v in mls_player.stats.items():
                    if k not in player.stats or player.stats[k] is None:
                        player.stats[k] = v
                merged += 1

            played = sum(1 for p in players if p.minutes > 0)
            print(f"  [MLS]  Season stats merged for {merged}/{played} players who played.")
            self.registry.save_if_updated()
        else:
            print(f"  [MLS]  No season stats available — using ESPN only.")

        return players


# ═══════════════════════════════════════════════════════════════════════════
# CLI connectivity test
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 65)
    print("  Sounders Analytics Engine — API Connectivity Test")
    print("=" * 65)

    client  = SoundersDataClient()
    results = client.probe_all()

    print("\n╔═══════════════════════════════════════════════════╗")
    print("║  Probe Summary                                    ║")
    print("╠═══════════════════════════════════════════════════╣")
    for api, endpoints in results.items():
        for ep, ok in endpoints.items():
            badge = "✓" if ok else "✗"
            print(f"║  {badge}  [{api.upper():4s}]  {ep:<38}║")
    print("╚═══════════════════════════════════════════════════╝")

    # Quick live data check
    print("\n[Live check] Fetching all MLS player benchmark …")
    players = client.get_all_mls_player_stats()
    if players:
        p = players[0]
        print(f"  Sample player: {p.player_name}  ({p.team_id})")
        print(f"  Stats keys: {list(p.stats.keys())[:8]}")
        print(f"  Total players: {len(players)}")
    print()
