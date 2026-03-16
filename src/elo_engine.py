"""
elo_engine.py
─────────────
MLS team ELO ratings built from ESPN scoreboard history.

Enhanced ELO formula (soccer variant, v2):
    E_A = 1 / (1 + 10 ^ ((ELO_B - ELO_A) / 400))
    S_A = 1.0 (win) | 0.5 (draw) | 0.0 (loss)
    ELO_A_new = ELO_A + K_eff × (S_A - E_blended)

Enhancements over v1:
    K_eff     = K × e^(−λ × days_ago)           recency decay
    E_blended = (1−w)×E_elo + w×E_h2h           H2H blending
    w         = min(0.25, N_h2h / 40)            grows with H2H sample
    Season regression: ELO → 1500 + 0.67×(ELO − 1500) at each new season

Constants:
    K = 32    — standard for soccer
    BASE = 1500 — starting ELO for all teams

Usage:
    elo = EloEngine()
    elo.build(seasons=[2025, 2026])   # fetches ESPN scoreboard history
    ratings = elo.ratings             # {team_abbrev: float}
    elo.save()                        # → data/processed/team_elo.json
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from math import exp
from pathlib import Path
from typing import Any

from config import SEASON

_ELO_PATH     = Path(__file__).parent.parent / "data" / "processed" / "team_elo.json"
_K            = 32
_BASE_ELO     = 1500.0
_DECAY        = 0.003    # year-old match → K * e^(-0.003*365) ≈ K * 0.33
_REGRESSION   = 0.67     # season carry-over: 1500 + 0.67*(prev_elo − 1500)
_H2H_SAMPLE_CAP = 40    # N games for full H2H weight
_H2H_MAX_WEIGHT = 0.25  # H2H never exceeds 25% of expected value
_ALGO_VERSION = 2        # bumping forces cache rebuild on next dashboard load

# Season start/end windows (inclusive) for fetching ESPN scoreboard
_SEASON_WINDOWS: dict[int, tuple[str, str]] = {
    2025: ("2025-02-22", "2025-11-30"),
    2026: ("2026-02-21", "2026-12-31"),
}

# Full ESPN team name → ELO abbreviation (used by load_by_name)
_MLS_NAME_TO_ABBREV: dict[str, str] = {
    "Seattle Sounders FC":    "SEA",  "Portland Timbers":       "POR",
    "LA Galaxy":              "LA",   "Los Angeles FC":         "LAFC",
    "Real Salt Lake":         "RSL",  "Colorado Rapids":        "COL",
    "Sporting Kansas City":   "SKC",  "FC Dallas":              "DAL",
    "Houston Dynamo FC":      "HOU",  "Minnesota United FC":    "MIN",
    "Vancouver Whitecaps FC": "VAN",  "San Jose Earthquakes":   "SJ",
    "Austin FC":              "ATX",  "St. Louis City SC":      "STL",
    "San Diego FC":           "SD",   "Atlanta United FC":      "ATL",
    "Charlotte FC":           "CLT",  "Chicago Fire FC":        "CHI",
    "FC Cincinnati":          "CIN",  "Columbus Crew":          "CLB",
    "D.C. United":            "DC",   "Inter Miami CF":         "MIA",
    "CF Montréal":            "MTL",  "Nashville SC":           "NSH",
    "New England Revolution": "NE",   "New York City FC":       "NYC",
    "New York Red Bulls":     "NYRB", "Orlando City SC":        "ORL",
    "Philadelphia Union":     "PHI",  "Toronto FC":             "TOR",
}


def _expected(elo_a: float, elo_b: float) -> float:
    return 1.0 / (1.0 + 10 ** ((elo_b - elo_a) / 400.0))


def _score(home_goals: int, away_goals: int) -> tuple[float, float]:
    if home_goals > away_goals:
        return 1.0, 0.0
    if home_goals < away_goals:
        return 0.0, 1.0
    return 0.5, 0.5


class EloEngine:
    """
    Fetches MLS match results from ESPN and computes team ELO ratings.
    Results are cached in data/processed/team_elo.json.
    """

    def __init__(self) -> None:
        self.ratings: dict[str, float] = {}   # abbrev → ELO
        self._history: list[dict] = []         # ordered match log

    # ── Build ─────────────────────────────────────────────────────────────

    def build(self, seasons: list[int] | None = None) -> None:
        """
        Fetch completed MLS matches for the given seasons and compute ELO.
        Processes matches in chronological order so carry-over is correct.
        """
        from api_client import _get

        if seasons is None:
            seasons = [SEASON - 1, SEASON]

        all_matches: list[dict] = []

        for season in sorted(seasons):
            window = _SEASON_WINDOWS.get(season)
            if not window:
                print(f"[ELO] No date window configured for season {season} — skipping.")
                continue

            start = datetime.strptime(window[0], "%Y-%m-%d")
            end   = datetime.strptime(window[1], "%Y-%m-%d")
            # Cap end at today so we don't scan future weeks
            end   = min(end, datetime.now())

            print(f"[ELO] Fetching {season} MLS matches…")
            season_count = 0
            cur = start

            while cur <= end:
                nxt      = cur + timedelta(days=6)
                date_str = f"{cur.strftime('%Y%m%d')}-{nxt.strftime('%Y%m%d')}"
                data     = _get(
                    "https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1/scoreboard",
                    params={"dates": date_str},
                )
                if data:
                    for event in data.get("events", []):
                        parsed = self._parse_event(event)
                        if parsed:
                            parsed["season"] = season   # tag for regression boundary
                            all_matches.append(parsed)
                            season_count += 1
                cur = nxt + timedelta(days=1)

            print(f"[ELO]   {season_count} completed matches loaded.")

        # Sort all matches chronologically before processing
        all_matches.sort(key=lambda m: m["date"])
        print(f"[ELO] Processing {len(all_matches)} total matches…")
        self._compute(all_matches)

    def _parse_event(self, event: dict) -> dict | None:
        """Extract a minimal match record from an ESPN scoreboard event."""
        try:
            comp = event.get("competitions", [{}])[0]
            status = comp.get("status", {}).get("type", {})
            if not status.get("completed", False):
                return None

            competitors = comp.get("competitors", [])
            if len(competitors) < 2:
                return None

            # ESPN always returns home team first in MLS
            home = competitors[0]
            away = competitors[1]

            home_score = int(home.get("score", 0) or 0)
            away_score = int(away.get("score", 0) or 0)

            home_team = home.get("team", {})
            away_team = away.get("team", {})

            return {
                "date":        event.get("date", ""),
                "home_abbrev": home_team.get("abbreviation", "?"),
                "away_abbrev": away_team.get("abbreviation", "?"),
                "home_id":     str(home_team.get("id", "")),
                "away_id":     str(away_team.get("id", "")),
                "home_score":  home_score,
                "away_score":  away_score,
            }
        except Exception:
            return None

    def _compute(self, matches: list[dict]) -> None:
        """Apply ELO updates with recency decay, H2H blending, and season regression."""
        today = datetime.now(timezone.utc).replace(tzinfo=None)
        h2h: dict[tuple, dict] = {}   # (abbrev_a, abbrev_b) sorted → {w0, w1, draws}
        current_season: int | None = None

        for m in matches:
            # ── Season regression at boundary ──────────────────────────────
            if m.get("season") != current_season:
                if current_season is not None:
                    for t in self.ratings:
                        self.ratings[t] = round(
                            _BASE_ELO + _REGRESSION * (self.ratings[t] - _BASE_ELO), 1
                        )
                current_season = m["season"]

            h, a = m["home_abbrev"], m["away_abbrev"]
            elo_h = self.ratings.setdefault(h, _BASE_ELO)
            elo_a = self.ratings.setdefault(a, _BASE_ELO)

            # ── Recency: K decays with match age ───────────────────────────
            try:
                mdt      = datetime.fromisoformat(m["date"].replace("Z", "")).replace(tzinfo=None)
                days_ago = max(0, (today - mdt).days)
            except Exception:
                days_ago = 0
            K_eff = _K * exp(-_DECAY * days_ago)

            # ── H2H blended expected value ─────────────────────────────────
            key = tuple(sorted([h, a]))
            rec = h2h.setdefault(key, {"w0": 0, "w1": 0, "draws": 0})
            N   = rec["w0"] + rec["w1"] + rec["draws"]
            w   = min(_H2H_MAX_WEIGHT, N / _H2H_SAMPLE_CAP)

            e_h_elo = _expected(elo_h, elo_a)
            if N > 0:
                h_wins  = rec["w0"] if h == key[0] else rec["w1"]
                e_h_h2h = (h_wins + 0.5 * rec["draws"]) / N
            else:
                e_h_h2h = 0.5
            e_h = (1 - w) * e_h_elo + w * e_h_h2h
            e_a = 1.0 - e_h

            s_h, s_a = _score(m["home_score"], m["away_score"])
            self.ratings[h] = round(elo_h + K_eff * (s_h - e_h), 1)
            self.ratings[a] = round(elo_a + K_eff * (s_a - e_a), 1)

            # Update H2H record AFTER using it for this match
            if s_h == 1.0:
                if h == key[0]: rec["w0"] += 1
                else:           rec["w1"] += 1
            elif s_a == 1.0:
                if a == key[0]: rec["w0"] += 1
                else:           rec["w1"] += 1
            else:
                rec["draws"] += 1

            self._history.append({**m,
                "elo_home_after": self.ratings[h],
                "elo_away_after": self.ratings[a],
            })

        print(f"[ELO] Ratings computed for {len(self.ratings)} teams.")
        top5 = sorted(self.ratings.items(), key=lambda x: -x[1])[:5]
        print(f"[ELO] Top 5: {top5}")

    # ── Persistence ───────────────────────────────────────────────────────

    def save(self) -> None:
        _ELO_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "_meta": {
                "built_at":     datetime.now(timezone.utc).isoformat(),
                "k_factor":     _K,
                "base_elo":     _BASE_ELO,
                "decay":        _DECAY,
                "regression":   _REGRESSION,
                "algo_version": _ALGO_VERSION,
                "matches":      len(self._history),
            },
            "ratings":  self.ratings,
            "history":  self._history[-200:],  # keep last 200 for debugging
        }
        with open(_ELO_PATH, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"[ELO] Saved → {_ELO_PATH}")

    @staticmethod
    def load() -> dict[str, float] | None:
        """Return {abbrev: elo} from cache, or None if file missing."""
        if not _ELO_PATH.exists():
            return None
        try:
            with open(_ELO_PATH) as f:
                return json.load(f).get("ratings", {})
        except Exception:
            return None

    @staticmethod
    def load_by_name() -> dict[str, float]:
        """Return {full_team_name: elo} using the static name→abbrev map."""
        ratings = EloEngine.load() or {}
        return {
            name: ratings[abbrev]
            for name, abbrev in _MLS_NAME_TO_ABBREV.items()
            if abbrev in ratings
        }

    @staticmethod
    def is_stale(max_age_hours: int = 6) -> bool:
        """Return True if cache is missing, older than max_age_hours, or wrong algo version."""
        if not _ELO_PATH.exists():
            return True
        try:
            with open(_ELO_PATH) as f:
                raw = json.load(f)
            if raw.get("_meta", {}).get("algo_version") != _ALGO_VERSION:
                return True
            built = datetime.fromisoformat(raw["_meta"]["built_at"])
            age = datetime.now(timezone.utc) - built
            return age > timedelta(hours=max_age_hours)
        except Exception:
            return True
