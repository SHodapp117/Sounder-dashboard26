"""
player_registry.py
──────────────────
Persisted ESPN_ID → MLS_ID mapping for the Sounders squad.

The ESPN Site API and MLS Stats API use different player ID systems
and are joined by normalised name-matching.  Name-matching runs once
per new ESPN athlete ID, the result is cached here, and all subsequent
merges become O(1) dict lookups keyed by the stable ESPN athlete ID.

Registry file: data/processed/player_registry.json
Schema:
    {
      "_meta": {"version": 1, "season": 2026, "updated_at": "…"},
      "entries": {
        "<espn_id>": {
          "mls_id":         "<MLS-OBJ-…>" | null,
          "canonical_name": "Display Name",
          "matched_via":    "name" | "manual" | null
        },
        …
      }
    }

mls_id = null  → ESPN player seen but no MLS record found
                  (new signing, loaned player, squad player)
matched_via = "manual"  → human-edited override in the JSON file;
                           registry will never overwrite these entries.
"""

from __future__ import annotations

import json
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import SEASON

_REGISTRY_PATH = (
    Path(__file__).parent.parent / "data" / "processed" / "player_registry.json"
)
_VERSION = 1


# ── Name normalisation (identical to the old api_client merge logic) ─────────

def _norm(name: str) -> str:
    """Collapse to ASCII-lowercase for fuzzy name matching."""
    nfkd = unicodedata.normalize("NFKD", name or "")
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()


# ═════════════════════════════════════════════════════════════════════════════

class PlayerRegistry:
    """
    ESPN-ID → MLS-ID player mapping, persisted across runs.

    Usage inside api_client.SoundersDataClient:

        registry = PlayerRegistry()           # loads existing JSON
        mls_by_id   = {p.player_id: p for p in mls_players}
        mls_by_name = {_norm(p.player_name): p for p in mls_players}

        for player in espn_players:
            mls_player = registry.resolve(
                player.player_id, player.player_name,
                mls_by_id, mls_by_name,
            )
            # merge stats…

        registry.save_if_updated()            # no-op if nothing changed
    """

    def __init__(self) -> None:
        # espn_id → {"mls_id", "canonical_name", "matched_via"}
        self._entries: dict[str, dict] = {}
        self._dirty   = False
        self._load()

    # ── Persistence ───────────────────────────────────────────────────────

    def _load(self) -> None:
        if not _REGISTRY_PATH.exists():
            return
        try:
            with open(_REGISTRY_PATH, encoding="utf-8") as f:
                raw = json.load(f)
            if raw.get("_meta", {}).get("season") != SEASON:
                print(f"[Registry] Season mismatch in cached file "
                      f"(expected {SEASON}) — starting fresh.")
                return
            self._entries = raw.get("entries", {})
            n_res   = sum(1 for e in self._entries.values() if e.get("mls_id"))
            n_unres = len(self._entries) - n_res
            print(f"[Registry] Loaded {n_res} resolved, {n_unres} unresolved mappings.")
        except Exception as exc:
            print(f"[Registry] Could not load registry: {exc} — starting empty.")

    def save_if_updated(self) -> None:
        """Write to disk only if new entries were added since last load/save."""
        if not self._dirty:
            return
        _REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "_meta": {
                "version":    _VERSION,
                "season":     SEASON,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            "entries": self._entries,
        }
        with open(_REGISTRY_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        n_res   = sum(1 for e in self._entries.values() if e.get("mls_id"))
        n_unres = len(self._entries) - n_res
        print(f"[Registry] Saved — {n_res} resolved, {n_unres} unresolved.")
        self._dirty = False

    # ── Lookup / resolution ────────────────────────────────────────────────

    def resolve(
        self,
        espn_id:      str,
        espn_name:    str,
        mls_by_id:    dict[str, Any],         # mls_player_id → PlayerStat
        mls_by_name:  dict[str, Any],         # _norm(name)   → PlayerStat
        position_raw: str | None = None,      # ESPN position code for this appearance
    ) -> Any | None:
        """
        Return the MLS PlayerStat for this ESPN athlete, or None.

        Fast path: registry hit → direct mls_by_id lookup (O(1)).
        Slow path: unseen ESPN ID → name-match → cache → return.

        'manual' entries are never overwritten so human corrections
        in the JSON file survive re-runs.
        """
        entry = self._entries.get(espn_id)

        if entry is not None:
            # Already in registry — honour the cached decision.
            # (mls_id may be null = known-unresolvable; don't retry.)
            mls_id = entry.get("mls_id")
            return mls_by_id.get(mls_id) if mls_id else None

        # ── First time seeing this ESPN ID ────────────────────────────────
        mls_player = mls_by_name.get(_norm(espn_name))
        stored_pos = position_raw if position_raw and position_raw not in self._SUB_CODES else None
        self._entries[espn_id] = {
            "mls_id":         mls_player.player_id if mls_player else None,
            "canonical_name": espn_name,
            "matched_via":    "name" if mls_player else None,
            "position_raw":   stored_pos,
        }
        self._dirty = True

        if mls_player:
            print(f"  [Registry] New mapping: '{espn_name}' "
                  f"→ {mls_player.player_id}")
        else:
            print(f"  [Registry] No MLS match for '{espn_name}' "
                  f"— Z-score will use ESPN stats only.")

        return mls_player

    # ── Position tracking ─────────────────────────────────────────────────

    _SUB_CODES = frozenset({"SUB", "BE", ""})  # codes ESPN uses for bench/unassigned

    def get_position(self, espn_id: str) -> str | None:
        """Return the stored position_raw for this ESPN player, if any."""
        return self._entries.get(espn_id, {}).get("position_raw")

    def update_position(self, espn_id: str, position_raw: str | None) -> None:
        """
        Store a confirmed (non-SUB) position code for this player.

        Called whenever a player starts a match and ESPN returns a real
        position code.  'manual' entries are never overwritten.
        Silently ignored if espn_id is not yet in the registry (the
        resolve() call will handle it on the same pass).
        """
        if not position_raw or position_raw in self._SUB_CODES:
            return
        entry = self._entries.get(espn_id)
        if entry is None:
            return  # resolve() hasn't run yet for this ID; it will set position too
        if entry.get("matched_via") == "manual" and entry.get("position_raw"):
            return  # honour manual overrides
        if entry.get("position_raw") != position_raw:
            entry["position_raw"] = position_raw
            self._dirty = True

    # ── Introspection ─────────────────────────────────────────────────────

    def unresolved(self) -> list[str]:
        """Return canonical names of players with no MLS ID mapping."""
        return [
            e["canonical_name"]
            for e in self._entries.values()
            if not e.get("mls_id")
        ]
