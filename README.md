# Seattle Sounders Analytics Engine — 2026 Season

A local Python data pipeline that ingests MLS/ESPN player data, calculates positional Z-scores relative to the full MLS player pool, and maintains a match-by-match time-series for the Seattle Sounders FC 2026 season.

---

## Prerequisites

- Python 3.12
- Internet connection (ESPN public API, no auth needed)

---

## Setup

```bash
# 1. Create and activate virtual environment
python3.12 -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows

# 2. Install dependencies
pip install -r requirements.txt
```

---

## Triggering an Update

### Standard run (real match data)
```bash
python run_update.py
```
You will be prompted to select from a list of completed Sounders matches found via the ESPN API. Select a number and press Enter.

### Dry run with synthetic data
```bash
python run_update.py --mock
```
Runs the full pipeline using generated mock data. **Nothing is written to the gold CSV.** Use this to verify the engine is working correctly before the season starts.

### API connectivity test
```bash
python run_update.py --probe
```
Tests both the MLS Stats API and ESPN API and prints a status table.

---

## What Happens on Each Run

| Step | Description |
|------|-------------|
| 1 | Interactive match selector — shows all completed 2026 Sounders matches |
| 2 | Loads existing `/data/gold/sounders_timeseries.csv` for Form Velocity baseline |
| 3 | Fetches (or loads cached) Global League Benchmark — all MLS player stats, used to normalise Z-scores |
| 4 | Fetches per-player stats for the selected match |
| 5 | Computes Composite Positional Z-scores and Form Velocity for every Sounders player |
| 6 | Appends new rows to the gold CSV (idempotent — safe to re-run) |
| 7 | Prints Match Report to the console |

---

## Analytics Definitions

### Composite Positional Z-Score
For each player, stats are grouped by position `[GK, CB, FB, DM, CM, AM, FW]` and normalised against all MLS players at the same position:

```
Z = Σ( weight_i × (stat_i − μ_pos_i) / σ_pos_i )
```

Weights are defined per position in `src/config.py → POSITION_STATS`.

### Form Velocity
Measures whether a player is trending up or down:

```
Velocity = current_match_Z − rolling_avg_Z (last 3 qualifying matches)
```

A *qualifying match* is one where the player logged ≥ 30 minutes. This filters out brief substitute appearances that would distort the trend.

**Positive velocity** → improving relative to recent form
**Negative velocity** → declining form

*Form Velocity requires at least 3 qualifying appearances; shown as `--` until then.*

### Underperformer Threshold
Any player with `Z < -1.0` (more than one standard deviation below their positional peers) is flagged in the Match Report.

---

## Directory Structure

```
MVP Dashboard/
├── data/
│   ├── raw/          API JSON dumps (auto-saved for inspection)
│   ├── processed/    league_benchmark.json (24-hr cache)
│   └── gold/         sounders_timeseries.csv  ← SOURCE OF TRUTH
├── src/
│   ├── config.py           Constants, thresholds, position stat weights
│   ├── api_client.py       MLS Stats API + ESPN wrappers
│   ├── analytics_engine.py Z-score + Form Velocity logic
│   ├── storage_manager.py  CSV append + benchmark cache
│   └── mock_data.py        Synthetic data generator for testing
├── run_update.py     Main entry point
└── requirements.txt
```

---

## Data Sources

| Source | Status | Used For |
|--------|--------|----------|
| ESPN `site.api.espn.com` | ✅ Public, no auth | Schedule, roster, match summaries, team data |
| MLS Stats `stats-api.mlssoccer.com/v1/` | ⚠️ Auth required | Richer player stats (when credentials obtained) |

To add MLS Stats API credentials when available:
```bash
echo "MLS_API_TOKEN=your_token_here" > .env
```

---

## Adding a New Season

1. Update `SEASON = 2027` in `src/config.py`
2. The time-series CSV will naturally continue appending — history is preserved
3. Delete `/data/processed/league_benchmark.json` to force a fresh benchmark

---

## Gold CSV Schema

`/data/gold/sounders_timeseries.csv` columns:

| Column | Description |
|--------|-------------|
| `match_id` | ESPN event ID (or `MOCK-xxx` for test runs) |
| `timestamp` | UTC ISO timestamp of when the row was written |
| `player_id` | ESPN athlete ID |
| `player_name` | Full display name |
| `position_raw` | Position code from API (e.g. `CDM`, `ST`) |
| `position_group` | Canonical group (`DM`, `FW`, etc.) |
| `minutes_played` | Minutes on pitch |
| `composite_zscore` | Weighted positional Z-score |
| `form_velocity` | Trend vs 3-match rolling average |
| `goals`, `assists`, `xg`, … | Individual stat values |
