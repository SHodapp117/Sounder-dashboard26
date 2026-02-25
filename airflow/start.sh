#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# airflow/start.sh
# Start the Airflow scheduler + webserver. Run from the project root:
#   bash airflow/start.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

PROJECT="$(cd "$(dirname "$0")/.." && pwd)"

export AIRFLOW_HOME="$PROJECT/airflow"
export PYTHONPATH="$PROJECT/src"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER="$AIRFLOW_HOME/dags"

# Load .env so SMTP vars are available to Airflow worker processes
if [ -f "$PROJECT/.env" ]; then
    set -a
    # shellcheck disable=SC1090
    source "$PROJECT/.env"
    set +a
fi

echo ""
echo "Starting Airflow (standalone)…"
echo "Web UI → http://localhost:8080  (admin / admin)"
echo "Ctrl-C to stop."
echo ""

exec "$PROJECT/.venv/bin/airflow" standalone
