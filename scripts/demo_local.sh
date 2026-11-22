#!/usr/bin/env bash
# Full Milestone-1 demo WITHOUT docker: base load + 2 refreshes through Airflow (`airflow dags test`).
# Prereqs: Postgres running (SDI_PG_DSN), ingestion API running (uvicorn pipeline.ingestion.api:app),
#          Airflow env configured (AIRFLOW_HOME, DAGS_FOLDER=<repo>/dags, PYTHONPATH=<repo>).
set -euo pipefail
cd "$(dirname "$0")/.."
step() { echo; echo "=================== $* ==================="; }

step "0. schemas"
python -m pipeline.ingestion.ddl

step "1. BASE LOAD (instructor dataset 2023-2025)"
cp data/raw/*.csv data/landing/
airflow dags test sdi_batch_pipeline

for R in 1 2; do
  step "$((R+1)). REFRESH $R - oversample -> batch pipeline -> stream"
  airflow dags test sdi_oversampling -c "{\"refresh_id\": $R}"
  airflow dags test sdi_batch_pipeline
  airflow dags test sdi_stream_ingest
done

step "4. fault drill - schema-broken file must be rejected and logged"
head -50 data/raw/06_hospital_demand_data.csv | cut -d, -f1-10 > /tmp/fault_drill__06_hospital_demand_data.csv
curl -s -X POST "${SDI_INGEST_API_URL:-http://localhost:8000}/ingest/batch" -H 'content-type: application/json' \
     -d '{"path": "/tmp/fault_drill__06_hospital_demand_data.csv", "archive": false}' | head -c 200; echo

step "5. render dashboard previews"
python scripts/render_dashboards.py
