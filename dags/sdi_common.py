"""Shared bits for the SDI DAGs (kept out of the DAG files to keep them declarative)."""
from __future__ import annotations

import json
import os
import sys
from datetime import timedelta
from pathlib import Path

# make the `pipeline` package importable when the repo is mounted next to dags/
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from airflow.sdk import Asset  # noqa: E402

LANDING_ASSET = Asset("sdi://landing_zone")        # emitted when the generator drops batch files
STREAM_ASSET = Asset("sdi://stream_queue")         # emitted when IoT events are queued for streaming
MART_ASSET = Asset("sdi://sdi_mart")               # emitted after the mart is refreshed (BI refresh hook)
DUCKDB_POOL = "duckdb_mart"   # size-1 pool: DuckDB allows one writer process, so mart tasks are serialised

DEFAULT_ARGS = {
    "owner": "sdi-team",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}


def record_task(dag_id: str, run_id: str, label: str, task: str, status: str, details: dict, started=None) -> None:
    from pipeline.db import pg_exec
    pg_exec("""INSERT INTO ops.pipeline_run (dag_id, dag_run_id, batch_label, task, started_at, finished_at, status, details)
               VALUES (%s,%s,%s,%s,coalesce(%s, now()),now(),%s,%s)""",
            (dag_id, run_id, label, task, started, status, json.dumps(details, default=str)))


def on_failure(context) -> None:
    ti = context["ti"]
    try:
        record_task(ti.dag_id, context["run_id"], "n/a", ti.task_id, "FAILED", {"exception": str(context.get("exception"))})
    except Exception:
        pass
