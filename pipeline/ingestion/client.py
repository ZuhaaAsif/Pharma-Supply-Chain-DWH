"""How Airflow tasks talk to the ingestion layer: HTTP to the FastAPI service by default
(SDI_INGEST_MODE=api), or direct function calls (SDI_INGEST_MODE=inprocess) for tests."""
from __future__ import annotations

import requests

from pipeline import config
from pipeline.ingestion import core


def ingest_batch(path: str, batch_label: str | None = None, dag_run_id: str | None = None) -> dict:
    if config.INGEST_MODE == "inprocess":
        return core.ingest_file(path, batch_label=batch_label, dag_run_id=dag_run_id)
    r = requests.post(f"{config.INGEST_API_URL}/ingest/batch",
                      json={"path": path, "batch_label": batch_label, "dag_run_id": dag_run_id}, timeout=900)
    if r.status_code != 200:
        raise RuntimeError(f"ingest API {r.status_code}: {r.text[:500]}")
    return r.json()


def ingest_stream(events: list[dict], stream_run_id: str, batch_label: str) -> dict:
    if config.INGEST_MODE == "inprocess":
        return core.ingest_stream_events(events, stream_run_id, batch_label)
    r = requests.post(f"{config.INGEST_API_URL}/ingest/stream/iot",
                      json={"stream_run_id": stream_run_id, "batch_label": batch_label, "events": events}, timeout=300)
    r.raise_for_status()
    return r.json()
