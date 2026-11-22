"""
Ingestion API (FastAPI) - the single entry point for data entering the platform.

  GET  /health                       liveness + DB check
  GET  /landing                      files the watcher considers ready
  POST /ingest/batch                 {"path": "...", "dag_run_id": "..."}  -> CSV into ODS
  POST /ingest/stream/{source_key}   {"stream_run_id": "...", "batch_label": "...", "events": [...]}
  GET  /metrics/summary              quick numbers for smoke tests

Run:  uvicorn pipeline.ingestion.api:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from pipeline import config
from pipeline.db import pg_fetch
from pipeline.ingestion import core

log = logging.getLogger("ingest-api")
app = FastAPI(title="SDI Ingestion API", version="1.0")


class BatchRequest(BaseModel):
    path: str
    batch_label: str | None = None
    dag_run_id: str | None = None
    archive: bool = True


class StreamRequest(BaseModel):
    stream_run_id: str
    batch_label: str = "stream"
    events: list[dict] = Field(default_factory=list)


@app.get("/health")
def health():
    try:
        pg_fetch("SELECT 1")
        return {"status": "ok", "db": "ok"}
    except Exception as e:  # pragma: no cover
        raise HTTPException(503, f"db unavailable: {e}")


@app.get("/landing")
def landing():
    return core.LandingZoneWatcher().scan()


@app.post("/ingest/batch")
def ingest_batch(req: BatchRequest):
    try:
        return core.ingest_file(req.path, batch_label=req.batch_label, dag_run_id=req.dag_run_id, archive=req.archive)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(422, str(e))


@app.post("/ingest/stream/{source_key}")
def ingest_stream(source_key: str, req: StreamRequest):
    if source_key not in config.SOURCES:
        raise HTTPException(404, f"unknown source {source_key}")
    if source_key != "iot":
        raise HTTPException(400, "only the iot source is streamed in this milestone")
    return core.ingest_stream_events(req.events, req.stream_run_id, req.batch_label, source_key)


@app.get("/metrics/summary")
def summary():
    rows = pg_fetch("""SELECT source_key, count(*), sum(rows_loaded), round(avg(duration_s)::numeric,3)
                       FROM ops.batch_load_log WHERE stage='ODS_LOAD' GROUP BY 1 ORDER BY 1""")
    return [{"source": r[0], "loads": r[1], "rows": int(r[2] or 0), "avg_duration_s": float(r[3] or 0)} for r in rows]
