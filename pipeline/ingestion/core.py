"""
Ingestion core - used by the FastAPI service (pipeline/ingestion/api.py), the Airflow DAGs
and the standalone watchdog daemon. Keeping the logic here (not in the DAG files) gives the
modular separation the spec asks for.

batch:  CSV file in landing zone  -> ods.raw_<table>   (+ ops.batch_load_log)
stream: list of JSON events       -> ods.raw_iot / staging.stg_iot  (+ ops.stream_batch_log)
"""
from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2.extras

from pipeline import config
from pipeline.db import pg, pg_fetch
from pipeline.ingestion.ddl import ods_table, q, stg_table
from pipeline.schemas import SOURCE_SCHEMAS

log = logging.getLogger(__name__)


# ============================================================================ file watcher
def sha256(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while b := fh.read(chunk):
            h.update(b)
    return h.hexdigest()


def parse_landing_name(path: Path) -> tuple[str, str]:
    """'refresh_01_2026Q1__05_pharmacy_sales_data.csv' -> ('refresh_01_2026Q1', 'sales').
    Files without a label prefix are treated as the base load."""
    name = path.name
    label, _, fname = name.rpartition("__")
    label = label or "base"
    if fname not in config.FILE_TO_SOURCE:
        raise ValueError(f"unrecognised source file: {name}")
    return label, config.FILE_TO_SOURCE[fname]


class LandingZoneWatcher:
    """Python file watcher. A file is 'ready' when (a) it is a recognised CSV, (b) its size has
    been stable for `stable_seconds` (not still being written), and (c) its SHA-256 is not yet
    in ops.file_registry (content-based de-duplication: re-dropping the same file is ignored)."""

    def __init__(self, landing: Path | None = None, stable_seconds: float = 2.0):
        self.landing = Path(landing or config.LANDING_DIR)
        self.stable_seconds = stable_seconds

    def scan(self) -> list[dict]:
        self.landing.mkdir(parents=True, exist_ok=True)
        known = {r[0] for r in pg_fetch("SELECT file_sha256 FROM ops.file_registry WHERE status IN ('LOADED','DETECTED')")}
        ready = []
        for p in sorted(self.landing.glob("*.csv")):
            try:
                label, key = parse_landing_name(p)
            except ValueError:
                log.warning("ignoring unknown file %s", p.name)
                continue
            s1 = p.stat().st_size
            time.sleep(0 if self.stable_seconds == 0 else min(self.stable_seconds, 0.2))
            if p.stat().st_size != s1:
                continue  # still being written
            digest = sha256(p)
            if digest in known:
                continue
            ready.append({"path": str(p), "file_name": p.name, "sha256": digest, "batch_label": label,
                          "source_key": key, "size_bytes": s1})
        # master first, then upstream -> downstream (so FK checks see their parents)
        order = list(config.SOURCES)
        ready.sort(key=lambda r: (r["batch_label"], order.index(r["source_key"])))
        return ready


# ============================================================================ batch ingestion
def _copy_df(cur, table: str, df: pd.DataFrame) -> None:
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    cols = ", ".join(q(c) if not c.startswith("_") else c for c in df.columns)
    cur.copy_expert(f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)


def ingest_file(path: str, batch_label: str | None = None, source_key: str | None = None,
                dag_run_id: str | None = None, archive: bool = True) -> dict:
    """Read a landing-zone CSV with pandas and bulk-load it (COPY) into ods.raw_<table>.
    Records Batch Load Duration, Rows Processed and success/failure in ops.batch_load_log."""
    p = Path(path)
    lbl, key = parse_landing_name(p)
    batch_label, source_key = batch_label or lbl, source_key or key
    src = config.SOURCES[source_key]
    batch_id = f"{batch_label}__{src.table}"
    started = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    digest = sha256(p)
    size_mb = p.stat().st_size / 1e6
    rows_read = rows_loaded = 0
    try:
        df = pd.read_csv(p, dtype=str, keep_default_na=False, na_values=[""])
        rows_read = len(df)                                  # spec: len(df) after loading
        expected = list(SOURCE_SCHEMAS[src.file])
        missing = set(expected) - set(df.columns)
        if missing:
            raise ValueError(f"schema mismatch, missing columns: {sorted(missing)}")
        df = df[expected]
        df["_batch_id"], df["_batch_label"], df["_source_file"] = batch_id, batch_label, p.name
        df["_row_num"] = np.arange(1, rows_read + 1)
        with pg() as c, c.cursor() as cur:
            cur.execute(f"DELETE FROM {ods_table(source_key)} WHERE _batch_id = %s", (batch_id,))  # idempotent re-run
            _copy_df(cur, ods_table(source_key), df)
            cur.execute("""INSERT INTO ops.file_registry (file_sha256, file_name, source_key, batch_label, size_bytes, status)
                           VALUES (%s,%s,%s,%s,%s,'LOADED')
                           ON CONFLICT (file_sha256) DO UPDATE SET status='LOADED'""",
                        (digest, p.name, source_key, batch_label, p.stat().st_size))
        rows_loaded = rows_read
        status, err = "SUCCESS", None
    except Exception as e:  # recorded, then re-raised so Airflow marks the task failed / retries
        status, err = "FAILED", f"{type(e).__name__}: {e}"[:1000]
    finished = datetime.now(timezone.utc)
    dur = time.perf_counter() - t0
    with pg() as c, c.cursor() as cur:
        cur.execute("""INSERT INTO ops.batch_load_log (batch_id, batch_label, source_key, file_name, stage, started_at,
                       finished_at, duration_s, rows_read, rows_loaded, file_size_mb, status, error_message, dag_run_id)
                       VALUES (%s,%s,%s,%s,'ODS_LOAD',%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (batch_id, batch_label, source_key, p.name, started, finished, dur, rows_read, rows_loaded,
                     size_mb, status, err, dag_run_id))
        if status == "FAILED":
            cur.execute("""INSERT INTO ops.file_registry (file_sha256, file_name, source_key, batch_label, size_bytes, status)
                           VALUES (%s,%s,%s,%s,%s,'FAILED') ON CONFLICT (file_sha256) DO UPDATE SET status='FAILED'""",
                        (digest, p.name, source_key, batch_label, p.stat().st_size))
    if status == "FAILED":
        raise RuntimeError(err)
    if archive:
        dest = config.PROCESSED_DIR / batch_label
        dest.mkdir(parents=True, exist_ok=True)
        shutil.move(str(p), dest / p.name)
    log.info("ODS load %s: %s rows in %.2fs", batch_id, rows_loaded, dur)
    return {"batch_id": batch_id, "batch_label": batch_label, "source_key": source_key, "rows": rows_loaded,
            "duration_s": round(dur, 3), "status": status}


# ============================================================================ stream ingestion
IOT_COLS = list(SOURCE_SCHEMAS[config.SOURCES["iot"].file])
IOT_TYPES = SOURCE_SCHEMAS[config.SOURCES["iot"].file]


def _validate_iot_event(ev: dict) -> tuple[dict, None] | tuple[None, str]:
    """Schema + null-key validation for one streamed IoT event (try/except counter, per spec)."""
    try:
        if not ev.get("SensorReadingID"):
            raise ValueError("null key SensorReadingID")
        row = {}
        for c in IOT_COLS:
            v = ev.get(c)
            if v in ("", None) or (isinstance(v, float) and np.isnan(v)):
                row[c] = None
                continue
            t = IOT_TYPES[c]
            if t == "double precision":
                row[c] = float(v)
            elif t == "bigint":
                row[c] = int(float(v))
            elif t == "timestamptz":
                row[c] = pd.Timestamp(v).isoformat()
            else:
                row[c] = str(v)
        if row["EventTimestampUTC"] is None:
            raise ValueError("null EventTimestampUTC")
        return row, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"[:300]


def ingest_stream_events(events: list[dict], stream_run_id: str, batch_label: str = "stream",
                         source_key: str = "iot") -> dict:
    """Validate and commit one micro-batch of streamed events directly into staging.stg_iot
    (stream data is validated in-flight, so it bypasses the bronze ODS hop).
    End-to-end latency = DB commit time - the moment the simulator emitted the event (_emitted_at)."""
    received = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    good, bad = [], []
    emitted = []
    for ev in events:
        row, err = _validate_iot_event(ev)
        if row is None:
            bad.append((err, ev))
        else:
            good.append(row)
            emitted.append(ev.get("_emitted_at"))
    inserted_ids: set = set()
    with pg() as c, c.cursor() as cur:
        if good:
            res = psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO {stg_table(source_key)} ({', '.join(q(x) for x in IOT_COLS)}, _batch_id, _batch_label) "
                f"VALUES %s ON CONFLICT DO NOTHING RETURNING sensorreadingid",
                [tuple(r[x] for x in IOT_COLS) + (f"{batch_label}__stream", batch_label) for r in good],
                page_size=1000, fetch=True)
            inserted_ids = {r[0] for r in res}
            # rows rejected by the natural-key constraint are duplicates -> malformed
            dup_seen = set()
            keep, keep_em = [], []
            for r, em in zip(good, emitted):
                rid = r["SensorReadingID"]
                if rid in inserted_ids and rid not in dup_seen:
                    keep.append(r); keep_em.append(em); dup_seen.add(rid)
                else:
                    bad.append(("duplicate key SensorReadingID", r))
            good, emitted = keep, keep_em
        if bad:
            psycopg2.extras.execute_values(
                cur, "INSERT INTO ops.stream_rejects (stream_run_id, source_key, reason, payload) VALUES %s",
                [(stream_run_id, source_key, e, json.dumps(ev, default=str)) for e, ev in bad])
        cur.execute("SELECT now()")
    committed = datetime.now(timezone.utc)
    dur = max(time.perf_counter() - t0, 1e-6)
    lat = np.array([(committed - datetime.fromisoformat(e)).total_seconds() for e in emitted if e]) if emitted else np.array([])
    rec = dict(stream_run_id=stream_run_id, batch_label=batch_label, source_key=source_key, received_at=received,
               committed_at=committed, events_received=len(events), events_committed=len(good),
               events_malformed=len(bad), duration_s=dur, throughput_eps=len(good) / dur,
               latency_avg_s=float(lat.mean()) if lat.size else None,
               latency_p95_s=float(np.percentile(lat, 95)) if lat.size else None,
               latency_max_s=float(lat.max()) if lat.size else None)
    with pg() as c, c.cursor() as cur:
        cur.execute(f"INSERT INTO ops.stream_batch_log ({', '.join(rec)}) VALUES ({', '.join(['%s'] * len(rec))})",
                    tuple(rec.values()))
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in rec.items()}


def new_stream_run_id() -> str:
    return f"stream_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"
