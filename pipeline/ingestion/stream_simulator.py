"""
Streaming simulator - replays a queued IoT file as a live event stream.

Each micro-batch of N events is stamped with `_emitted_at` (wall-clock time the event is
"produced"), optionally corrupted at a small rate (schema mismatch / null key), and POSTed to
the ingestion API. The API measures commit time, so:
    End-to-end latency = commit timestamp - _emitted_at
    Throughput (eps)   = events committed / micro-batch wall time
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline import config
from pipeline.ingestion import client, core

log = logging.getLogger(__name__)


def queued_files() -> list[Path]:
    return sorted(config.STREAM_QUEUE_DIR.glob("*.csv"))


def replay(path: str | Path, microbatch: int | None = None, max_events: int | None = None,
           corrupt_rate: float = 0.002, pace_s: float = 0.0, seed: int = 7) -> dict:
    path = Path(path)
    label = path.name.split("__")[0] if "__" in path.name else "stream"
    microbatch = microbatch or config.STREAM_MICROBATCH_SIZE
    max_events = max_events if max_events is not None else config.STREAM_MAX_EVENTS
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    if max_events:
        df = df.head(max_events)
    rng = np.random.default_rng(seed)
    run_id = core.new_stream_run_id()
    totals = {"stream_run_id": run_id, "batch_label": label, "events": 0, "committed": 0, "malformed": 0, "microbatches": 0}
    t_start = time.perf_counter()
    records = df.to_dict("records")
    for i in range(0, len(records), microbatch):
        chunk = records[i:i + microbatch]
        now = datetime.now(timezone.utc).isoformat()
        for ev in chunk:
            for k, v in list(ev.items()):
                if isinstance(v, float) and np.isnan(v):
                    ev[k] = None
            ev["_emitted_at"] = now
            r = rng.random()
            if r < corrupt_rate / 2:
                ev["TemperatureC"] = "ERR#SENSOR"        # schema mismatch
            elif r < corrupt_rate:
                ev["SensorReadingID"] = None             # null key
        res = client.ingest_stream(chunk, run_id, label)
        totals["events"] += res["events_received"]
        totals["committed"] += res["events_committed"]
        totals["malformed"] += res["events_malformed"]
        totals["microbatches"] += 1
        if pace_s:
            time.sleep(pace_s)
    totals["duration_s"] = round(time.perf_counter() - t_start, 2)
    totals["throughput_eps"] = round(totals["committed"] / max(totals["duration_s"], 1e-6), 1)
    done = config.PROCESSED_DIR / label
    done.mkdir(parents=True, exist_ok=True)
    path.replace(done / path.name)
    log.info("stream replay done: %s", totals)
    return totals
