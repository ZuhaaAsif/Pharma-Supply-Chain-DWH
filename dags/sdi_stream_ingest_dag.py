"""
DAG 3 - sdi_stream_ingest  (micro-batch streaming of IoT telemetry)

Replays every queued IoT file as a live stream of micro-batches through the ingestion API
(POST /ingest/stream/iot). Throughput, end-to-end latency and malformed/dropped counts are
recorded per micro-batch in ops.stream_batch_log -> streaming dashboard.
Runs when sdi_oversampling queues events (STREAM_ASSET) or every 5 minutes.
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import PokeReturnValue, dag, task, get_current_context
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

from sdi_common import DEFAULT_ARGS, STREAM_ASSET, on_failure, record_task


@dag(
    dag_id="sdi_stream_ingest",
    description="Micro-batch streaming ingestion of IoT telemetry with throughput/latency metrics",
    schedule=AssetOrTimeSchedule(timetable=CronTriggerTimetable("*/5 * * * *", timezone="UTC"), assets=[STREAM_ASSET]),
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "on_failure_callback": on_failure},
    tags=["sdi", "streaming", "milestone-1"],
)
def sdi_stream_ingest():

    @task.sensor(poke_interval=10, timeout=60, mode="reschedule", soft_fail=True)
    def wait_for_stream_queue() -> PokeReturnValue:
        from pipeline.ingestion.stream_simulator import queued_files
        files = [str(p) for p in queued_files()]
        return PokeReturnValue(is_done=bool(files), xcom_value=files)

    @task
    def replay_stream(path: str) -> dict:
        from pipeline.ingestion.stream_simulator import replay
        return replay(path)

    @task(pool="duckdb_mart")   # DuckDB = single writer -> serialise every task that opens the mart
    def capture_storage_metrics(results: list[dict]) -> list[dict]:
        from pipeline.monitoring.storage import capture
        results = [dict(r) for r in results]          # materialise the lazy mapped-XCom sequence
        label = sorted(r["batch_label"] for r in results)[-1] + "__stream"
        capture(label)
        return results

    @task
    def summarize(results: list[dict]) -> None:
        ctx = get_current_context()
        for r in results:
            record_task("sdi_stream_ingest", ctx["run_id"], r["batch_label"], "replay_stream", "SUCCESS", r)

    summarize(capture_storage_metrics(replay_stream.expand(path=wait_for_stream_queue())))


sdi_stream_ingest()
