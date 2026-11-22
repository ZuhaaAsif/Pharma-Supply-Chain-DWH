"""
DAG 2 - sdi_batch_pipeline  (ingestion -> staging -> star schema -> query layer)

  ensure_schemas
      -> watch_landing_zone        (Python file-watcher sensor: stable size + SHA-256 de-dup)
      -> ingest_to_ods[*]          (dynamic task mapping: one task per file, via the FastAPI ingestion API)
      -> validate_to_staging       (price > 0, valid dates, non-null SKU, FK, type, duplicate rules)
      -> build_star_schema         (incremental SCD2 / SCD1 / junk / facts / aggregates in DuckDB)
      -> reconcile_counts          (staging vs mart row reconciliation - fails the run on mismatch)
      -> run_query_layer           (timed OLAP catalog -> ops.query_log)  [emits MART_ASSET]
      -> capture_storage_metrics   (row counts, MB, date coverage -> ops.storage_snapshot)
      -> pipeline_summary

Scheduled by data (LANDING_ASSET from sdi_oversampling) OR every 30 minutes, whichever first.
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import PokeReturnValue, dag, task, task_group, get_current_context
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

from sdi_common import DEFAULT_ARGS, LANDING_ASSET, MART_ASSET, on_failure, record_task


def _latest_label(files: list[dict] | None) -> str:
    labels = sorted({f["batch_label"] for f in (files or [])})
    return labels[-1] if labels else "scheduled"


@dag(
    dag_id="sdi_batch_pipeline",
    description="SDI mart: batch ingestion -> validation -> dimensional model -> query layer",
    schedule=AssetOrTimeSchedule(timetable=CronTriggerTimetable("*/30 * * * *", timezone="UTC"), assets=[LANDING_ASSET]),
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "on_failure_callback": on_failure},
    tags=["sdi", "ingestion", "dwh", "milestone-1"],
)
def sdi_batch_pipeline():

    @task
    def ensure_schemas() -> str:
        from pipeline.ingestion.ddl import create_all
        create_all()
        return "ok"

    @task.sensor(poke_interval=15, timeout=120, mode="reschedule", soft_fail=True)
    def watch_landing_zone() -> PokeReturnValue:
        """File-watcher: succeed as soon as at least one new, fully-written CSV is present.
        soft_fail -> if nothing arrives the run is SKIPPED (not failed)."""
        from pipeline.ingestion.core import LandingZoneWatcher
        ready = LandingZoneWatcher().scan()
        return PokeReturnValue(is_done=bool(ready), xcom_value=ready)

    @task(max_active_tis_per_dagrun=4)
    def ingest_to_ods(file: dict) -> dict:
        from pipeline.ingestion import client
        ctx = get_current_context()
        return client.ingest_batch(file["path"], file["batch_label"], dag_run_id=ctx["run_id"])

    @task
    def validate_to_staging(loaded: list[dict]) -> dict:
        from pipeline.staging.validate import validate_pending
        res = validate_pending(dag_run_id=get_current_context()["run_id"])
        return {"batches": len(res), "rows_loaded": sum(r["loaded"] for r in res),
                "rows_rejected": sum(r["rejected"] for r in res), "detail": res}

    @task_group(group_id="transform")
    def transform(v: dict):
        @task(pool="duckdb_mart")
        def build_star_schema(validation: dict) -> dict:
            from pipeline.mart.transform import load_mart
            return load_mart()

        @task(pool="duckdb_mart")
        def reconcile_counts(load: dict) -> dict:
            """Every validated SDI staging row must be in the mart exactly once."""
            from pipeline.db import duck, pg_fetch
            stg_s = pg_fetch("SELECT count(*) FROM staging.stg_sales")[0][0]
            stg_h = pg_fetch("SELECT count(*) FROM staging.stg_hospital_demand")[0][0]
            with duck(read_only=True) as con:
                f_s = con.execute("SELECT count(*) FROM mart.fact_sales").fetchone()[0]
                f_h = con.execute("SELECT count(*) FROM mart.fact_hospital_demand").fetchone()[0]
                orphans = con.execute("""SELECT count(*) FROM mart.fact_sales
                                         WHERE medicine_sk = -1 OR channel_sk = -1 OR pharmacy_sk = -1""").fetchone()[0]
            res = {"staging_sales": stg_s, "mart_sales": f_s, "staging_hospital": stg_h, "mart_hospital": f_h,
                   "orphan_fact_rows": orphans}
            if stg_s != f_s or stg_h != f_h:
                raise ValueError(f"reconciliation failed: {res}")
            return res

        return reconcile_counts(build_star_schema(v))

    @task(outlets=[MART_ASSET], pool="duckdb_mart")
    def run_query_layer(recon: dict, files: list[dict]) -> dict:
        from pipeline.query.runner import run_catalog
        r = run_catalog(_latest_label(files), triggered_by="airflow")
        return {"executions": r["executions"], "failed": r["failed"]}

    @task(pool="duckdb_mart")
    def capture_storage_metrics(q: dict, files: list[dict]) -> int:
        from pipeline.monitoring.storage import capture
        return len(capture(_latest_label(files)))

    @task
    def pipeline_summary(files: list[dict], v: dict, q: dict, n_storage: int) -> None:
        ctx = get_current_context()
        record_task("sdi_batch_pipeline", ctx["run_id"], _latest_label(files), "pipeline_summary", "SUCCESS",
                    {"files": [f["file_name"] for f in files], "validation": {k: v[k] for k in ("batches", "rows_loaded", "rows_rejected")},
                     "queries": q, "storage_rows": n_storage})

    ready = ensure_schemas() >> watch_landing_zone()
    loaded = ingest_to_ods.expand(file=ready)
    v = validate_to_staging(loaded)
    recon = transform(v)
    q = run_query_layer(recon, ready)
    n = capture_storage_metrics(q, ready)
    pipeline_summary(ready, v, q, n)


sdi_batch_pipeline()
