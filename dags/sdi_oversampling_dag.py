"""
DAG 1 - sdi_oversampling
Simulates the 24/7 source systems: synthesises one new calendar quarter for all 7 source files
(refresh 1 = 2026Q1, refresh 2 = 2026Q2) and publishes them atomically to the landing zone
(batch files) and the stream queue (IoT). Publishing emits Assets, which automatically
schedule sdi_batch_pipeline and sdi_stream_ingest (data-aware scheduling).

Trigger:  airflow dags trigger sdi_oversampling --conf '{"refresh_id": 1}'
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import Param, dag, task, get_current_context

from sdi_common import DEFAULT_ARGS, LANDING_ASSET, STREAM_ASSET, on_failure, record_task


@dag(
    dag_id="sdi_oversampling",
    description="Generate one new quarter of synthetic source data for all 7 files",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "on_failure_callback": on_failure},
    params={"refresh_id": Param(1, type="integer", enum=[1, 2], description="1 = 2026Q1, 2 = 2026Q2")},
    tags=["sdi", "oversampling", "milestone-1"],
)
def sdi_oversampling():

    @task
    def plan_refresh() -> dict:
        """Work out template window, target window and growth factors before generating."""
        from pipeline import config
        from pipeline.oversampling.generator import OversamplingEngine, _growth
        rid = int(get_current_context()["params"]["refresh_id"])
        eng = OversamplingEngine(rid)
        plan = {"refresh_id": rid, "label": eng.label, "window": [str(eng.start.date()), str(eng.end.date())],
                "template_window": [str(eng.t_start.date()), str(eng.t_end.date())]}
        plan["already_generated"] = (config.OVERSAMPLED_DIR / eng.label / "generation_stats.json").exists()
        return plan

    @task
    def generate(plan: dict) -> dict:
        from pipeline.oversampling.generator import OversamplingEngine
        from pipeline.db import pg_insert_many
        eng = OversamplingEngine(plan["refresh_id"])
        stats = eng.run(publish=False)
        pg_insert_many("ops.oversampling_log", [dict(
            refresh_label=eng.label, source_key=s.source_key, window_start=s.window_start, window_end=s.window_end,
            template_rows=s.template_rows, growth_factor=s.growth_factor, rows_generated=s.rows_generated,
            malformed_injected=s.malformed_injected, output_file=getattr(s, "output", "")) for s in stats])
        return {"label": eng.label, "rows": {s.source_key: s.rows_generated for s in stats},
                "malformed": {s.source_key: s.malformed_injected for s in stats}}

    @task
    def quality_check(gen: dict) -> dict:
        """Referential checks on the generated files before they are released to the landing zone."""
        import pandas as pd
        from pipeline import config
        d = config.OVERSAMPLED_DIR / gen["label"]
        read = lambda k: pd.read_csv(d / config.SOURCES[k].file, dtype=str)
        master = read("master")
        sales, hosp, wh = read("sales"), read("hospital_demand"), read("warehouse")
        skus = set(master.MedicineID)
        win = config.REFRESHES[int(get_current_context()["params"]["refresh_id"])]
        sale_dt = pd.to_datetime(sales.SalesDate, errors="coerce")
        checks = {
            "sales_sku_fk_pct": round(100 * sales.MedicineID.isin(skus).mean(), 2),
            "hospital_panel_rows": len(hosp),
            "sales_lot_in_new_warehouse_pct": round(100 * sales.WarehouseLotID.isin(set(wh.WarehouseLotID)).mean(), 2),
            "sales_dates_in_window_pct": round(100 * sale_dt.between(pd.Timestamp(win["start"]),
                                                                      pd.Timestamp(win["end"])).mean(), 2),
        }
        # injected faults mean slightly < 100% is EXPECTED; anything far lower is a generator bug
        assert checks["sales_sku_fk_pct"] > 98.0, checks
        return checks

    @task(outlets=[LANDING_ASSET, STREAM_ASSET])
    def publish(gen: dict, checks: dict) -> list[str]:
        from pipeline.oversampling.generator import OversamplingEngine
        ctx = get_current_context()
        eng = OversamplingEngine(int(ctx["params"]["refresh_id"]))
        files = eng.publish()
        record_task("sdi_oversampling", ctx["run_id"], gen["label"], "publish", "SUCCESS",
                    {"files": files, "checks": checks, "rows": gen["rows"]})
        return files

    plan = plan_refresh()
    gen = generate(plan)
    publish(gen, quality_check(gen))


sdi_oversampling()
