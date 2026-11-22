"""
Query layer runner: executes the catalog against the DuckDB mart, times every execution with a
time.perf_counter() wrapper, and records Query Execution Time, Active Query Count and Query Error
Rate inputs in ops.query_log. Result sets are snapshotted per refresh so the BI layer (and the
report) can compare refreshes.

Optimisation applied before timing (see optimize()):
  * fact tables are physically ordered by date key at load time -> DuckDB zone maps (min/max per
    row group) skip row groups on date filters, i.e. implicit range partitioning;
  * ART indexes on the natural keys used for lookups / drill-through;
  * pre-aggregated summary tables (mart.agg_*) = materialised views, refreshed incrementally;
  * gold Parquet export partitioned by year/month (hive layout) for external BI tools.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path

from pipeline import config
from pipeline.db import duck, pg_insert_many
from pipeline.query.catalog import CATALOG

log = logging.getLogger(__name__)


def optimize(con) -> None:
    for stmt in (
        "CREATE INDEX IF NOT EXISTS ix_fs_txn ON mart.fact_sales (sales_transaction_id)",
        "CREATE INDEX IF NOT EXISTS ix_fs_lot ON mart.fact_sales (warehouse_lot_id)",
        "CREATE INDEX IF NOT EXISTS ix_dm_id ON mart.dim_medicine (medicine_id)",
        "CREATE INDEX IF NOT EXISTS ix_fh_rec ON mart.fact_hospital_demand (hospital_demand_record_id)",
    ):
        con.execute(stmt)
    con.execute("ANALYZE")


def timed(con, sql: str) -> tuple[float, int, str | None]:
    t0 = time.perf_counter()
    try:
        n = len(con.execute(sql).fetchall())
        return (time.perf_counter() - t0) * 1000, n, None
    except Exception as e:  # counted toward Query Error Rate
        return (time.perf_counter() - t0) * 1000, 0, f"{type(e).__name__}: {e}"[:500]


def run_catalog(batch_label: str, repeats: int = 3, triggered_by: str = "airflow",
                snapshot: bool = True, names: list[str] | None = None) -> dict:
    rows, summary = [], {}
    snap_dir = config.WAREHOUSE_DIR / "query_results" / batch_label
    with duck() as con:
        optimize(con)
        for q in CATALOG:
            if names and q["name"] not in names:
                continue
            for variant in ("baseline", "optimized"):
                sql = q.get(variant)
                if not sql:
                    continue
                timed(con, sql)  # warm-up (not logged): excludes first-touch I/O
                times = []
                for _ in range(repeats):
                    ms, n, err = timed(con, sql)
                    times.append(ms)
                    rows.append(dict(batch_label=batch_label, query_name=q["name"], widget=q["widget"],
                                     dashboard=q["dashboard"], variant=variant, duration_ms=round(ms, 3),
                                     rows_returned=n, status="FAILED" if err else "SUCCESS", error_message=err,
                                     triggered_by=triggered_by))
                summary[f"{q['name']}:{variant}"] = round(sorted(times)[len(times) // 2], 2)
            if snapshot:
                snap_dir.mkdir(parents=True, exist_ok=True)
                best = q.get("optimized") or q["baseline"]
                try:
                    con.execute(best).df().to_csv(snap_dir / f"{q['name']}.csv", index=False)
                except Exception as e:  # pragma: no cover
                    log.warning("snapshot failed for %s: %s", q["name"], e)
    pg_insert_many("ops.query_log", rows)
    failed = sum(r["status"] == "FAILED" for r in rows)
    log.info("query layer: %s executions, %s failed", len(rows), failed)
    return {"executions": len(rows), "failed": failed, "median_ms": summary}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import json
    print(json.dumps(run_catalog("adhoc", triggered_by="adhoc"), indent=1))
