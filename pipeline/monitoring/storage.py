"""Storage & staging metrics: row counts, footprint (MB) and date-coverage span per table/layer."""
from __future__ import annotations

import logging
import os

from pipeline import config
from pipeline.db import duck, pg, pg_insert_many
from pipeline.ingestion.ddl import ods_table, q, stg_table

log = logging.getLogger(__name__)
MART_TABLES = {"fact_sales": ("Pharmacy Sales", "sale_date_key"),
               "fact_hospital_demand": ("Hospital Demand", "month_date_key"),
               "agg_sales_monthly": ("Pharmacy Sales", None), "agg_hospital_monthly": ("Hospital Demand", None),
               "dim_medicine": ("Master Data", None), "dim_pharmacy": ("Pharmacy Sales", None),
               "dim_hospital": ("Hospital Demand", None), "dim_location": ("Conformed", None),
               "dim_channel": ("Pharmacy Sales", None), "dim_sales_profile": ("Pharmacy Sales", None),
               "dim_demand_profile": ("Hospital Demand", None), "dim_date": ("Conformed", None)}


def capture(batch_label: str) -> list[dict]:
    rows = []
    with pg() as c, c.cursor() as cur:
        for key, src in config.SOURCES.items():
            for layer, table in (("ods", ods_table(key)), ("staging", stg_table(key))):
                dcol = q(src.event_date)
                dexpr = f"ops.try_date({dcol})" if layer == "ods" else f"{dcol}::date"
                cur.execute(f"""SELECT count(*), pg_total_relation_size('{table}') / 1048576.0,
                                       min({dexpr}), max({dexpr}), count(DISTINCT date_trunc('month', {dexpr}))
                                FROM {table}""")
                n, mb, mn, mx, months = cur.fetchone()
                rows.append(dict(batch_label=batch_label, store="postgres", layer=layer, table_name=table,
                                 domain=src.domain, row_count=n, size_mb=float(mb), min_date=mn, max_date=mx,
                                 distinct_months=months))
        cur.execute("SELECT pg_database_size(current_database()) / 1048576.0")
        rows.append(dict(batch_label=batch_label, store="postgres", layer="database", table_name="pharma_dwh (total)",
                         domain="All", row_count=None, size_mb=float(cur.fetchone()[0]), min_date=None, max_date=None,
                         distinct_months=None))
    with duck(read_only=True) as con:
        est = dict(con.execute("SELECT table_name, estimated_size FROM duckdb_tables() WHERE schema_name='mart'").fetchall())
        for t, (domain, dk) in MART_TABLES.items():
            n = con.execute(f"SELECT count(*) FROM mart.{t}").fetchone()[0]
            mn = mx = months = None
            if dk:
                mn, mx, months = con.execute(f"""SELECT min(d.full_date), max(d.full_date), count(DISTINCT d.year_month)
                                                FROM mart.{t} f JOIN mart.dim_date d ON d.date_key = f.{dk}""").fetchone()
            rows.append(dict(batch_label=batch_label, store="duckdb", layer="mart", table_name=f"mart.{t}", domain=domain,
                             row_count=n, size_mb=None, min_date=mn, max_date=mx, distinct_months=months))
    rows.append(dict(batch_label=batch_label, store="duckdb", layer="database", table_name="sdi_mart.duckdb (file)",
                     domain="All", row_count=None, size_mb=os.path.getsize(config.DUCKDB_PATH) / 1048576.0,
                     min_date=None, max_date=None, distinct_months=None))
    pg_insert_many("ops.storage_snapshot", rows)
    log.info("storage snapshot: %s tables", len(rows))
    return rows
