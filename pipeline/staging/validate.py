"""
ODS (bronze) -> staging (silver): data validation + typing + de-duplication, set-based in SQL.

Rules (first failing rule wins and becomes the reject reason):
  R1 null natural key                         R5 invalid / out-of-range date (2020-2030)
  R2 null SKU (MedicineID)                    R6 type mismatch on any typed column
  R3 unknown SKU (not in master)  [FK]        R7 duplicate key inside the batch
  R4 non-positive price / qty                 R8 duplicate key already in staging
Master is versioned: an unchanged SKU snapshot row is skipped (not an error); a changed one
is inserted as a new version (new EffectiveFromDate) and becomes an SCD-2 row in the mart.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from pipeline import config
from pipeline.db import pg, pg_fetch
from pipeline.ingestion.ddl import ods_table, q, stg_table
from pipeline.schemas import SOURCE_SCHEMAS

log = logging.getLogger(__name__)
CAST = {"double precision": "ops.try_numeric", "bigint": "ops.try_numeric", "date": "ops.try_date",
        "timestamptz": "ops.try_ts"}


def _reason_sql(key: str) -> str:
    src = config.SOURCES[key]
    types = SOURCE_SCHEMAS[src.file]
    rules = []
    pk_null = " OR ".join(f"o.{q(p)} IS NULL" for p in src.pk)
    rules.append(f"WHEN {pk_null} THEN 'R1 null natural key'")
    if "MedicineID" in types and key != "master":
        rules.append("WHEN o.\"medicineid\" IS NULL THEN 'R2 null SKU (MedicineID)'")
        rules.append("WHEN NOT EXISTS (SELECT 1 FROM staging.stg_master m WHERE m.\"medicineid\" = o.\"medicineid\") "
                     "THEN 'R3 unknown SKU (FK to master)'")
    for c in src.positive_cols:
        rules.append(f"WHEN ops.try_numeric(o.{q(c)}) IS NULL OR ops.try_numeric(o.{q(c)}) <= 0 THEN 'R4 non-positive {c}'")
    for c in src.date_cols:
        fn = "ops.try_ts" if types[c] == "timestamptz" else "ops.try_date"
        rules.append(f"WHEN {fn}(o.{q(c)}) IS NULL OR {fn}(o.{q(c)})::date NOT BETWEEN DATE '2020-01-01' AND DATE '2030-12-31' "
                     f"THEN 'R5 invalid date {c}'")
    for c, t in types.items():
        if t in CAST:
            rules.append(f"WHEN o.{q(c)} IS NOT NULL AND {CAST[t]}(o.{q(c)}) IS NULL THEN 'R6 type mismatch {c}'")
    rules.append("WHEN o._dup_rank > 1 THEN 'R7 duplicate key in batch'")
    return "CASE " + " ".join(rules) + " ELSE NULL END"


def _select_typed(key: str) -> str:
    types = SOURCE_SCHEMAS[config.SOURCES[key].file]
    parts = []
    for c, t in types.items():
        if t == "bigint":
            parts.append(f"round(ops.try_numeric(v.{q(c)}))::bigint")
        elif t in CAST:
            parts.append(f"{CAST[t]}(v.{q(c)})")
        else:
            parts.append(f"v.{q(c)}")
    return ", ".join(parts)


def pending_batches(key: str) -> list[str]:
    return [r[0] for r in pg_fetch(
        f"""SELECT DISTINCT _batch_id FROM {ods_table(key)} o
            WHERE NOT EXISTS (SELECT 1 FROM ops.batch_load_log l
                              WHERE l.batch_id = o._batch_id AND l.stage = 'STAGING_VALIDATE' AND l.status = 'SUCCESS')
            ORDER BY 1""")]


def validate_batch(key: str, batch_id: str, dag_run_id: str | None = None) -> dict:
    src = config.SOURCES[key]
    cols = list(SOURCE_SCHEMAS[src.file])
    col_list = ", ".join(q(c) for c in cols)
    pk_part = ", ".join(q(p) for p in src.pk) + (', "effectivefromdate"' if key == "master" else "")
    pk_join = " AND ".join(f"s.{q(p)} = ops_v.{q(p)}" for p in src.pk)
    typed = _select_typed(key)
    reason = _reason_sql(key)
    started, t0 = datetime.now(timezone.utc), time.perf_counter()
    with pg() as c, c.cursor() as cur:
        cur.execute(f"SELECT max(_batch_label), count(*) FROM {ods_table(key)} WHERE _batch_id = %s", (batch_id,))
        label, rows_read = cur.fetchone()
        cur.execute(f"""
            CREATE TEMP TABLE _v ON COMMIT DROP AS
            SELECT o.*, {reason} AS _reason
            FROM (SELECT *, row_number() OVER (PARTITION BY {pk_part} ORDER BY _row_num) AS _dup_rank
                  FROM {ods_table(key)} WHERE _batch_id = %s) o""", (batch_id,))
        # R8 / master no-change: key already present in staging
        if key == "master":
            cur.execute(f"""UPDATE _v SET _reason = 'SKIP unchanged master row' WHERE _reason IS NULL AND EXISTS (
                SELECT 1 FROM {stg_table(key)} s WHERE s."medicineid" = _v."medicineid"
                AND s."effectivefromdate" = ops.try_date(_v."effectivefromdate"))""")
        else:
            pk_cmp = " AND ".join(f"s.{q(p)} = " + (f"round(ops.try_numeric(_v.{q(p)}))::bigint"
                                                    if SOURCE_SCHEMAS[src.file][p] == "bigint" else f"_v.{q(p)}")
                                  for p in src.pk)
            cur.execute(f"""UPDATE _v SET _reason = 'R8 duplicate key already loaded' WHERE _reason IS NULL AND EXISTS (
                SELECT 1 FROM {stg_table(key)} s WHERE {pk_cmp})""")
        pk_txt = " || '|' || ".join(f"coalesce({q(p)}, '∅')" for p in src.pk)
        cur.execute(f"""INSERT INTO staging.rejects (source_key, batch_id, batch_label, record_key, reason, raw)
                        SELECT %s, _batch_id, _batch_label, {pk_txt}, _reason,
                               to_jsonb(_v) - '_reason' - '_dup_rank'
                        FROM _v WHERE _reason IS NOT NULL AND _reason NOT LIKE 'SKIP%%'""", (key,))
        rejected = cur.rowcount
        cur.execute("SELECT count(*) FROM _v WHERE _reason LIKE 'SKIP%%'")
        skipped = cur.fetchone()[0]
        cur.execute(f"""INSERT INTO {stg_table(key)} ({col_list}, _batch_id, _batch_label)
                        SELECT {typed}, v._batch_id, v._batch_label FROM _v v WHERE v._reason IS NULL
                        ON CONFLICT DO NOTHING""")
        loaded = cur.rowcount
    dur = time.perf_counter() - t0
    with pg() as c, c.cursor() as cur:
        cur.execute("""INSERT INTO ops.batch_load_log (batch_id, batch_label, source_key, file_name, stage, started_at,
                       finished_at, duration_s, rows_read, rows_loaded, rows_rejected, status, dag_run_id)
                       VALUES (%s,%s,%s,%s,'STAGING_VALIDATE',%s,now(),%s,%s,%s,%s,'SUCCESS',%s)""",
                    (batch_id, label, key, src.file, started, dur, rows_read, loaded, rejected, dag_run_id))
    log.info("validated %s: read=%s loaded=%s rejected=%s skipped=%s (%.2fs)", batch_id, rows_read, loaded, rejected, skipped, dur)
    return {"batch_id": batch_id, "rows_read": rows_read, "loaded": loaded, "rejected": rejected, "skipped": skipped,
            "duration_s": round(dur, 3)}


def validate_pending(keys: list[str] | None = None, dag_run_id: str | None = None) -> list[dict]:
    """Validate every ODS batch not yet promoted. Master goes first (FK rule R3 depends on it)."""
    out = []
    for key in (keys or list(config.SOURCES)):
        for b in pending_batches(key):
            out.append(validate_batch(key, b, dag_run_id))
    return out
