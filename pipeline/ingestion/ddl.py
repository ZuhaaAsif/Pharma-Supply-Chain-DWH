"""Create ODS (bronze, all-text) and staging (silver, typed) tables from the frozen data contract."""
from __future__ import annotations

from pathlib import Path

from pipeline import config
from pipeline.db import pg
from pipeline.schemas import SOURCE_SCHEMAS

META_ODS = ("_batch_id text, _batch_label text, _source_file text, _row_num bigint, "
            "_ingested_at timestamptz DEFAULT now()")
META_STG = ("_stg_id bigint GENERATED ALWAYS AS IDENTITY, _batch_id text, _batch_label text, "
            "_validated_at timestamptz DEFAULT now()")


def q(col: str) -> str:
    return '"' + col.lower() + '"'


def ods_table(key: str) -> str:
    return f"ods.raw_{config.SOURCES[key].table}"


def stg_table(key: str) -> str:
    return f"staging.stg_{config.SOURCES[key].table}"


def create_all() -> None:
    init_sql = (Path(__file__).resolve().parents[2] / "sql" / "01_init_postgres.sql").read_text()
    with pg() as c, c.cursor() as cur:
        cur.execute(init_sql)
        for key, src in config.SOURCES.items():
            cols = SOURCE_SCHEMAS[src.file]
            ods_cols = ", ".join(f"{q(c_)} text" for c_ in cols)
            cur.execute(f"CREATE TABLE IF NOT EXISTS {ods_table(key)} ({ods_cols}, {META_ODS})")
            cur.execute(f"CREATE INDEX IF NOT EXISTS ix_{src.table}_ods_batch ON {ods_table(key)} (_batch_id)")
            stg_cols = ", ".join(f"{q(c_)} {t}" for c_, t in cols.items())
            cur.execute(f"CREATE TABLE IF NOT EXISTS {stg_table(key)} ({stg_cols}, {META_STG})")
            pk = ", ".join(q(p) for p in src.pk)
            # natural-key uniqueness makes re-loads idempotent (ON CONFLICT DO NOTHING) -
            # master is versioned (SCD source), so its key includes EffectiveFromDate
            if key == "master":
                pk += ', "effectivefromdate"'
            cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS ux_{src.table}_stg_pk ON {stg_table(key)} ({pk})")
            cur.execute(f"CREATE INDEX IF NOT EXISTS ix_{src.table}_stg_id ON {stg_table(key)} (_stg_id)")
            if src.event_date:
                cur.execute(f"CREATE INDEX IF NOT EXISTS ix_{src.table}_stg_dt ON {stg_table(key)} ({q(src.event_date)})")
        cur.execute("""CREATE TABLE IF NOT EXISTS staging.rejects (
            reject_id bigserial PRIMARY KEY, source_key text, batch_id text, batch_label text,
            record_key text, reason text, raw jsonb, rejected_at timestamptz DEFAULT now())""")


if __name__ == "__main__":
    create_all()
    print("schemas ready")
