"""Thin connection helpers for Postgres (ODS / staging / ops metrics) and DuckDB (mart)."""
from __future__ import annotations

import contextlib
import time
from pathlib import Path

import duckdb
import psycopg2
import psycopg2.extras

from pipeline import config


@contextlib.contextmanager
def pg(autocommit: bool = False):
    conn = psycopg2.connect(config.PG_DSN, options="-c timezone=UTC")
    conn.autocommit = autocommit
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def pg_exec(sql: str, params=None) -> None:
    with pg() as c, c.cursor() as cur:
        cur.execute(sql, params)


def pg_fetch(sql: str, params=None) -> list[tuple]:
    with pg() as c, c.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def pg_insert_many(table: str, rows: list[dict]) -> None:
    if not rows:
        return
    cols = list(rows[0])
    with pg() as c, c.cursor() as cur:
        psycopg2.extras.execute_values(
            cur, f"INSERT INTO {table} ({','.join(cols)}) VALUES %s",
            [tuple(r[k] for k in cols) for r in rows])


@contextlib.contextmanager
def duck(read_only: bool = False, retries: int = 20, wait_s: float = 3.0):
    """DuckDB permits a single writer process. Airflow serialises mart tasks with the size-1
    `duckdb_mart` pool; this retry loop is the safety net for anything outside Airflow."""
    Path(config.DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(retries):
        try:
            conn = duckdb.connect(str(config.DUCKDB_PATH), read_only=read_only)
            break
        except duckdb.IOException as e:
            if "lock" not in str(e).lower() or attempt == retries - 1:
                raise
            time.sleep(wait_s)
    try:
        yield conn
    finally:
        conn.close()
