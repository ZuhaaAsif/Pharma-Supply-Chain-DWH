-- =====================================================================
-- Postgres bootstrap for the SDI pipeline (runs once, idempotent)
--   ods      : bronze  - raw rows exactly as received (all TEXT) + load metadata
--   staging  : silver  - typed, validated, de-duplicated rows
--   ops      : observability - every metric shown on the 4 DE dashboards
-- ODS/staging tables are created by pipeline/ingestion/ddl.py from the
-- frozen data contract (pipeline/schemas.py).
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS ops;

-- ---------- safe casting helpers used by the validation SQL ----------
CREATE OR REPLACE FUNCTION ops.try_numeric(t text) RETURNS double precision
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN RETURN t::double precision; EXCEPTION WHEN others THEN RETURN NULL; END $$;

CREATE OR REPLACE FUNCTION ops.try_date(t text) RETURNS date
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  IF t IS NULL OR t !~ '^\d{4}-\d{2}-\d{2}' THEN RETURN NULL; END IF;
  RETURN left(t, 10)::date;
EXCEPTION WHEN others THEN RETURN NULL; END $$;

CREATE OR REPLACE FUNCTION ops.try_ts(t text) RETURNS timestamptz
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN RETURN t::timestamptz; EXCEPTION WHEN others THEN RETURN NULL; END $$;

-- ---------- file registry: what the watcher has already seen ----------
CREATE TABLE IF NOT EXISTS ops.file_registry (
    file_sha256   text PRIMARY KEY,
    file_name     text NOT NULL,
    source_key    text,
    batch_label   text,
    size_bytes    bigint,
    first_seen_at timestamptz DEFAULT now(),
    status        text DEFAULT 'DETECTED'          -- DETECTED | LOADED | FAILED
);

-- ---------- batch ingestion metrics (dashboard 1) ----------
CREATE TABLE IF NOT EXISTS ops.batch_load_log (
    load_id        bigserial PRIMARY KEY,
    batch_id       text NOT NULL,                  -- e.g. base__05_pharmacy_sales_data
    batch_label    text NOT NULL,                  -- base | refresh_01_2026Q1 | refresh_02_2026Q2
    source_key     text NOT NULL,
    file_name      text NOT NULL,
    stage          text NOT NULL,                  -- ODS_LOAD | STAGING_VALIDATE
    started_at     timestamptz NOT NULL,
    finished_at    timestamptz,
    duration_s     double precision,
    rows_read      bigint,
    rows_loaded    bigint,
    rows_rejected  bigint DEFAULT 0,
    file_size_mb   double precision,
    status         text NOT NULL,                  -- SUCCESS | FAILED
    error_message  text,
    dag_run_id     text
);

-- ---------- streaming ingestion metrics (dashboard 2) ----------
CREATE TABLE IF NOT EXISTS ops.stream_batch_log (
    microbatch_id      bigserial PRIMARY KEY,
    stream_run_id      text NOT NULL,
    batch_label        text,
    source_key         text NOT NULL,
    received_at        timestamptz NOT NULL,
    committed_at       timestamptz NOT NULL,
    events_received    int NOT NULL,
    events_committed   int NOT NULL,
    events_malformed   int NOT NULL,
    duration_s         double precision NOT NULL,
    throughput_eps     double precision,
    latency_avg_s      double precision,
    latency_p95_s      double precision,
    latency_max_s      double precision
);
CREATE TABLE IF NOT EXISTS ops.stream_rejects (
    reject_id     bigserial PRIMARY KEY,
    stream_run_id text,
    source_key    text,
    reason        text,
    payload       jsonb,
    rejected_at   timestamptz DEFAULT now()
);

-- ---------- storage metrics (dashboard 3) ----------
CREATE TABLE IF NOT EXISTS ops.storage_snapshot (
    snapshot_id    bigserial PRIMARY KEY,
    captured_at    timestamptz NOT NULL DEFAULT now(),
    batch_label    text,
    store          text NOT NULL,                  -- postgres | duckdb
    layer          text NOT NULL,                  -- ods | staging | mart
    table_name     text NOT NULL,
    domain         text,
    row_count      bigint,
    size_mb        double precision,
    min_date       date,
    max_date       date,
    distinct_months int
);

-- ---------- query metrics (dashboard 4) ----------
CREATE TABLE IF NOT EXISTS ops.query_log (
    query_log_id   bigserial PRIMARY KEY,
    executed_at    timestamptz NOT NULL DEFAULT now(),
    batch_label    text,
    query_name     text NOT NULL,
    widget         text NOT NULL,                  -- dashboard widget / KPI card it feeds
    dashboard      text,
    variant        text NOT NULL,                  -- baseline (fact scan) | optimized (aggregate)
    duration_ms    double precision,
    rows_returned  bigint,
    status         text NOT NULL,                  -- SUCCESS | FAILED
    error_message  text,
    triggered_by   text                            -- airflow | bi_refresh | adhoc
);

-- ---------- pipeline-level audit ----------
CREATE TABLE IF NOT EXISTS ops.pipeline_run (
    run_id        bigserial PRIMARY KEY,
    dag_id        text,
    dag_run_id    text,
    batch_label   text,
    task          text,
    started_at    timestamptz,
    finished_at   timestamptz,
    status        text,
    details       jsonb
);

CREATE TABLE IF NOT EXISTS ops.oversampling_log (
    id             bigserial PRIMARY KEY,
    generated_at   timestamptz DEFAULT now(),
    refresh_label  text,
    source_key     text,
    window_start   date,
    window_end     date,
    template_rows  bigint,
    growth_factor  double precision,
    rows_generated bigint,
    malformed_injected bigint,
    output_file    text
);
