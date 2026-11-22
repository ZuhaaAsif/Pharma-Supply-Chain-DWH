"""
Generates the four Data-Engineering dashboards as Grafana JSON (provisioned automatically by
docker-compose from grafana/dashboards/). Every panel reads the ops.* metric tables in Postgres.

    python scripts/build_grafana_dashboards.py
"""
from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "grafana" / "dashboards"
DS = {"type": "grafana-postgresql-datasource", "uid": "sdi-postgres"}
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]


class Board:
    def __init__(self, uid, title, description):
        self.uid, self.title, self.description = uid, title, description
        self.panels, self.y, self.id, self.cx, self.row_h = [], 0, 1, 0, 0

    def add(self, ptype, title, sql, w=12, h=8, fmt="table", desc="", field_cfg=None, options=None):
        if self.cx + w > 24:                       # wrap to a new row
            self.y += self.row_h
            self.cx, self.row_h = 0, 0
        p = {"id": self.id, "type": ptype, "title": title, "description": desc, "datasource": DS,
             "gridPos": {"h": h, "w": w, "x": self.cx, "y": self.y},
             "targets": [{"refId": "A", "datasource": DS, "editorMode": "code", "format": fmt, "rawQuery": True,
                          "rawSql": " ".join(sql.split())}],
             "fieldConfig": field_cfg or {"defaults": {}, "overrides": []}, "options": options or {}}
        self.panels.append(p)
        self.id += 1
        self.cx += w
        self.row_h = max(self.row_h, h)
        return p

    def json(self):
        return {"uid": self.uid, "title": self.title, "description": self.description, "tags": ["sdi", "data-engineering"],
                "timezone": "utc", "schemaVersion": 39, "version": 1, "editable": True, "refresh": "30s",
                "time": {"from": "now-7d", "to": "now"}, "panels": self.panels,
                "templating": {"list": [{
                    "name": "batch", "label": "Batch / refresh", "type": "query", "datasource": DS, "multi": True,
                    "includeAll": True, "current": {"text": "All", "value": "$__all"}, "refresh": 2,
                    "query": "SELECT DISTINCT batch_label FROM ops.batch_load_log ORDER BY 1"}]}}


def stat(unit=None, decimals=None, color="#2a78d6", thresholds=None):
    d = {"color": {"mode": "fixed", "fixedColor": color}, "unit": unit or "none"}
    if decimals is not None:
        d["decimals"] = decimals
    if thresholds:
        d["color"] = {"mode": "thresholds"}
        d["thresholds"] = {"mode": "absolute", "steps": thresholds}
    return {"defaults": d, "overrides": []}


STAT_OPTS = {"reduceOptions": {"calcs": ["lastNotNull"], "values": False}, "colorMode": "value", "graphMode": "none",
             "textMode": "value_and_name", "justifyMode": "center"}
BAR_H = {"orientation": "horizontal", "xField": None, "showValue": "always", "barWidth": 0.6, "groupWidth": 0.7,
         "legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"},
         "tooltip": {"mode": "multi"}, "barRadius": 0.1}
BAR_V = {**BAR_H, "orientation": "vertical", "xTickLabelRotation": -30}
TS_OPTS = {"legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}
BATCH = "batch_label IN (${batch:sqlstring})"


def series_cfg(unit="none", line=2, fill=0, draw="line"):
    return {"defaults": {"unit": unit, "color": {"mode": "palette-classic"},
                         "custom": {"lineWidth": line, "fillOpacity": fill, "drawStyle": draw, "pointSize": 6,
                                    "showPoints": "auto", "axisSoftMin": 0}},
            "overrides": []}


# ------------------------------------------------------------------ 1 batch ingestion
b = Board("sdi-batch-ingestion", "SDI · 1 · Batch Ingestion", "File-watcher -> ingestion API -> ODS -> staging validation")
b.add("stat", "Batch success rate", f"""SELECT round(100.0*count(*) FILTER (WHERE status='SUCCESS')/nullif(count(*),0),2) AS "Success %"
      FROM ops.batch_load_log WHERE {BATCH}""", w=6, h=5, field_cfg=stat("percent", 2, thresholds=[
      {"color": "#d03b3b", "value": None}, {"color": "#fab219", "value": 95}, {"color": "#0ca30c", "value": 99.5}]),
      options=STAT_OPTS, desc="Successful runs / attempted runs x 100 (ODS load + staging validation).")
b.add("stat", "Rows processed (ODS)", f"""SELECT sum(rows_loaded) AS "Rows" FROM ops.batch_load_log WHERE stage='ODS_LOAD' AND {BATCH}""",
      w=6, h=5, field_cfg=stat("short"), options=STAT_OPTS)
b.add("stat", "Rows rejected by validation", f"""SELECT sum(rows_rejected) AS "Rejected" FROM ops.batch_load_log
      WHERE stage='STAGING_VALIDATE' AND {BATCH}""", w=6, h=5, field_cfg=stat("short", color="#e34948"), options=STAT_OPTS)
b.add("stat", "Files ingested", f"""SELECT count(*) AS "Files" FROM ops.batch_load_log WHERE stage='ODS_LOAD' AND status='SUCCESS' AND {BATCH}""",
      w=6, h=5, field_cfg=stat("short"), options=STAT_OPTS)
b.add("barchart", "Batch load duration (s) per source file", f"""SELECT file_name AS "Source file",
      round(avg(duration_s)::numeric,3) AS "ODS load (s)",
      round((SELECT avg(v.duration_s) FROM ops.batch_load_log v WHERE v.stage='STAGING_VALIDATE' AND v.source_key=l.source_key AND {BATCH.replace('batch_label','v.batch_label')})::numeric,3) AS "Validation (s)"
      FROM ops.batch_load_log l WHERE stage='ODS_LOAD' AND {BATCH} GROUP BY file_name, source_key ORDER BY 2 DESC""",
      w=12, h=10, field_cfg={"defaults": {"unit": "s", "color": {"mode": "palette-classic"}}, "overrides": []},
      options={**BAR_H, "stacking": "normal"}, desc="Recommended chart: which large file takes the longest. Timestamp_end - Timestamp_start per file.")
b.add("barchart", "Rows processed per batch (by source)", f"""SELECT source_key AS "Source",
      sum(rows_loaded) FILTER (WHERE batch_label='base') AS "base",
      sum(rows_loaded) FILTER (WHERE batch_label LIKE 'refresh_01%') AS "refresh 1",
      sum(rows_loaded) FILTER (WHERE batch_label LIKE 'refresh_02%') AS "refresh 2"
      FROM ops.batch_load_log WHERE stage='ODS_LOAD' GROUP BY 1 ORDER BY 2 DESC""",
      w=12, h=10, field_cfg={"defaults": {"unit": "short", "color": {"mode": "palette-classic"}},
                             "overrides": []}, options={**BAR_V, "yAxis": {"scale": "log"}},
      desc="len(df) after pandas load, per file per refresh (log scale: files differ by 4 orders of magnitude).")
b.add("barchart", "Validation rejects by rule", f"""SELECT split_part(reason,' ',1)||' '||split_part(reason,' ',2)||' '||split_part(reason,' ',3) AS "Rule",
      count(*) AS "Rows" FROM staging.rejects WHERE {BATCH} GROUP BY 1 ORDER BY 2 DESC""", w=12, h=9,
      field_cfg={"defaults": {"unit": "short", "color": {"mode": "fixed", "fixedColor": "#e34948"}}, "overrides": []},
      options=BAR_H)
b.add("table", "Load audit (latest 50)", f"""SELECT started_at AS "Started", batch_label AS "Batch", stage AS "Stage",
      file_name AS "File", rows_read AS "Read", rows_loaded AS "Loaded", rows_rejected AS "Rejected",
      round(duration_s::numeric,2) AS "Seconds", status AS "Status" FROM ops.batch_load_log WHERE {BATCH}
      ORDER BY load_id DESC LIMIT 50""", w=12, h=9)

# ------------------------------------------------------------------ 2 streaming ingestion
s = Board("sdi-stream-ingestion", "SDI · 2 · Streaming Ingestion", "IoT telemetry micro-batches via POST /ingest/stream/iot")
s.add("stat", "Stream throughput (events/s)", """SELECT round(sum(events_committed)/nullif(sum(duration_s),0)) AS "Events/s"
      FROM ops.stream_batch_log WHERE $__timeFilter(committed_at)""", w=6, h=5, field_cfg=stat("short"), options=STAT_OPTS,
      desc="Total records ingested / total stream processing seconds.")
s.add("stat", "End-to-end latency p95 (s)", """SELECT round(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_p95_s)::numeric,3) AS "p95 s"
      FROM ops.stream_batch_log WHERE $__timeFilter(committed_at)""", w=6, h=5, field_cfg=stat("s", 3), options=STAT_OPTS,
      desc="Timestamp_db_insert - Timestamp_source_row_creation (_emitted_at).")
s.add("stat", "Malformed / dropped", """SELECT sum(events_malformed) AS "Dropped" FROM ops.stream_batch_log WHERE $__timeFilter(committed_at)""",
      w=6, h=5, field_cfg=stat("short", color="#e34948"), options=STAT_OPTS)
s.add("stat", "Events committed", """SELECT sum(events_committed) AS "Committed" FROM ops.stream_batch_log WHERE $__timeFilter(committed_at)""",
      w=6, h=5, field_cfg=stat("short"), options=STAT_OPTS)
s.add("timeseries", "Stream throughput per micro-batch (events/s)", """SELECT committed_at AS time,
      throughput_eps AS "Throughput (eps)" FROM ops.stream_batch_log WHERE $__timeFilter(committed_at) ORDER BY 1""",
      w=24, h=9, fmt="time_series", field_cfg=series_cfg("short"), options=TS_OPTS,
      desc="Recommended chart: pipeline stability over the simulation run.")
s.add("timeseries", "End-to-end latency (s)", """SELECT committed_at AS time, latency_avg_s AS "avg", latency_p95_s AS "p95"
      FROM ops.stream_batch_log WHERE $__timeFilter(committed_at) ORDER BY 1""", w=12, h=8, fmt="time_series",
      field_cfg=series_cfg("s"), options=TS_OPTS)
s.add("barchart", "Dropped records by reason", """SELECT CASE WHEN reason LIKE 'duplicate%' THEN 'Duplicate key'
      WHEN reason LIKE '%SensorReadingID%' THEN 'Null key' WHEN reason LIKE 'ValueError: could not convert%' THEN 'Schema mismatch (type)'
      ELSE 'Invalid timestamp / other' END AS "Reason", count(*) AS "Events" FROM ops.stream_rejects
      WHERE $__timeFilter(rejected_at) GROUP BY 1 ORDER BY 2 DESC""", w=12, h=8,
      field_cfg={"defaults": {"unit": "short", "color": {"mode": "fixed", "fixedColor": "#e34948"}}, "overrides": []}, options=BAR_H)
s.add("table", "Stream runs", """SELECT stream_run_id AS "Run", batch_label AS "Batch", count(*) AS "Micro-batches",
      sum(events_received) AS "Received", sum(events_committed) AS "Committed", sum(events_malformed) AS "Dropped",
      round((sum(events_committed)/nullif(extract(epoch FROM max(committed_at)-min(received_at)),0))::numeric) AS "Events/s (wall)",
      round(avg(latency_avg_s)::numeric,3) AS "Avg latency s" FROM ops.stream_batch_log GROUP BY 1,2 ORDER BY min(received_at)""",
      w=24, h=6)

# ------------------------------------------------------------------ 3 storage
st = Board("sdi-storage", "SDI · 3 · Storage & Staging", "Postgres ODS/staging + DuckDB mart footprint and coverage")
LATEST = "captured_at = (SELECT max(captured_at) FROM ops.storage_snapshot)"
st.add("stat", "Postgres database (MB)", f"""SELECT round(size_mb::numeric,1) AS "MB" FROM ops.storage_snapshot
       WHERE layer='database' AND store='postgres' AND {LATEST}""", w=6, h=5, field_cfg=stat("decmbytes", 1), options=STAT_OPTS)
st.add("stat", "DuckDB mart file (MB)", f"""SELECT round(size_mb::numeric,1) AS "MB" FROM ops.storage_snapshot
       WHERE layer='database' AND store='duckdb' AND {LATEST}""", w=6, h=5, field_cfg=stat("decmbytes", 1), options=STAT_OPTS)
st.add("stat", "Rows in staging (all domains)", f"""SELECT sum(row_count) AS "Rows" FROM ops.storage_snapshot
       WHERE layer='staging' AND {LATEST}""", w=6, h=5, field_cfg=stat("short"), options=STAT_OPTS)
st.add("stat", "Mart fact rows", f"""SELECT sum(row_count) AS "Rows" FROM ops.storage_snapshot
       WHERE table_name IN ('mart.fact_sales','mart.fact_hospital_demand') AND {LATEST}""", w=6, h=5, field_cfg=stat("short"), options=STAT_OPTS)
st.add("piechart", "Storage breakdown - % of staging rows by source domain", f"""SELECT domain AS "Domain", row_count AS "Rows"
       FROM ops.storage_snapshot WHERE layer='staging' AND {LATEST} ORDER BY 2 DESC""", w=8, h=10,
       options={"pieType": "donut", "reduceOptions": {"values": True, "calcs": ["lastNotNull"]},
                "legend": {"showLegend": True, "displayMode": "table", "placement": "right", "values": ["percent"]},
                "displayLabels": []},
       field_cfg={"defaults": {"unit": "short", "color": {"mode": "palette-classic"}}, "overrides": []})
st.add("barchart", "Storage footprint (MB) by domain and layer", f"""SELECT domain AS "Domain",
       round(sum(size_mb) FILTER (WHERE layer='ods')::numeric,1) AS "ODS (bronze)",
       round(sum(size_mb) FILTER (WHERE layer='staging')::numeric,1) AS "Staging (silver)"
       FROM ops.storage_snapshot WHERE layer IN ('ods','staging') AND {LATEST} GROUP BY 1 ORDER BY 3 DESC""",
       w=16, h=10, field_cfg={"defaults": {"unit": "decmbytes", "color": {"mode": "palette-classic"}}, "overrides": []},
       options={**BAR_H, "stacking": "normal"})
st.add("barchart", "Fact-table growth per refresh (history is appended, never truncated)", """SELECT batch_label AS "Batch",
       max(row_count) FILTER (WHERE table_name='mart.fact_sales') AS "fact_sales",
       max(row_count) FILTER (WHERE table_name='staging.stg_iot') AS "stg_iot",
       max(row_count) FILTER (WHERE table_name='staging.stg_warehouse') AS "stg_warehouse"
       FROM ops.storage_snapshot GROUP BY 1 ORDER BY 1""", w=12, h=9,
       field_cfg={"defaults": {"unit": "short", "color": {"mode": "palette-classic"}}, "overrides": []}, options=BAR_V)
st.add("table", "Date coverage span (validation of 2023 -> 2026 range)", f"""SELECT layer AS "Layer", table_name AS "Table",
       row_count AS "Rows", min_date AS "Min date", max_date AS "Max date", (max_date-min_date) AS "Span (days)",
       distinct_months AS "Months" FROM ops.storage_snapshot WHERE min_date IS NOT NULL AND layer IN ('staging','mart') AND {LATEST}
       ORDER BY 1,2""", w=12, h=9)

# ------------------------------------------------------------------ 4 query performance
qb = Board("sdi-query-performance", "SDI · 4 · Analytical Query Layer", "Timed OLAP catalog over the DuckDB mart")
qb.add("stat", "Query error rate", f"""SELECT round(100.0*count(*) FILTER (WHERE status='FAILED')/nullif(count(*),0),2) AS "Error %"
       FROM ops.query_log WHERE {BATCH}""", w=6, h=5, field_cfg=stat("percent", 2, thresholds=[
       {"color": "#0ca30c", "value": None}, {"color": "#fab219", "value": 1}, {"color": "#d03b3b", "value": 5}]), options=STAT_OPTS)
qb.add("stat", "Active queries today", """SELECT count(*) AS "Queries" FROM ops.query_log WHERE executed_at::date = current_date""",
       w=6, h=5, field_cfg=stat("short"), options=STAT_OPTS, desc="Counter incremented each time a BI widget/refresh fetches data.")
qb.add("stat", "Median widget latency (optimized)", f"""SELECT round(percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms)::numeric,2) AS "ms"
       FROM ops.query_log WHERE variant='optimized' AND {BATCH}""", w=6, h=5, field_cfg=stat("ms", 2), options=STAT_OPTS)
qb.add("stat", "Slowest widget (p95, any variant)", f"""SELECT round(percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)::numeric,2) AS "ms"
       FROM ops.query_log WHERE {BATCH}""", w=6, h=5, field_cfg=stat("ms", 2), options=STAT_OPTS)
qb.add("barchart", "Average query execution time per dashboard widget (ms)", f"""SELECT left(query_name, 34) AS "Widget",
       round(avg(duration_ms) FILTER (WHERE variant='baseline')::numeric,2) AS "Baseline (fact scan)",
       round(avg(duration_ms) FILTER (WHERE variant='optimized')::numeric,2) AS "Optimized (summary table)"
       FROM ops.query_log WHERE {BATCH} GROUP BY 1 ORDER BY 1""", w=24, h=11,
       field_cfg={"defaults": {"unit": "ms", "color": {"mode": "palette-classic"}}, "overrides": []}, options=BAR_V,
       desc="Recommended chart: spot unoptimised joins. Paired bars show what the summary tables buy.")
qb.add("timeseries", "Queries executed (per minute)", """SELECT $__timeGroupAlias(executed_at, '1m'), count(*) AS "Queries",
       count(*) FILTER (WHERE status='FAILED') AS "Failed" FROM ops.query_log WHERE $__timeFilter(executed_at) GROUP BY 1 ORDER BY 1""",
       w=12, h=8, fmt="time_series", field_cfg=series_cfg("short", draw="bars", fill=80), options=TS_OPTS)
qb.add("table", "Speed-up from pre-aggregation", f"""SELECT query_name AS "Query",
       round(avg(duration_ms) FILTER (WHERE variant='baseline')::numeric,2) AS "Baseline ms",
       round(avg(duration_ms) FILTER (WHERE variant='optimized')::numeric,2) AS "Optimized ms",
       round((avg(duration_ms) FILTER (WHERE variant='baseline') / nullif(avg(duration_ms) FILTER (WHERE variant='optimized'),0))::numeric,1) AS "Speed-up x"
       FROM ops.query_log WHERE {BATCH} GROUP BY 1 HAVING count(*) FILTER (WHERE variant='optimized') > 0 ORDER BY 4 DESC""", w=12, h=8)

if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    for board in (b, s, st, qb):
        (OUT / f"{board.uid}.json").write_text(json.dumps(board.json(), indent=2))
        print("wrote", board.uid, len(board.panels), "panels")
