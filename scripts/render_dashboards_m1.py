"""
Milestone 1 previews of the four DE dashboards: base load only (refresh runs belong to Milestone 2).
Validation and streaming panels show the TEST runs on the oversampled files, labelled as such.

    python scripts/render_dashboards_m1.py  -> docs/dashboards/m1/*.png
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

from render_dashboards import (CRIT, GOOD, GRID, INK, INK2, S, SURF, fmt_n, frame, hbar, q, tile)  # noqa: E402

OUT = Path(__file__).resolve().parents[1] / "docs" / "dashboards" / "m1"
M1 = "batch_label IN ('base','fault_drill')"
LB = "captured_at = (SELECT max(captured_at) FROM ops.storage_snapshot WHERE batch_label='base')"


def table_ax(ax, cells, cols, title, fs=9):
    ax.axis("off"); ax.set_title(title)
    tb = ax.table(cells, colLabels=cols, cellLoc="left", colLoc="left", bbox=[0, 0, 1, 0.92])
    tb.auto_set_font_size(False); tb.set_fontsize(fs)
    for (rr, cc), cell in tb.get_celld().items():
        cell.set_edgecolor(GRID); cell.set_facecolor("white" if rr else "#f3f2ee")
        if rr == 0:
            cell.set_text_props(fontweight="bold", color=INK2)


def batch():
    fig = frame("SDI · 1 · Batch Ingestion", "File-watcher → FastAPI ingestion API → Postgres ODS (bronze) → validated staging (silver)  ·  base load + 1 fault drill")
    k = q(f"""SELECT round(100.0*count(*) FILTER (WHERE status='SUCCESS')/count(*),2) sr, count(*) n,
              sum(rows_loaded) FILTER (WHERE stage='ODS_LOAD') rows, sum(rows_rejected) FILTER (WHERE stage='STAGING_VALIDATE') rej,
              count(*) FILTER (WHERE stage='ODS_LOAD') files FROM ops.batch_load_log WHERE {M1}""").iloc[0]
    for i, (lab, val, sub, col) in enumerate([
            ("Batch success rate", f"{k.sr}%", f"{int(k.n)} runs incl. 1 fault drill", "#b07b00"),
            ("Rows processed (ODS)", fmt_n(k.rows), "len(df) per file", INK),
            ("Rows rejected — base load", f"{int(k.rej or 0):,}", "instructor data is clean", INK),
            ("Files received", f"{int(k.files)}", "7 sources + 1 broken file", INK)]):
        tile(fig.add_axes([0.02 + i * 0.245, 0.80, 0.23, 0.11]), lab, val, sub, col)
    d = q("""SELECT l.file_name f, l.duration_s ods,
             (SELECT v.duration_s FROM ops.batch_load_log v WHERE v.stage='STAGING_VALIDATE' AND v.source_key=l.source_key AND v.batch_label='base') val
             FROM ops.batch_load_log l WHERE stage='ODS_LOAD' AND batch_label='base' ORDER BY 2 DESC""")
    ax = fig.add_axes([0.17, 0.44, 0.32, 0.30])
    hbar(ax, d.f.str.replace(".csv", ""), {"ODS load (read+parse+COPY)": d.ods.astype(float), "Staging validation": d.val.astype(float)}, unit="s")
    ax.set_title("Batch load duration per source file (s)", pad=28)
    ax.set_xlim(right=float((d.ods.astype(float) + d.val.astype(float)).max()) * 1.22)
    r = q("SELECT file_name f, rows_loaded n FROM ops.batch_load_log WHERE stage='ODS_LOAD' AND batch_label='base' ORDER BY 2 DESC")
    ax = fig.add_axes([0.68, 0.44, 0.29, 0.30])
    ax.barh(r.f.str.replace(".csv", ""), r.n.astype(float), color=S[0], height=0.6, edgecolor=SURF, linewidth=2)
    for i, v in enumerate(r.n):
        ax.text(v, i, f"  {fmt_n(v)}", va="center", fontsize=9, color=INK)
    ax.set_xscale("log"); ax.invert_yaxis(); ax.grid(axis="y", visible=False)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: fmt_n(v))); ax.set_xlim(right=float(r.n.max()) * 6)
    ax.set_title("Rows processed per file (log scale)")
    rj = q("""SELECT split_part(reason,' ',1) || ' ' || CASE split_part(reason,' ',1) WHEN 'R2' THEN 'null SKU'
              WHEN 'R4' THEN 'non-positive price/qty' WHEN 'R5' THEN 'invalid date' WHEN 'R7' THEN 'duplicate key' ELSE reason END r,
              count(*) n FROM staging.rejects GROUP BY 1 ORDER BY 2 DESC""")
    ax = fig.add_axes([0.17, 0.07, 0.32, 0.27])
    ax.barh(rj.r, rj.n.astype(float), color=S[7], height=0.55, edgecolor=SURF, linewidth=2)
    for i, v in enumerate(rj.n):
        ax.text(v, i, f"  {v}", va="center", fontsize=9, color=INK)
    ax.invert_yaxis(); ax.grid(axis="y", visible=False)
    ax.set_title("Validation test: injected bad rows caught, by rule (209 / 209)")
    t = q(f"""SELECT batch_label, stage, file_name, rows_read, rows_loaded, rows_rejected, round(duration_s::numeric,2) s, status
              FROM ops.batch_load_log WHERE {M1} AND (stage='ODS_LOAD') ORDER BY load_id""")
    table_ax(fig.add_axes([0.55, 0.07, 0.43, 0.27]),
             [[r.batch_label, r.file_name.replace(".csv", "").replace("_data", "").replace("fault_drill__", "")[:26], f"{int(r.rows_read or 0):,}", r.s, r.status] for r in t.itertuples()],
             ["Batch", "File", "Rows", "Sec", "Status"], "Load audit (ODS stage)", fs=8.2)
    fig.savefig(OUT / "1_batch_ingestion.png", facecolor=SURF); plt.close(fig)


def stream():
    fig = frame("SDI · 2 · Streaming Ingestion", "Stream test: oversampled IoT events replayed in micro-batches of 500 → POST /ingest/stream/iot → staging.stg_iot  ·  latency = DB commit − event emitted")
    m = q("""SELECT throughput_eps, latency_avg_s, latency_p95_s, events_malformed, events_committed, duration_s
             FROM ops.stream_batch_log ORDER BY microbatch_id""")
    m["seq"] = np.arange(1, len(m) + 1)
    eps = m.events_committed.sum() / m.duration_s.astype(float).sum()
    for i, (lab, val, sub, col) in enumerate([
            ("Stream throughput (events/s)", f"{eps/1000:.1f}k", "records ÷ processing seconds", INK),
            ("End-to-end latency p95", f"{np.percentile(m.latency_p95_s.astype(float), 95)*1000:.0f} ms", f"avg {m.latency_avg_s.astype(float).mean()*1000:.0f} ms", INK),
            ("Malformed / dropped", f"{int(m.events_malformed.sum())}", f"{100*m.events_malformed.sum()/(m.events_committed.sum()+m.events_malformed.sum()):.2f}% of events", CRIT),
            ("Events committed", fmt_n(m.events_committed.sum()), f"{len(m)} micro-batches", INK)]):
        tile(fig.add_axes([0.02 + i * 0.245, 0.80, 0.23, 0.11]), lab, val, sub, col)
    ax = fig.add_axes([0.05, 0.45, 0.93, 0.28])
    ax.plot(m.seq, m.throughput_eps.astype(float), color=S[0], lw=2)
    ax.set_title("Stream throughput per micro-batch (events / second)"); ax.set_xlabel("micro-batch #")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: fmt_n(v))); ax.set_ylim(bottom=0)
    ax = fig.add_axes([0.05, 0.07, 0.55, 0.28])
    ax.plot(m.seq, m.latency_avg_s.astype(float) * 1000, color=S[0], lw=2, label="avg")
    ax.plot(m.seq, m.latency_p95_s.astype(float) * 1000, color=S[1], lw=2, label="p95")
    ax.set_title("End-to-end latency per micro-batch (ms)"); ax.set_xlabel("micro-batch #"); ax.set_ylim(bottom=0); ax.legend()
    r = q("""SELECT CASE WHEN reason LIKE 'duplicate%' THEN 'Duplicate key' WHEN reason LIKE '%SensorReadingID%' THEN 'Null key'
             WHEN reason LIKE 'ValueError: could not convert%' THEN 'Schema mismatch (type)' ELSE 'Invalid timestamp' END r, count(*) n
             FROM ops.stream_rejects GROUP BY 1 ORDER BY 2 DESC""")
    ax = fig.add_axes([0.74, 0.07, 0.24, 0.28])
    ax.barh(r.r, r.n.astype(float), color=S[7], height=0.55, edgecolor=SURF, linewidth=2)
    for i, v in enumerate(r.n):
        ax.text(v, i, f" {v}", va="center", fontsize=9, color=INK)
    ax.invert_yaxis(); ax.grid(axis="y", visible=False); ax.set_title("Dropped events by reason")
    fig.savefig(OUT / "2_streaming_ingestion.png", facecolor=SURF); plt.close(fig)


def storage():
    fig = frame("SDI · 3 · Storage & Staging", "Postgres ODS (bronze) + staging (silver) and the DuckDB SDI mart (gold)  ·  snapshot after the base load")
    db = q(f"SELECT store, size_mb FROM ops.storage_snapshot WHERE layer='database' AND {LB}").set_index("store").size_mb
    stg = q(f"SELECT domain, row_count FROM ops.storage_snapshot WHERE layer='staging' AND {LB} ORDER BY row_count DESC")
    facts = q(f"SELECT sum(row_count) n FROM ops.storage_snapshot WHERE table_name IN ('mart.fact_sales','mart.fact_hospital_demand') AND {LB}").n[0]
    for i, (lab, val, sub) in enumerate([("Postgres database", f"{float(db['postgres']):,.0f} MB", "pg_database_size"),
                                         ("DuckDB mart file", f"{float(db['duckdb']):,.1f} MB", "os.path.getsize"),
                                         ("Rows in staging", fmt_n(stg.row_count.sum()), "7 source domains"),
                                         ("Mart fact rows", fmt_n(facts), "2 fact tables")]):
        tile(fig.add_axes([0.02 + i * 0.245, 0.80, 0.23, 0.11]), lab, val, sub)
    ax = fig.add_axes([0.02, 0.40, 0.30, 0.34])
    vals = stg.row_count.astype(float)
    wedges, _ = ax.pie(vals, colors=S[:len(vals)], startangle=90, counterclock=False, wedgeprops=dict(width=0.36, edgecolor=SURF, linewidth=2))
    ax.text(0, 0, f"{fmt_n(vals.sum())}\nrows", ha="center", va="center", fontsize=13, fontweight="bold", color=INK)
    ax.legend(wedges, [f"{d}  {100*v/vals.sum():.1f}%" for d, v in zip(stg.domain, vals)], loc="center left", bbox_to_anchor=(0.95, 0.5))
    ax.set_title("Storage breakdown — % of staging rows by domain", pad=8)
    mb = q(f"""SELECT domain, sum(size_mb) FILTER (WHERE layer='ods') ods, sum(size_mb) FILTER (WHERE layer='staging') stg
              FROM ops.storage_snapshot WHERE layer IN ('ods','staging') AND {LB} GROUP BY 1 ORDER BY 3 DESC""")
    ax = fig.add_axes([0.66, 0.42, 0.32, 0.32])
    hbar(ax, mb.domain, {"ODS (bronze)": mb.ods.astype(float), "Staging (silver)": mb.stg.astype(float)}, unit=" MB")
    ax.set_title("Storage footprint (MB) by domain and layer", pad=28)
    ax.set_xlim(right=float((mb.ods.astype(float) + mb.stg.astype(float)).max()) * 1.25)
    mt = q(f"""SELECT replace(table_name,'mart.','') t, row_count n FROM ops.storage_snapshot
              WHERE layer='mart' AND {LB} ORDER BY row_count DESC""")
    ax = fig.add_axes([0.14, 0.07, 0.32, 0.26])
    ax.barh(mt.t, mt.n.astype(float), color=S[2], height=0.62, edgecolor=SURF, linewidth=1.5)
    for i, v in enumerate(mt.n):
        ax.text(v, i, f" {fmt_n(v)}", va="center", fontsize=8.5, color=INK)
    ax.set_xscale("log"); ax.invert_yaxis(); ax.grid(axis="y", visible=False); ax.set_xlim(right=float(mt.n.max()) * 8)
    ax.tick_params(axis="y", labelsize=8.5); ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: fmt_n(v)))
    ax.set_title("Mart tables after the base load (rows, log scale)")
    cv = q(f"""SELECT table_name, row_count, min_date, max_date, (max_date-min_date) span, distinct_months
              FROM ops.storage_snapshot WHERE min_date IS NOT NULL AND layer IN ('staging','mart') AND {LB} ORDER BY layer DESC, table_name""")
    table_ax(fig.add_axes([0.52, 0.07, 0.46, 0.27]),
             [[r.table_name.replace("staging.stg_", "stg_").replace("mart.", "").replace("_demand", "_dmd"), f"{r.row_count:,}",
               r.min_date, r.max_date, r.span, r.distinct_months] for r in cv.itertuples()],
             ["Table", "Rows", "Min", "Max", "Span d", "Months"], "Date coverage span (validates the 2023 → 2025 history)", fs=8.4)
    fig.savefig(OUT / "3_storage.png", facecolor=SURF); plt.close(fig)


def query():
    fig = frame("SDI · 4 · Analytical Query Layer", "15 widget queries timed with a perf_counter wrapper (3 runs after warm-up) after the base load  ·  DuckDB mart")
    B = "batch_label='base'"
    k = q(f"""SELECT round(100.0*count(*) FILTER (WHERE status='FAILED')/count(*),2) er, count(*) n,
              percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) FILTER (WHERE variant='optimized') mo,
              percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) FILTER (WHERE variant='baseline') mb,
              max(duration_ms) mx FROM ops.query_log WHERE {B}""").iloc[0]
    for i, (lab, val, sub, col) in enumerate([("Query error rate", f"{k.er}%", "failed ÷ executed", GOOD),
                                              ("Queries executed", f"{int(k.n)}", "23 variants × 3 timed runs", INK),
                                              ("Median latency — summary tables", f"{k.mo:.1f} ms", f"fact scan {k.mb:.1f} ms", INK),
                                              ("Slowest single execution", f"{k.mx:.1f} ms", "all sub-second", INK)]):
        tile(fig.add_axes([0.02 + i * 0.245, 0.80, 0.23, 0.11]), lab, val, sub, col)
    d = q(f"""SELECT query_name, avg(duration_ms) FILTER (WHERE variant='baseline') b, avg(duration_ms) FILTER (WHERE variant='optimized') o
              FROM ops.query_log WHERE {B} GROUP BY 1 ORDER BY 1""")
    ax = fig.add_axes([0.05, 0.47, 0.93, 0.26])
    x = np.arange(len(d)); w = 0.38
    ax.bar(x - w / 2, d.b.astype(float), w, color=S[0], label="Baseline — star join over fact table", edgecolor=SURF, linewidth=2)
    ax.bar(x + w / 2, d.o.astype(float).fillna(0), w, color=S[1], label="Optimized — pre-aggregated summary table", edgecolor=SURF, linewidth=2)
    ax.set_xticks(x, [" ".join(n.split("_")[:3]) for n in d.query_name], rotation=25, ha="right", fontsize=8.5)
    ax.set_ylabel("avg ms"); ax.grid(axis="x", visible=False); ax.legend(loc="upper left")
    ax.set_title("Average query execution time per dashboard widget (ms)")
    sp = d.dropna().assign(x=lambda t: t.b / t.o).sort_values("x")
    ax = fig.add_axes([0.20, 0.05, 0.28, 0.22])
    ax.barh(sp.query_name.map(lambda n: " ".join(n.split("_")[:3])), sp.x, color=S[2], height=0.55, edgecolor=SURF, linewidth=2)
    for i, v in enumerate(sp.x):
        ax.text(v, i, f"  {v:.1f}×", va="center", fontsize=9, color=INK)
    ax.grid(axis="y", visible=False); ax.set_title("Speed-up from pre-aggregation (baseline ÷ optimized)")
    t = q(f"""SELECT left(dashboard, 30) dsh, count(DISTINCT query_name) nq,
              round((percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) FILTER (WHERE variant='baseline'))::numeric,2) b,
              round((percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) FILTER (WHERE variant='optimized'))::numeric,2) o
              FROM ops.query_log WHERE {B} GROUP BY 1 ORDER BY 1""")
    table_ax(fig.add_axes([0.58, 0.05, 0.40, 0.22]),
             [[{"A": "A Forecast vs Actual", "B": "B Channel Revenue", "C": "C Fast/Slow Movers"}.get(r.dsh[0], "OLAP operations"), r.nq, r.b, r.o if r.o is not None else "—"] for r in t.itertuples()],
             ["Dashboard", "Queries", "Median base ms", "Median opt. ms"], "Latency by target dashboard", fs=9)
    fig.savefig(OUT / "4_query_performance.png", facecolor=SURF); plt.close(fig)


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    for fn in (batch, stream, storage, query):
        fn(); print("rendered", fn.__name__)
