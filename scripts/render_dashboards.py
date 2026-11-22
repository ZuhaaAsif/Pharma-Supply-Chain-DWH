"""
Renders static PNG previews of the four DE dashboards from the live ops.* metric tables (same
logic as the Grafana panels). Used in the report / video; the interactive versions are Grafana.

    python scripts/render_dashboards.py  -> docs/dashboards/*.png
"""
from __future__ import annotations

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psycopg2
from matplotlib.ticker import FuncFormatter

import os
DSN = os.getenv("SDI_PG_DSN", "postgresql://pharma:pharma@localhost:5432/pharma_dwh")
OUT = Path(__file__).resolve().parents[1] / "docs" / "dashboards"
SURF, INK, INK2, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9"
S = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
GOOD, CRIT = "#0ca30c", "#d03b3b"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "axes.edgecolor": GRID, "axes.labelcolor": INK2,
                     "xtick.color": INK2, "ytick.color": INK2, "axes.titlecolor": INK, "axes.titlesize": 11.5,
                     "axes.titleweight": "bold", "axes.titlelocation": "left", "figure.facecolor": SURF,
                     "axes.facecolor": SURF, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.7,
                     "axes.spines.top": False, "axes.spines.right": False, "axes.spines.left": False,
                     "legend.frameon": False, "legend.fontsize": 9})


def q(sql: str) -> pd.DataFrame:
    with psycopg2.connect(DSN) as c:
        with c.cursor() as cur:
            cur.execute(sql)
            return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])


def short_label(b):
    return str(b).replace("refresh_01_2026Q1", "R1 2026Q1").replace("refresh_02_2026Q2", "R2 2026Q2")


def fmt_n(v):
    v = float(v)
    return f"{v/1e6:.2f}M" if v >= 1e6 else f"{v/1e3:.1f}k" if v >= 1e4 else f"{v:,.0f}" if v >= 100 else f"{v:,.2f}".rstrip("0").rstrip(".")


def tile(ax, label, value, sub=None, color=INK):
    ax.axis("off")
    ax.add_patch(plt.Rectangle((0, 0), 1, 1, transform=ax.transAxes, facecolor="white", edgecolor=GRID, lw=1))
    ax.text(0.06, 0.70, label, transform=ax.transAxes, color=INK2, fontsize=10)
    ax.text(0.06, 0.28, value, transform=ax.transAxes, color=color, fontsize=22, fontweight="bold")
    if sub:
        ax.text(0.94, 0.30, sub, transform=ax.transAxes, color=MUTED, fontsize=8.5, ha="right")


def frame(title, subtitle):
    fig = plt.figure(figsize=(16, 10), dpi=110)
    fig.text(0.02, 0.965, title, fontsize=17, fontweight="bold", color=INK)
    fig.text(0.02, 0.938, subtitle, fontsize=10, color=INK2)
    return fig


def hbar(ax, labels, series: dict, unit="", stacked=True, log=False):
    y = np.arange(len(labels))
    left = np.zeros(len(labels))
    h = 0.62 / (1 if stacked else len(series))
    for i, (name, vals) in enumerate(series.items()):
        vals = np.asarray(vals, dtype=float)
        if stacked:
            ax.barh(y, vals, left=left, height=0.62, color=S[i], label=name, edgecolor=SURF, linewidth=2)
            left += vals
        else:
            ax.barh(y + (i - (len(series) - 1) / 2) * h, vals, height=h, color=S[i], label=name, edgecolor=SURF, linewidth=1.5)
    if stacked:
        for yi, tot in zip(y, left):
            ax.text(tot, yi, f"  {fmt_n(tot)}{unit}", va="center", color=INK, fontsize=9)
    ax.set_yticks(y, labels)
    ax.invert_yaxis()
    ax.grid(axis="y", visible=False)
    if log:
        ax.set_xscale("log")
    if len(series) > 1:
        ax.legend(loc="lower left", bbox_to_anchor=(0, 1.0), ncols=len(series), borderaxespad=0.2)


# ====================================================================== 1 batch
def batch():
    fig = frame("SDI · 1 · Batch Ingestion", "File-watcher → FastAPI ingestion API → Postgres ODS (bronze) → validated staging (silver)  ·  base + 2 refreshes")
    k = q("""SELECT round(100.0*count(*) FILTER (WHERE status='SUCCESS')/count(*),2) sr, count(*) n,
             (SELECT sum(rows_loaded) FROM ops.batch_load_log WHERE stage='ODS_LOAD') rows,
             (SELECT sum(rows_rejected) FROM ops.batch_load_log WHERE stage='STAGING_VALIDATE') rej,
             (SELECT count(*) FROM ops.batch_load_log WHERE stage='ODS_LOAD') files FROM ops.batch_load_log""").iloc[0]
    for i, (lab, val, sub, col) in enumerate([
            ("Batch success rate", f"{k.sr}%", f"{int(k.n)} attempted runs", GOOD if k.sr > 99.5 else "#b07b00"),
            ("Rows processed (ODS)", fmt_n(k.rows), "len(df) per file", INK),
            ("Rows rejected by validation", f"{int(k.rej):,}", "rules R1–R8", CRIT),
            ("Files received", f"{int(k.files)}", "1 failed fault-drill", INK)]):
        tile(fig.add_axes([0.02 + i * 0.245, 0.80, 0.23, 0.11]), lab, val, sub, col)
    d = q("""SELECT l.file_name f, avg(l.duration_s) ods,
             (SELECT avg(v.duration_s) FROM ops.batch_load_log v WHERE v.stage='STAGING_VALIDATE' AND v.source_key=l.source_key AND v.batch_label='base') val
             FROM ops.batch_load_log l WHERE stage='ODS_LOAD' AND batch_label='base' GROUP BY 1, l.source_key ORDER BY 2 DESC""")
    ax = fig.add_axes([0.17, 0.44, 0.32, 0.30])
    hbar(ax, d.f.str.replace(".csv", ""), {"ODS load (read+parse+COPY)": d.ods.astype(float), "Staging validation": d.val.astype(float)}, unit="s")
    ax.set_title("Batch load duration per source file — base load (s)", pad=28)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0f}s"))
    r = q("""SELECT source_key s, sum(rows_loaded) FILTER (WHERE batch_label='base') b,
             sum(rows_loaded) FILTER (WHERE batch_label LIKE 'refresh_01%') r1, sum(rows_loaded) FILTER (WHERE batch_label LIKE 'refresh_02%') r2
             FROM ops.batch_load_log WHERE stage='ODS_LOAD' AND status='SUCCESS' GROUP BY 1 ORDER BY 2 DESC""")
    ax = fig.add_axes([0.62, 0.44, 0.36, 0.30])
    hbar(ax, r.s, {"base": r.b.astype(float), "refresh 1 (2026Q1)": r.r1.astype(float), "refresh 2 (2026Q2)": r.r2.astype(float)},
         stacked=False, log=True)
    ax.set_title("Rows processed per batch, by source (log scale)", pad=28)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: fmt_n(v)))
    rj = q("""SELECT split_part(reason,' ',1) || ' ' || CASE split_part(reason,' ',1) WHEN 'R2' THEN 'null SKU'
              WHEN 'R4' THEN 'non-positive price/qty' WHEN 'R5' THEN 'invalid date' WHEN 'R7' THEN 'duplicate key' ELSE reason END r,
              count(*) n FROM staging.rejects GROUP BY 1 ORDER BY 2 DESC""")
    ax = fig.add_axes([0.17, 0.07, 0.32, 0.28])
    ax.barh(rj.r, rj.n.astype(float), color=S[7], height=0.55, edgecolor=SURF, linewidth=2)
    for i, v in enumerate(rj.n):
        ax.text(v, i, f"  {v}", va="center", fontsize=9, color=INK)
    ax.invert_yaxis(); ax.grid(axis="y", visible=False)
    ax.set_title("Validation rejects by rule (refresh 1 + 2)")
    t = q("""SELECT batch_label, stage, count(*) files, sum(rows_read) rd, sum(rows_loaded) ld, sum(rows_rejected) rj,
             round(sum(duration_s)::numeric,1) s, string_agg(DISTINCT status, '/') st
             FROM ops.batch_load_log GROUP BY 1,2 ORDER BY 1,2""")
    ax = fig.add_axes([0.55, 0.07, 0.43, 0.28]); ax.axis("off")
    ax.set_title("Load audit summary")
    cells = [[short_label(r.batch_label), r.stage.replace("STAGING_", ""), r.files, f"{int(r.rd or 0):,}", f"{int(r.ld or 0):,}", int(r.rj or 0), r.s, r.st] for r in t.itertuples()]
    tb = ax.table(cells, colLabels=["Batch", "Stage", "Files", "Read", "Loaded", "Rejected", "Sec", "Status"], loc="upper left",
                  cellLoc="left", colLoc="left", bbox=[0, 0, 1, 0.92])
    tb.auto_set_font_size(False); tb.set_fontsize(8.6)
    for (rr, cc), cell in tb.get_celld().items():
        cell.set_edgecolor(GRID); cell.set_facecolor("white" if rr else "#f3f2ee")
        if rr == 0: cell.set_text_props(fontweight="bold", color=INK2)
    fig.savefig(OUT / "1_batch_ingestion.png", facecolor=SURF); plt.close(fig)


# ====================================================================== 2 streaming
def stream():
    fig = frame("SDI · 2 · Streaming Ingestion", "IoT telemetry replayed as micro-batches of 500 events → POST /ingest/stream/iot → staging.stg_iot  ·  latency = DB commit − event emitted")
    m = q("""SELECT microbatch_id, batch_label, throughput_eps, latency_avg_s, latency_p95_s, events_malformed, events_committed, duration_s
             FROM ops.stream_batch_log ORDER BY microbatch_id""")
    m["seq"] = np.arange(1, len(m) + 1)
    tot_eps = m.events_committed.sum() / m.duration_s.astype(float).sum()
    for i, (lab, val, sub, col) in enumerate([
            ("Stream throughput (events/s)", f"{tot_eps/1000:.1f}k", "records ÷ processing seconds", INK),
            ("End-to-end latency p95", f"{np.percentile(m.latency_p95_s.astype(float), 95)*1000:.0f} ms", f"avg {m.latency_avg_s.astype(float).mean()*1000:.0f} ms", INK),
            ("Malformed / dropped", f"{int(m.events_malformed.sum())}", f"{100*m.events_malformed.sum()/(m.events_committed.sum()+m.events_malformed.sum()):.2f}% of events", CRIT),
            ("Events committed", fmt_n(m.events_committed.sum()), f"{len(m)} micro-batches", INK)]):
        tile(fig.add_axes([0.02 + i * 0.245, 0.80, 0.23, 0.11]), lab, val, sub, col)
    ax = fig.add_axes([0.05, 0.45, 0.93, 0.28])
    for i, (lab, g) in enumerate(m.groupby("batch_label")):
        ax.plot(g.seq, g.throughput_eps.astype(float), color=S[i], lw=2, label=lab)
        ax.text(g.seq.iloc[-1] + 0.5, float(g.throughput_eps.iloc[-1]), lab.replace("refresh_0", "R").replace("_2026", " "), color=INK2, fontsize=9, va="center")
    ax.set_title("Stream throughput per micro-batch (events / second)"); ax.set_xlabel("micro-batch #")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: fmt_n(v))); ax.set_ylim(bottom=0); ax.legend(loc="lower left")
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


# ====================================================================== 3 storage
def storage():
    fig = frame("SDI · 3 · Storage & Staging", "Postgres ODS (bronze) + staging (silver) and the DuckDB SDI mart (gold)  ·  latest snapshot after refresh 2")
    L = "captured_at = (SELECT max(captured_at) FROM ops.storage_snapshot)"
    db = q(f"SELECT store, size_mb FROM ops.storage_snapshot WHERE layer='database' AND {L}").set_index("store").size_mb
    stg = q(f"SELECT domain, row_count, size_mb FROM ops.storage_snapshot WHERE layer='staging' AND {L} ORDER BY row_count DESC")
    facts = q(f"SELECT sum(row_count) n FROM ops.storage_snapshot WHERE table_name IN ('mart.fact_sales','mart.fact_hospital_demand') AND {L}").n[0]
    for i, (lab, val, sub) in enumerate([("Postgres database", f"{float(db['postgres']):,.0f} MB", "pg_database_size"),
                                         ("DuckDB mart file", f"{float(db['duckdb']):,.1f} MB", "os.path.getsize"),
                                         ("Rows in staging", fmt_n(stg.row_count.sum()), "7 source domains"),
                                         ("Mart fact rows", fmt_n(facts), "2 fact tables")]):
        tile(fig.add_axes([0.02 + i * 0.245, 0.80, 0.23, 0.11]), lab, val, sub)
    ax = fig.add_axes([0.02, 0.40, 0.30, 0.34])
    vals = stg.row_count.astype(float)
    wedges, _ = ax.pie(vals, colors=S[:len(vals)], startangle=90, counterclock=False,
                       wedgeprops=dict(width=0.36, edgecolor=SURF, linewidth=2))
    ax.text(0, 0, f"{fmt_n(vals.sum())}\nrows", ha="center", va="center", fontsize=13, fontweight="bold", color=INK)
    ax.legend(wedges, [f"{d}  {100*v/vals.sum():.1f}%" for d, v in zip(stg.domain, vals)], loc="center left", bbox_to_anchor=(0.95, 0.5))
    ax.set_title("Storage breakdown — % of staging rows by domain", pad=8)
    mb = q(f"""SELECT domain, sum(size_mb) FILTER (WHERE layer='ods') ods, sum(size_mb) FILTER (WHERE layer='staging') stg
              FROM ops.storage_snapshot WHERE layer IN ('ods','staging') AND {L} GROUP BY 1 ORDER BY 3 DESC""")
    ax = fig.add_axes([0.66, 0.42, 0.32, 0.32])
    hbar(ax, mb.domain, {"ODS (bronze)": mb.ods.astype(float), "Staging (silver)": mb.stg.astype(float)}, unit=" MB")
    ax.set_title("Storage footprint (MB) by domain and layer", pad=28)
    g = q("""SELECT batch_label, max(row_count) FILTER (WHERE table_name='mart.fact_sales') fs,
             max(row_count) FILTER (WHERE table_name='mart.fact_hospital_demand') fh,
             max(row_count) FILTER (WHERE table_name='staging.stg_iot') iot FROM ops.storage_snapshot
             WHERE batch_label IN ('base','refresh_01_2026Q1__stream','refresh_02_2026Q2__stream') GROUP BY 1 ORDER BY 1""")
    ax = fig.add_axes([0.05, 0.07, 0.40, 0.25])
    x = np.arange(len(g)); w = 0.26
    for i, (col, name) in enumerate([("fs", "mart.fact_sales"), ("iot", "staging.stg_iot"), ("fh", "mart.fact_hospital_demand")]):
        ax.bar(x + (i - 1) * w, g[col].astype(float), w, color=S[i], label=name, edgecolor=SURF, linewidth=2)
        for xi, v in zip(x, g[col]):
            ax.text(xi + (i - 1) * w, v, fmt_n(v), ha="center", va="bottom", fontsize=8, color=INK)
    ax.set_xticks(x, ["base", "after refresh 1", "after refresh 2"]); ax.grid(axis="x", visible=False)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: fmt_n(v))); ax.legend(loc="upper left", ncols=3, bbox_to_anchor=(0, 1.0))
    ax.set_ylim(0, float(g.fs.max()) * 1.25)
    ax.set_title("Cumulative rows per refresh — history appended, never truncated")
    cv = q(f"""SELECT table_name, row_count, min_date, max_date, (max_date-min_date) span, distinct_months
              FROM ops.storage_snapshot WHERE min_date IS NOT NULL AND layer IN ('staging','mart') AND {L} ORDER BY layer DESC, table_name""")
    ax = fig.add_axes([0.52, 0.07, 0.46, 0.27]); ax.axis("off"); ax.set_title("Date coverage span (2023-01 → 2026-06 expected)")
    tb = ax.table([[r.table_name.replace("staging.stg_", "stg_").replace("mart.", "").replace("_demand", "_dmd"), f"{r.row_count:,}", r.min_date, r.max_date, r.span, r.distinct_months] for r in cv.itertuples()],
                  colLabels=["Table", "Rows", "Min", "Max", "Span d", "Months"], cellLoc="left", colLoc="left", bbox=[0, 0, 1, 0.93])
    tb.auto_set_font_size(False); tb.set_fontsize(8.4)
    for (rr, cc), cell in tb.get_celld().items():
        cell.set_edgecolor(GRID); cell.set_facecolor("white" if rr else "#f3f2ee")
        if rr == 0: cell.set_text_props(fontweight="bold", color=INK2)
    fig.savefig(OUT / "3_storage.png", facecolor=SURF); plt.close(fig)


# ====================================================================== 4 query
def query():
    fig = frame("SDI · 4 · Analytical Query Layer", "15 widget queries timed with a perf_counter wrapper (3 runs after warm-up) after every refresh  ·  DuckDB mart")
    k = q("""SELECT round(100.0*count(*) FILTER (WHERE status='FAILED')/count(*),2) er, count(*) n,
             percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) FILTER (WHERE variant='optimized') med_o,
             percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) FILTER (WHERE variant='baseline') med_b,
             max(duration_ms) mx FROM ops.query_log""").iloc[0]
    for i, (lab, val, sub, col) in enumerate([("Query error rate", f"{k.er}%", "failed ÷ executed", GOOD),
                                              ("Queries executed", f"{int(k.n)}", "3 refreshes × 69", INK),
                                              ("Median latency — summary tables", f"{k.med_o:.1f} ms", f"fact scan {k.med_b:.1f} ms", INK),
                                              ("Slowest single execution", f"{k.mx:.1f} ms", "all sub-second", INK)]):
        tile(fig.add_axes([0.02 + i * 0.245, 0.80, 0.23, 0.11]), lab, val, sub, col)
    d = q("""SELECT query_name, avg(duration_ms) FILTER (WHERE variant='baseline') b, avg(duration_ms) FILTER (WHERE variant='optimized') o
             FROM ops.query_log GROUP BY 1 ORDER BY 1""")
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
    t = q("""SELECT batch_label, round(avg(duration_ms) FILTER (WHERE variant='baseline')::numeric,2) b,
             round(avg(duration_ms) FILTER (WHERE variant='optimized')::numeric,2) o, count(*) n,
             count(*) FILTER (WHERE status='FAILED') f FROM ops.query_log GROUP BY 1 ORDER BY 1""")
    fs = q("SELECT batch_label, max(row_count) n FROM ops.storage_snapshot WHERE table_name='mart.fact_sales' AND batch_label NOT LIKE '%stream' GROUP BY 1").set_index("batch_label").n
    ax = fig.add_axes([0.58, 0.05, 0.40, 0.22]); ax.axis("off"); ax.set_title("Latency stays flat as history grows")
    tb = ax.table([[short_label(r.batch_label), f"{int(fs.get(r.batch_label, 0)):,}", r.b, r.o, r.n, r.f] for r in t.itertuples()],
                  colLabels=["After load", "Fact rows", "Base ms", "Opt. ms", "Queries", "Failed"],
                  cellLoc="left", colLoc="left", bbox=[0, 0.25, 1, 0.65])
    tb.auto_set_font_size(False); tb.set_fontsize(9)
    for (rr, cc), cell in tb.get_celld().items():
        cell.set_edgecolor(GRID); cell.set_facecolor("white" if rr else "#f3f2ee")
        if rr == 0: cell.set_text_props(fontweight="bold", color=INK2)
    fig.savefig(OUT / "4_query_performance.png", facecolor=SURF); plt.close(fig)


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    for f in (batch, stream, storage, query):
        f(); print("rendered", f.__name__)
