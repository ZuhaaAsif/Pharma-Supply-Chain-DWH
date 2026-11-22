"""Builds deliverables/Milestone.1.SDI.DWH.Report.pdf (A4, max 5 pages) with reportlab."""
from __future__ import annotations

from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (Image, KeepTogether, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "deliverables" / "Milestone.1.SDI.DWH.Report.pdf"
NAVY, INK2, GRID, SOFT = colors.HexColor("#1F3A5F"), colors.HexColor("#52514e"), colors.HexColor("#d9d7cf"), colors.HexColor("#f3f2ee")

body = ParagraphStyle("b", fontName="Helvetica", fontSize=11, leading=13.6, spaceAfter=4, alignment=TA_LEFT)
small = ParagraphStyle("s", parent=body, fontSize=8.6, leading=10.4, spaceAfter=0)
cell = ParagraphStyle("c", parent=body, fontSize=8.6, leading=10.2, spaceAfter=0)
cellb = ParagraphStyle("cb", parent=cell, fontName="Helvetica-Bold")
h1 = ParagraphStyle("h1", fontName="Helvetica-Bold", fontSize=13.5, leading=16, textColor=NAVY, spaceBefore=7, spaceAfter=4)
h2 = ParagraphStyle("h2", fontName="Helvetica-Bold", fontSize=11, leading=13.5, textColor=NAVY, spaceBefore=4, spaceAfter=2)
cap = ParagraphStyle("cap", parent=small, textColor=INK2, fontName="Helvetica-Oblique", spaceAfter=4)
bullet = ParagraphStyle("bl", parent=body, leftIndent=11, bulletIndent=2, spaceAfter=2)


def P(t, s=body):
    return Paragraph(t, s)


def B(t):
    return Paragraph(t, bullet, bulletText="•")


def img(path, width_cm):
    from PIL import Image as PI
    w, h = PI.open(path).size
    return Image(str(path), width=width_cm * cm, height=width_cm * cm * h / w)


def table(rows, widths, header=True, font=cell):
    data = [[Paragraph(str(c), cellb if (header and i == 0) else font) for c in r] for i, r in enumerate(rows)]
    t = Table(data, colWidths=[w * cm for w in widths], repeatRows=1 if header else 0)
    st = [("GRID", (0, 0), (-1, -1), 0.4, GRID), ("VALIGN", (0, 0), (-1, -1), "TOP"),
          ("LEFTPADDING", (0, 0), (-1, -1), 3), ("RIGHTPADDING", (0, 0), (-1, -1), 3),
          ("TOPPADDING", (0, 0), (-1, -1), 1.6), ("BOTTOMPADDING", (0, 0), (-1, -1), 1.6)]
    if header:
        st.append(("BACKGROUND", (0, 0), (-1, 0), SOFT))
    t.setStyle(TableStyle(st))
    return t


def footer(c, d):
    c.saveState()
    c.setFont("Helvetica", 8); c.setFillColor(INK2)
    c.drawString(1.7 * cm, 1.0 * cm, "Milestone.1.SDI.DWH.Report · Pharma Cold-Chain DWH + BI · Phase 1 SDI mart")
    c.drawRightString(A4[0] - 1.7 * cm, 1.0 * cm, f"Page {d.page}")
    c.restoreState()


s = []
s.append(P("<b>Milestone 1 — Sales &amp; Demand Intelligence (SDI) Data Warehouse</b>",
           ParagraphStyle("t", fontName="Helvetica-Bold", fontSize=17, leading=20, textColor=NAVY)))
s.append(P("Intelligent Data Architectures, Fall 2026 · Automated DWH + BI pipeline for the pharmaceutical cold supply chain", cap))
s.append(table([["Group members", "<i>Name 1 (roll no.) · Name 2 (roll no.) · Name 3 (roll no.)</i>"],
                ["GitHub", "<i>https://github.com/&lt;group-account&gt;/pharma-sdi-dwh</i> — code, Excel, data, video"]],
               [3.2, 14.4], header=False))
s.append(Spacer(1, 5))
s.append(P("<b>Scope.</b> Milestone 1 implements the SDI mart up to the analytical query layer. The pipeline oversamples all 7 source "
           "files, ingests them through a REST API and a file watcher, validates them into PostgreSQL, models a star schema in "
           "DuckDB and times an OLAP query catalog. <b>Apache Airflow 3.1.8</b> orchestrates every step. Every number below comes from "
           "the final end-to-end Airflow run: base load + two refreshes, <b>7 DAG runs, 59 task instances, all successful</b>."))
s.append(table([
    ["Layer", "Base load (2023–25)", "Refresh 1 (2026 Q1)", "Refresh 2 (2026 Q2)", "Key result"],
    ["Batch ingestion → ODS", "934,448 rows · 7 files · 14.3 s", "44,465 rows · 6 files · 0.8 s", "34,226 rows · 6 files · 0.7 s", "success rate 97.4% (1 deliberate fault drill)"],
    ["Validation → staging", "0 rejected", "119 rejected / 119 injected", "90 rejected / 90 injected", "100% of injected faults caught"],
    ["Streaming (IoT)", "back-filled as batch", "25,471 events · 52 micro-batches", "22,072 events · 45 micro-batches", "17.3k events/s · 34 ms avg latency"],
    ["Star schema (DuckDB)", "527,216 fact rows · 8.2 s", "+28,154 · 0.66 s", "+19,547 · 0.52 s", "0 orphan keys · 12 SCD-2 versions"],
    ["Query layer", "69 timed executions", "69", "69", "0 errors · median 1.6 ms optimized vs 8.8 ms"],
], [3.0, 3.6, 3.6, 3.6, 3.8]))

# ---------------------------------------------------------------- a) Airflow
s.append(P("a) Design of the Airflow pipeline", h1))
s.append(img(ROOT / "docs/diagrams/airflow_pipeline_compact.png", 17.6))
s.append(P("Figure 1 — Three DAGs linked by Airflow Assets (data-aware scheduling); dashed = HTTP calls to the ingestion API.", cap))
s.append(P("The pipeline is split into three DAGs so each can be triggered, retried and monitored on its own. The DAG files are "
           "declarative: all logic lives in the <font face='Courier'>pipeline/</font> package, which keeps task separation clean."))
s.append(B("<b>sdi_oversampling</b> (manual, param <font face='Courier'>refresh_id</font>): <i>plan_refresh → generate → quality_check → "
           "publish</i>. Files are published by atomic rename, so the watcher never sees a half-written file. Publishing emits the "
           "Assets <font face='Courier'>sdi://landing_zone</font> and <font face='Courier'>sdi://stream_queue</font>."))
s.append(B("<b>sdi_batch_pipeline</b> runs when that Asset is updated <i>or</i> every 30 min. A <b>file-watcher sensor</b> (reschedule mode, "
           "soft-fail) waits until a file's size is stable and its SHA-256 is not in <font face='Courier'>ops.file_registry</font>, "
           "so re-dropping the same file is ignored. <b>Dynamic task mapping</b> then creates one <i>ingest_to_ods</i> task per file, "
           "each calling <font face='Courier'>POST /ingest/batch</font> on the FastAPI service. Next come validation, the "
           "<i>transform</i> TaskGroup (star schema + row reconciliation that fails the run on any mismatch), the timed query layer "
           "and storage metrics."))
s.append(B("<b>sdi_stream_ingest</b> (Asset or every 5 min) replays queued IoT telemetry in 500-event micro-batches through "
           "<font face='Courier'>POST /ingest/stream/iot</font> and records throughput, latency and drops per micro-batch."))
s.append(P("<b>Resilience:</b> 2 retries with exponential back-off, <font face='Courier'>max_active_runs=1</font>, failure callbacks into "
           "<font face='Courier'>ops.pipeline_run</font>, and idempotent loads (a re-run deletes and reloads its own batch; natural-key "
           "unique indexes stop duplicates). DuckDB allows only one writer process, so every task that opens the mart runs in a "
           "size-1 Airflow pool, <font face='Courier'>duckdb_mart</font>."))

# ---------------------------------------------------------------- d) DWH approach
s.append(P("d) Problem-solving approach for the DWH", h1))
s.append(P("<b>1 — Understand the grain and the business question first.</b> The SDI questions are forecast vs actual, channel "
           "revenue and margin, and fast vs slow movers. They need two grains: one row per <i>sale</i> and one row per "
           "<i>hospital × medicine × month</i>. That gives two facts sharing conformed dimensions, a mini fact constellation that "
           "Phase 2/3 marts can join later. Profiling showed the channel split the brief asks for (Retail / Hospital / E-commerce) "
           "is not a source column: 109,192 'Physical Pharmacy' sales come from <i>Hospital Pharmacies</i>. So "
           "<font face='Courier'>dim_channel.channel_group</font> is derived from SalesChannel × PharmacyType."))
s.append(KeepTogether([img(ROOT / "docs/diagrams/star_schema.png", 15.5),
                       P("Figure 2 — SDI star schema (DuckDB). DD = degenerate dimension.", cap)]))
s.append(P("<b>2 — Apply the DWH toolkit where the data justifies it.</b> <i>SCD Type 2</i> on <font face='Courier'>dim_medicine</font> "
           "versions prices, cost and status: a sale is joined to the version valid on its date (point-in-time lookup), so "
           "historical margin is never rewritten. The refreshes created 12 versions. <i>SCD Type 1</i> covers pharmacy, hospital "
           "and location. <i>Junk dimensions</i> fold 8 sales flags (1,784 observed combinations) and 2 demand flags out of the "
           "facts. <i>Role-playing</i> <font face='Courier'>dim_date</font> serves as sale, delivery, expiry and demand-month date. "
           "<i>Degenerate dimensions</i> keep batch and lot IDs for Phase 3 traceability, and a −1 <i>unknown member</i> removes NULL "
           "foreign keys. Measures are tagged additive, semi-additive (stock levels) or non-additive (service level %)."))
s.append(P("<b>3 — Medallion layering and incremental loads.</b> Postgres <font face='Courier'>ods</font> (bronze, raw text) → "
           "<font face='Courier'>staging</font> (silver: typed, validated by set-based SQL rules R1–R8 such as price &gt; 0, valid "
           "dates, non-null SKU, foreign key to master and duplicate keys) → DuckDB <font face='Courier'>mart</font> (gold). The mart "
           "pulls only rows above a per-source watermark, so each refresh <b>appends</b>: fact_sales grew 500,000 → 525,892 → "
           "543,177. Summary tables are rebuilt only for the months a refresh touches."))
s.append(P("<b>4 — Optimise, then measure.</b> Facts are inserted ordered by date key, so DuckDB zone maps prune row groups like a "
           "range partition. ART indexes cover natural keys, and <font face='Courier'>agg_sales_monthly</font> / "
           "<font face='Courier'>agg_hospital_monthly</font> act as materialised views. The 15-widget catalog also covers ROLLUP, "
           "CUBE, slice/dice, PIVOT, drill-across and an SCD-2 audit. Every query runs in a baseline (fact scan) and an optimized "
           "variant: summary tables give a <b>1.7×–6.9× speed-up</b>, and every query stays under 25 ms as history grows."))

# ---------------------------------------------------------------- e) Oversampling
s.append(P("e) Oversampling strategy", h1))
s.append(P("<b>Method: seasonal temporal bootstrap.</b> Each refresh adds one new calendar quarter (2026 Q1, then 2026 Q2), so the "
           "two refreshes cover different periods as required. The template is <i>the same quarter one year earlier</i>, which keeps "
           "seasonality. Whole rows (whole shipments and warehouse-lot trajectories) are resampled with replacement. This keeps the "
           "joint distribution of channel × pharmacy × segment × payer; per-column sampling or SMOTE-style interpolation would "
           "break it. All dates in a row shift by the same 364 days (52 weeks), so weekday patterns and intra-row intervals survive. "
           "New primary keys continue each file's own sequence. Foreign keys are re-pointed to entities generated in the same refresh "
           "(sale → new lot → new shipment → new batch), and the business rules found by profiling are re-applied: Gross = Qty × "
           "RRP, COGS = Qty × wholesale price, Tax = 6% of Net, Closing stock = Opening + Received − Dispensed − Wastage, and breach "
           "flags against temperature limits. 10% of SKUs are re-priced per refresh to exercise SCD-2, and 0.3% of rows are "
           "deliberately corrupted to test validation."))
s.append(P("<b>Why this much data.</b> Volume = template rows × the data's own trailing-12-month growth, clipped to 0.90–1.25. "
           "A new quarter therefore has the same density as history: trend charts get no artificial cliff or spike, and quarter-on-"
           "quarter comparisons are like-for-like. Hospital demand is a full panel, so it is exactly 12 × 63 × 3 = 2,268 rows. "
           "Each refresh is large enough to visibly move every KPI, e.g. a different top-10 fast mover list each quarter, yet "
           "small enough to load in seconds."))
s.append(table([
    ["File", "Refresh 1", "Refresh 2", "g (R1 / R2)", "Bad", "File", "Refresh 1", "Refresh 2", "g (R1 / R2)", "Bad"],
    ["01 master", "63 (6 re-priced)", "63 (6 re-priced)", "—", "0", "05 sales", "25,969", "17,337", "0.90 / 0.90", "129"],
    ["02 manufacturing", "56", "35", "0.98 / 1.05", "1", "06 hospital", "2,268", "2,268", "1.03 / 1.04", "12"],
    ["03 delivery", "105", "53", "1.00 / 1.00", "2", "07 IoT (stream)", "25,548", "22,132", "1.00 / 0.99", "69"],
    ["04 warehouse", "16,004", "14,470", "1.25 / 1.25", "65", "<b>Total</b>", "<b>70,013</b>", "<b>56,358</b>", "", "<b>278</b>"],
], [2.5, 2.2, 2.2, 1.75, 1.0, 2.2, 1.5, 1.5, 1.75, 1.0]))
s.append(P("Total = 126,371 new rows (+13.5% on the 934,448 base rows for +16.7% more time). Full per-column treatment: "
           "<font face='Courier'>data/oversampled/DATA_DICTIONARY.md</font>.", cap))

# ---------------------------------------------------------------- c) dashboards
s.append(P("c) Data-engineering dashboards", h1))
s.append(P("Four Grafana dashboards (provisioned as JSON, 31 panels) read the <font face='Courier'>ops.*</font> metric tables, which "
           "every pipeline task writes to. They refresh every 30 s, so a live run shows them change. Previews below are rendered "
           "from the same metrics after the final run."))
grid = Table([[img(ROOT / "docs/dashboards/1_batch_ingestion.png", 8.7), img(ROOT / "docs/dashboards/2_streaming_ingestion.png", 8.7)],
              [img(ROOT / "docs/dashboards/3_storage.png", 8.7), img(ROOT / "docs/dashboards/4_query_performance.png", 8.7)]],
             colWidths=[8.9 * cm, 8.9 * cm])
grid.setStyle(TableStyle([("LEFTPADDING", (0, 0), (-1, -1), 1), ("RIGHTPADDING", (0, 0), (-1, -1), 1), ("BOTTOMPADDING", (0, 0), (-1, -1), 2)]))
s.append(grid)
s.append(P("Figure 3 — Batch ingestion · Streaming ingestion · Storage &amp; staging · Analytical query layer.", cap))
s.append(table([
    ["Dashboard", "Metrics (computation)", "The story it tells"],
    ["1 Batch ingestion", "Load duration = t_end − t_start per file; rows = len(df); success rate = successful ÷ attempted runs; "
     "rejects by rule; recommended horizontal bar of duration per file",
     "The 218 MB sales file dominates: 9 s to load and 20 s to validate of the 47 s base load. Refreshes take about 1 s each. "
     "The single failure (97.4%) is the schema-broken fault-drill file, rejected with a precise error."],
    ["2 Streaming ingestion", "Throughput = events ÷ processing s; latency = DB commit − event emission; malformed count from "
     "the try/except validator; line chart of eps per micro-batch",
     "Throughput holds steady at ~17k events/s across 97 micro-batches, with a few transient dips. 137 events (0.29%) are dropped: "
     "duplicates, null keys, type and timestamp errors. Avg ≈ p95 latency because a micro-batch commits atomically."],
    ["3 Storage &amp; staging", "COUNT(*) per table; pg_total_relation_size and os.path.getsize; MIN/MAX date and distinct months; "
     "donut of rows by domain",
     "Sales is 51% of rows and 60% of MB, followed by IoT. Coverage spans 2023-01 → 2026-06 in every table, and the cumulative "
     "bars show history is appended, never truncated. The DuckDB mart is 14× smaller than Postgres (72 MB vs 1,028 MB)."],
    ["4 Query layer", "perf_counter wrapper per execution; active-query counter; error rate = failed ÷ executed; column chart "
     "of avg ms per widget, baseline vs optimized",
     "0% errors over 207 executions; every widget sub-second (max 23.7 ms). Summary tables are what keep the slower star-joins "
     "(B1–B3) flat as the facts grow."],
], [2.6, 6.7, 8.3]))

# ---------------------------------------------------------------- b) problems
s.append(P("b) Implementation problems and their solutions", h1))
s.append(table([
    ["Problem faced", "Solution"],
    ["Airflow 2.10 does not install on Python 3.13 (requires &lt; 3.13).", "Moved to Airflow 3.1.8 (Task SDK, Assets, AssetOrTimeSchedule); the docker image pins Python 3.12."],
    ["Schema inference typed IoT <i>ShipmentID</i> as numeric: the first 200k readings are warehouse readings with no ShipmentID, "
     "so 82,993 valid rows were rejected as type mismatches.", "Infer from the full file; empty columns and *ID/GLN/GTIN/SSCC columns become text. The contract is frozen in "
     "<font face='Courier'>schemas.py</font>."],
    ["Bulk-posting anomaly: 82,764 sales dated 2025-01-01 and 27,985 dated 2024-01-01 (median 288/day).", "Kept in the DWH (valid rows) but "
     "detected (&gt;10× median) and excluded from oversampling templates and growth estimates."],
    ["Quarter-on-quarter growth was too noisy (the Jan spikes swung it from 0.9 to 3×).", "Trailing-12-month growth, clipped to 0.90–1.25."],
    ["Refresh 2 crashed: the history it learns from (refresh 1) still contained the injected bad rows, e.g. date '2026-13-45'.",
     "History = what validation would accept: rules R1/R2/R4/R5/R7 are re-applied before templating."],
    ["Hospital demand landed in Feb–Apr / May–Jul: the 52-week shift is wrong for a monthly grain.", "Calendar-aligned (+12 months) template for monthly facts; caught by the date-coverage panel."],
    ["Re-delivered master snapshot looked like 57 duplicate rows per refresh.", "Versioned master key (MedicineID, EffectiveFromDate): unchanged rows are skipped and changed rows become SCD-2 versions."],
    ["Stream duplicates were silently swallowed by ON CONFLICT and counted as committed.", "INSERT … RETURNING the keys actually written; the rest are counted as malformed."],
    ["Concurrent batch and stream DAGs both open DuckDB (single-writer lock).", "Size-1 Airflow pool <font face='Courier'>duckdb_mart</font> plus a lock-retry in the connection helper."],
    ["Mapped-task results (lazy XCom sequence) could not be serialised as a return value.", "Materialise to a plain list before returning."],
    ["Timestamps cast to dates in the server's local time zone (IoT showed 2026-01-01).", "Every Postgres session forced to UTC."],
], [7.8, 9.8]))
s.append(P("<b>Key assumptions:</b> the CSV drops simulate nightly source extracts; IoT is the streaming source (its 2023–25 history is "
           "back-filled as a batch); latency is measured on a local deployment, hence milliseconds; Milestone 2 adds BI dashboards on "
           "the mart. The full assumption list is in the Excel workbook.", small))

doc = SimpleDocTemplate(str(OUT), pagesize=A4, leftMargin=1.7 * cm, rightMargin=1.7 * cm, topMargin=1.4 * cm, bottomMargin=1.6 * cm,
                        title="Milestone.1.SDI.DWH.Report", author="SDI group")
doc.build(s, onFirstPage=footer, onLaterPages=footer)
from pypdf import PdfReader
print("pages:", len(PdfReader(str(OUT)).pages))
