"""Builds deliverables/Milestone.1.SDI.DWH.Excel.xlsx from the live mart + ops metrics (formulas for every summary)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import pandas as pd
import psycopg2
from openpyxl import Workbook
from openpyxl.chart import BarChart, Reference
from openpyxl.comments import Comment
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline import config  # noqa: E402

OUT = ROOT / "deliverables" / "Milestone.1.SDI.DWH.Excel.xlsx"
PG = psycopg2.connect(config.PG_DSN, options="-c timezone=UTC")
DK = duckdb.connect(str(config.DUCKDB_PATH), read_only=True)
F = "Arial"
HDR = PatternFill("solid", fgColor="1F3A5F")
SUB = PatternFill("solid", fgColor="E8EEF6")
KPI = PatternFill("solid", fgColor="F3F2EE")
THIN = Side(style="thin", color="D0D0D0")
BOX = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
BLUE = Font(name=F, color="0000FF", size=10)


def pg(sql):
    with PG.cursor() as cur:
        cur.execute(sql)
        return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])


def title(ws, text, sub=None):
    ws["A1"] = text
    ws["A1"].font = Font(name=F, size=14, bold=True, color="1F3A5F")
    if sub:
        ws["A2"] = sub
        ws["A2"].font = Font(name=F, size=9, italic=True, color="555555")


def write_table(ws, df: pd.DataFrame, r0: int, c0: int = 1, name: str | None = None, widths=None, fmt=None):
    for j, col in enumerate(df.columns):
        c = ws.cell(row=r0, column=c0 + j, value=str(col))
        c.font = Font(name=F, bold=True, color="FFFFFF", size=10); c.fill = HDR
        c.alignment = Alignment(wrap_text=True, vertical="center"); c.border = BOX
    for i, row in enumerate(df.itertuples(index=False), start=1):
        for j, v in enumerate(row):
            if isinstance(v, float) and pd.isna(v):
                v = None
            if hasattr(v, "item"):
                v = v.item()
            if isinstance(v, pd.Timestamp):
                v = v.to_pydatetime().replace(tzinfo=None)
            elif hasattr(v, "tzinfo") and getattr(v, "tzinfo", None) is not None:
                v = v.replace(tzinfo=None)
            c = ws.cell(row=r0 + i, column=c0 + j, value=v)
            c.font = Font(name=F, size=10); c.border = BOX
            if fmt and df.columns[j] in fmt:
                c.number_format = fmt[df.columns[j]]
    last = r0 + len(df)
    if name and len(df):
        ref = f"{get_column_letter(c0)}{r0}:{get_column_letter(c0 + len(df.columns) - 1)}{last}"
        t = Table(displayName=name, ref=ref)
        t.tableStyleInfo = TableStyleInfo(name="TableStyleLight9", showRowStripes=True)
        ws.add_table(t)
    for j, col in enumerate(df.columns):
        w = (widths or {}).get(col) or min(55, max(10, len(str(col)) + 2, int(df[col].astype(str).str.len().quantile(0.9)) + 2 if len(df) else 10))
        ws.column_dimensions[get_column_letter(c0 + j)].width = w
    ws.freeze_panes = ws.cell(row=r0 + 1, column=c0)
    return last


def kpi(ws, r, label, formula, fmt="#,##0", note=None):
    a, b = ws.cell(row=r, column=1, value=label), ws.cell(row=r, column=2, value=formula)
    a.font = Font(name=F, bold=True, size=10); b.font = Font(name=F, size=11, bold=True)
    a.fill = b.fill = KPI; a.border = b.border = BOX; b.number_format = fmt
    if note:
        c = ws.cell(row=r, column=3, value=note); c.font = Font(name=F, size=9, italic=True, color="555555")


wb = Workbook()

# ============================================================== 0 README
ws = wb.active; ws.title = "README"
title(ws, "Milestone 1 — SDI (Sales & Demand Intelligence) Data Mart", "Pharma Cold-Chain DWH + BI · Intelligent Data Architectures, Fall 2026")
rows = [
    ("Group", "Fill in member names / roll numbers in the README.md of the GitHub repo (shown on the repo home page)."),
    ("Scope", "Milestone 1 only — pipeline up to the analytical query layer: oversampling → ingestion (batch + stream) → Postgres staging → DuckDB star schema → timed OLAP queries, orchestrated by Airflow 3.1.8. Refreshing and BI belong to Milestone 2 and are not included."),
    ("Data", "Instructor dataset (7 CSVs, 2023-01 → 2025-12, 934,448 rows) loaded as the base; oversampled data for 2 future refreshes generated (2026Q1, 2026Q2)."),
    ("Numbers", "Figures come from the Airflow run of the base load (ops.* metric tables and the DuckDB mart). Summary cells are formulas over the raw log sheets."),
    ("", ""),
    ("Sheet", "What it shows"),
    ("Bus_Matrix", "Conformed dimensions × fact tables (Kimball bus matrix)"),
    ("Star_Schema", "Every mart table and column with type, role, SCD type"),
    ("Source_to_Target", "Mapping of source CSV columns to mart columns + transformation rule"),
    ("DWH_Techniques", "SCD, junk, role-playing, degenerate dimensions, fact types, aggregates — where and why"),
    ("Validation_Rules", "Staging rules R1–R8, base-load rejects, and the fault-injection test on the oversampled files (formulas)"),
    ("Oversampling", "Rows generated per file for each future refresh, template rows, growth factor, fault injection (formulas)"),
    ("Batch_Ingestion", "Base-load log per file: duration, rows, status + KPI formulas (success rate, rows, rejects)"),
    ("Streaming", "Stream-ingestion test: per-micro-batch throughput, latency, malformed + KPI formulas"),
    ("Storage", "Row counts, MB, date coverage per table after the base load (formulas for span)"),
    ("Query_Performance", "Median latency per widget query, baseline vs optimized + speed-up formulas"),
    ("Data_Dictionary", "All 272 source columns: type, meaning, example, how the generator produced them"),
    ("Assumptions", "Project assumptions"),
]
for i, (a, b) in enumerate(rows, start=4):
    ws.cell(row=i, column=1, value=a).font = Font(name=F, bold=True, size=10)
    ws.cell(row=i, column=2, value=b).font = Font(name=F, size=10)
    ws.cell(row=i, column=2).alignment = Alignment(wrap_text=True)
for r in (9,):
    for c in (1, 2):
        ws.cell(row=r, column=c).fill = HDR; ws.cell(row=r, column=c).font = Font(name=F, bold=True, color="FFFFFF")
ws.column_dimensions["A"].width = 20; ws.column_dimensions["B"].width = 120

# ============================================================== 1 Bus matrix
ws = wb.create_sheet("Bus_Matrix")
title(ws, "Bus matrix — SDI mart", "✔ = fact table references the dimension (conformed dims are shared with Phase 2/3 marts)")
bus = pd.DataFrame([
    ["dim_date (sale date)", "✔", "", "Conformed, role-playing"], ["dim_date (pharmacy delivery date)", "✔", "", "Role-playing"],
    ["dim_date (batch expiry date)", "✔", "", "Role-playing"], ["dim_date (demand month)", "", "✔", "Role-playing"],
    ["dim_medicine", "✔", "✔", "Conformed, SCD Type 2"], ["dim_location", "✔", "✔", "Conformed, SCD Type 1"],
    ["dim_hospital", "✔", "✔", "SCD Type 1 (−1 = not a hospital sale)"], ["dim_pharmacy", "✔", "", "SCD Type 1"],
    ["dim_channel", "✔", "", "Derives Retail / Hospital / E-commerce"], ["dim_sales_profile", "✔", "", "Junk dimension"],
    ["dim_demand_profile", "", "✔", "Junk dimension"],
], columns=["Dimension", "fact_sales (transaction)", "fact_hospital_demand (periodic snapshot)", "Notes"])
write_table(ws, bus, 4, widths={"Dimension": 34, "fact_sales (transaction)": 24, "fact_hospital_demand (periodic snapshot)": 38, "Notes": 42})
for r in range(5, 5 + len(bus)):
    for c in (2, 3):
        ws.cell(row=r, column=c).alignment = Alignment(horizontal="center")

# ============================================================== 2 Star schema
ws = wb.create_sheet("Star_Schema")
title(ws, "Star schema — all mart tables and columns", "Source: DuckDB information_schema of data/warehouse/sdi_mart.duckdb; row counts after the base load")
cols = DK.execute("""SELECT table_name, column_name, data_type, ordinal_position FROM information_schema.columns
                 WHERE table_schema='mart' AND table_name NOT LIKE 'etl%' ORDER BY table_name, ordinal_position""").df()
_base = pg("""SELECT table_name, row_count FROM ops.storage_snapshot WHERE store='duckdb' AND layer='mart'
               AND captured_at = (SELECT max(captured_at) FROM ops.storage_snapshot WHERE batch_label='base')""")
rowcount = {t.replace("mart.", ""): int(n) for t, n in zip(_base.table_name, _base.row_count)}
TTYPE = {"fact_sales": "Fact — transactional", "fact_hospital_demand": "Fact — periodic snapshot (monthly)",
         "agg_sales_monthly": "Aggregate fact (summary table)", "agg_hospital_monthly": "Aggregate fact (summary table)",
         "dim_medicine": "Dimension — SCD Type 2", "dim_sales_profile": "Junk dimension", "dim_demand_profile": "Junk dimension",
         "dim_date": "Dimension — role-playing", "dim_hospital": "Dimension — SCD Type 1", "dim_pharmacy": "Dimension — SCD Type 1",
         "dim_location": "Conformed dimension — SCD Type 1", "dim_channel": "Dimension — static/derived"}


def role(t, c):
    if c.endswith("_sk") and t.startswith("dim_") and c.replace("_sk", "") in t:
        return "Surrogate key (PK)"
    if c.endswith("_sk") or c.endswith("_date_key") and t.startswith("fact"):
        return "Foreign key"
    if c == "date_key":
        return "Surrogate key (PK, yyyymmdd)"
    if t.startswith("fact") and c in ("sales_transaction_id", "production_batch_id", "warehouse_lot_id", "pharmacy_delivery_id",
                                      "prescription_id", "source_warehouse_id", "hospital_demand_record_id"):
        return "Degenerate dimension"
    if c in ("effective_from", "effective_to", "is_current", "version", "scd_hash"):
        return "SCD-2 housekeeping"
    if c in ("unit_mfg_cost_usd", "wholesale_price_usd", "retail_price_usd", "product_status") and t == "dim_medicine":
        return "SCD-2 tracked attribute"
    if t.startswith(("fact", "agg")) and cols[(cols.table_name == t) & (cols.column_name == c)].data_type.iloc[0] in ("INTEGER", "DOUBLE", "BIGINT", "HUGEINT"):
        if c in ("opening_stock_packs", "closing_stock_packs"):
            return "Measure — semi-additive"
        if c in ("service_level_pct", "avg_lead_time_days", "demand_trend_index", "unit_price_usd", "days_to_expiry_at_sale"):
            return "Measure — non-additive"
        return "Measure — additive"
    if c in ("batch_label", "loaded_at", "loaded_batch", "updated_batch", "profile_hash"):
        return "Audit / lineage"
    return "Attribute"


cols["table_type"] = cols.table_name.map(TTYPE)
cols["role"] = [role(t, c) for t, c in zip(cols.table_name, cols.column_name)]
cols["table_rows"] = cols.table_name.map(rowcount)
out = cols[["table_name", "table_type", "table_rows", "column_name", "data_type", "role"]]
out.columns = ["Table", "Table type", "Rows (base load)", "Column", "Data type", "Role"]
write_table(ws, out, 4, name="StarSchema", fmt={"Rows (base load)": "#,##0"})

# ============================================================== 3 Source to target
ws = wb.create_sheet("Source_to_Target")
title(ws, "Source-to-target mapping (SDI mart)", "Staging column (Postgres, silver) → mart column (DuckDB, gold)")
s2t = pd.DataFrame([
    ["05_pharmacy_sales_data", "SalesTransactionID", "fact_sales.sales_transaction_id", "Degenerate dimension, natural key, de-duplicated by R7/R8"],
    ["05_pharmacy_sales_data", "SalesDate", "fact_sales.sale_date_key", "yyyymmdd → dim_date (role: sale date)"],
    ["05_pharmacy_sales_data", "PharmacyDeliveryDate", "fact_sales.delivery_date_key", "yyyymmdd → dim_date (role: delivery date); −1 if null"],
    ["05_pharmacy_sales_data", "BatchExpiryDate", "fact_sales.expiry_date_key", "yyyymmdd → dim_date (role: expiry date)"],
    ["05_pharmacy_sales_data", "MedicineID + SalesDate", "fact_sales.medicine_sk", "Point-in-time SCD-2 lookup: SalesDate BETWEEN effective_from AND effective_to"],
    ["05_pharmacy_sales_data", "PharmacyID", "fact_sales.pharmacy_sk / location_sk", "Lookup dim_pharmacy (SCD1); location via pharmacy city/state"],
    ["05_pharmacy_sales_data", "HospitalID", "fact_sales.hospital_sk", "Lookup dim_hospital; −1 'Not a hospital sale' when null"],
    ["05_pharmacy_sales_data", "SalesChannel + PharmacyType", "fact_sales.channel_sk", "dim_channel.channel_group = E-commerce if Ecommerce; Hospital if Hospital Pharmacy; else Retail"],
    ["05_pharmacy_sales_data", "CustomerSegment, PayerType, PaymentMethod, PrescriptionRequiredFlag, PromotionCode, EcommerceDeliveryStatus, ReturnFlag, SaleStatus",
     "fact_sales.sales_profile_sk", "md5 of the 8 flags → junk dimension (PromotionCode null → 'NONE')"],
    ["05_pharmacy_sales_data", "QuantitySoldPacks … GrossMarginUSD", "fact_sales measures", "Copied (typed in staging); additive"],
    ["05_pharmacy_sales_data", "ProductionBatchID, WarehouseLotID, SourceWarehouseID, PharmacyDeliveryID, PrescriptionID", "fact_sales degenerate dims", "Kept for drill-through / traceability (Phase 3)"],
    ["06_hospital_demand_data", "DemandMonth", "fact_hospital_demand.month_date_key", "yyyymmdd of month start → dim_date (role: demand month)"],
    ["06_hospital_demand_data", "HospitalID + hospital attributes", "dim_hospital", "SCD1 upsert from latest month; bed_size_band derived"],
    ["06_hospital_demand_data", "City, State", "dim_location", "Conformed with pharmacy locations; census_region derived from state"],
    ["06_hospital_demand_data", "MedicineID + DemandMonth", "fact_hospital_demand.medicine_sk", "Point-in-time SCD-2 lookup"],
    ["06_hospital_demand_data", "MedicationRequestStatus, FormularyStatus", "fact_hospital_demand.demand_profile_sk", "Junk dimension"],
    ["06_hospital_demand_data", "ForecastDemandPacks … DemandTrendIndex", "fact_hospital_demand measures", "Additive / semi-additive (stock) / non-additive (%, index)"],
    ["01_master_data", "All descriptive columns", "dim_medicine (Type-1 attributes)", "Overwrite across all versions"],
    ["01_master_data", "WholesalePriceUSD, RecommendedRetailPriceUSD, UnitManufacturingCostUSD, ProductStatus", "dim_medicine (Type-2 attributes)",
     "Hash change → expire current row (effective_to = new date − 1) and insert version+1"],
    ["01_master_data", "EffectiveFromDate", "dim_medicine.effective_from", "First version backdated to 1900-01-01 so all history resolves"],
], columns=["Source file", "Source column(s)", "Target", "Transformation rule"])
write_table(ws, s2t, 4, widths={"Source file": 24, "Source column(s)": 60, "Target": 40, "Transformation rule": 80})
for r in range(5, 5 + len(s2t)):
    for c in (2, 4):
        ws.cell(row=r, column=c).alignment = Alignment(wrap_text=True, vertical="top")

# ============================================================== 4 DWH techniques
ws = wb.create_sheet("DWH_Techniques")
title(ws, "DWH techniques used and why")
tech = pd.DataFrame([
    ["Slowly Changing Dimension Type 2", "dim_medicine", "Prices/cost/status change over time; revenue must be analysed at the price valid on the sale date. Each sale is joined to the version valid on its date (point-in-time lookup)."],
    ["Slowly Changing Dimension Type 1", "dim_pharmacy, dim_hospital, dim_location", "Corrections (names, bed counts) should overwrite; history not analytically relevant."],
    ["Junk dimension", "dim_sales_profile (8 flags), dim_demand_profile (2 flags)", "Removes 10 low-cardinality columns from the fact table; 1,784 observed combinations instead of 8 FK columns."],
    ["Role-playing dimension", "dim_date ×4 (sale, delivery, expiry, demand month)", "One physical calendar reused under several meanings; supports expiry-at-sale analysis."],
    ["Degenerate dimension", "sales_transaction_id, production_batch_id, warehouse_lot_id, …", "Identifiers with no attributes of their own, kept for drill-through and Phase 3 traceability."],
    ["Conformed dimensions", "dim_date, dim_medicine, dim_location", "Shared by both SDI facts now and by Phase 2/3 marts later (fact constellation)."],
    ["Transactional fact", "fact_sales", "Finest grain (1 row per sale); all measures additive."],
    ["Periodic snapshot fact", "fact_hospital_demand", "Monthly state per hospital × medicine; stock levels are semi-additive (never sum over months)."],
    ["Unknown member (−1)", "every dimension", "No NULL foreign keys in facts; orphan count is a monitored quality metric (0 in all loads)."],
    ["Aggregate / summary tables", "agg_sales_monthly, agg_hospital_monthly", "Materialised-view equivalent; refreshed incrementally only for touched months; see Query_Performance for the speed-up."],
    ["Physical clustering", "facts inserted ORDER BY date key", "DuckDB zone maps prune row groups on date filters (implicit range partitioning)."],
    ["Indexes", "ART indexes on natural keys", "Point look-ups / drill-through by transaction or lot."],
    ["Gold Parquet export", "data/warehouse/gold/fact_sales (hive year/month)", "Partitioned columnar files for external BI tools (medallion gold layer)."],
    ["Incremental load with watermark", "mart.etl_watermark (_stg_id)", "Loads append only rows above the watermark; the DWH is never truncated (history preserved)."],
], columns=["Technique", "Where", "Why"])
write_table(ws, tech, 3, widths={"Technique": 32, "Where": 48, "Why": 100})

# ============================================================== 5 Validation rules
ws = wb.create_sheet("Validation_Rules")
title(ws, "Staging validation rules (bronze → silver)", "Counts are COUNTIFS formulas over the Rejects_Log sheet; the rules also run on streamed IoT events")
rules = [("R1", "Null natural key"), ("R2", "Null SKU (MedicineID)"), ("R3", "Unknown SKU — FK to master"),
         ("R4", "Non-positive price / quantity / forecast (price > 0)"), ("R5", "Invalid or out-of-range date (2020–2030)"),
         ("R6", "Type mismatch on any typed column"), ("R7", "Duplicate key inside the batch"), ("R8", "Duplicate key already in staging")]
hdr = ["Rule", "Description", "Base load (instructor data)", "Test: oversampled set 1", "Test: oversampled set 2", "Total"]
labels = ["base", "refresh_01_2026Q1", "refresh_02_2026Q2"]
for j, h in enumerate(hdr, 1):
    c = ws.cell(row=4, column=j, value=h); c.font = Font(name=F, bold=True, color="FFFFFF"); c.fill = HDR; c.border = BOX
    c.alignment = Alignment(wrap_text=True, vertical="center")
for i, (code, desc) in enumerate(rules, start=5):
    ws.cell(row=i, column=1, value=code); ws.cell(row=i, column=2, value=desc)
    for j, lbl in enumerate(labels, start=3):
        ws.cell(row=i, column=j, value=f'=COUNTIFS(Rejects_Log!$E:$E,"{code}*",Rejects_Log!$C:$C,"{lbl}")')
    ws.cell(row=i, column=6, value=f"=SUM(C{i}:E{i})")
    for j in range(1, 7):
        ws.cell(row=i, column=j).font = Font(name=F, size=10); ws.cell(row=i, column=j).border = BOX
ws.cell(row=13, column=2, value="Total rejected").font = Font(name=F, bold=True)
for j in range(3, 7):
    col = get_column_letter(j)
    c = ws.cell(row=13, column=j, value=f"=SUM({col}5:{col}12)"); c.font = Font(name=F, bold=True); c.border = BOX
ws.cell(row=14, column=2, value="Bad rows deliberately injected (from Oversampling sheet)").font = Font(name=F, italic=True)
ws.cell(row=14, column=3, value=0).font = BLUE
ws.cell(row=14, column=4, value='=SUMIFS(Oversampling!$G:$G,Oversampling!$A:$A,"refresh_01_2026Q1",Oversampling!$B:$B,"<>iot")')
ws.cell(row=14, column=5, value='=SUMIFS(Oversampling!$G:$G,Oversampling!$A:$A,"refresh_02_2026Q2",Oversampling!$B:$B,"<>iot")')
ws.cell(row=15, column=2, value="Catch rate (rejected ÷ injected)").font = Font(name=F, bold=True)
for j in (4, 5):
    col = get_column_letter(j)
    c = ws.cell(row=15, column=j, value=f"=IFERROR({col}13/{col}14,0)"); c.number_format = "0.0%"; c.font = Font(name=F, bold=True)
ws.cell(row=17, column=2, value="The instructor data is clean (0 rejects), so the rules were proven by running the oversampled files, which "
        "carry deliberately corrupted rows, through the same validation step. IoT is excluded here: it is streamed (see Streaming).").font = Font(name=F, size=9, italic=True)
ws.cell(row=17, column=2).alignment = Alignment(wrap_text=True)
ws.row_dimensions[4].height = 30; ws.row_dimensions[17].height = 40
ws.column_dimensions["A"].width = 8; ws.column_dimensions["B"].width = 52
for col in "CDEF":
    ws.column_dimensions[col].width = 20
rej = pg("SELECT reject_id, source_key, batch_label, record_key, reason FROM staging.rejects ORDER BY reject_id")
wr = wb.create_sheet("Rejects_Log")
title(wr, "staging.rejects — every row rejected by validation", "Raw log (feeds the Validation_Rules formulas)")
rej.columns = ["Reject ID", "Source", "Batch", "Record key", "Reason"]
write_table(wr, rej, 3, name="RejectsLog")
# headers on row 3 -> COUNTIFS over full columns works (header text never matches 'R1*' etc. except 'Reason'? no: 'Reason' does not start with R1..R8)

# ============================================================== 6 Oversampling
ws = wb.create_sheet("Oversampling")
recs = []
base_rows = {k: len(pd.read_csv(config.RAW_DIR / s.file, usecols=[0])) for k, s in config.SOURCES.items()}
for rid, r in config.REFRESHES.items():
    st = json.loads((config.OVERSAMPLED_DIR / r["label"] / "generation_stats.json").read_text())
    for f in st["files"]:
        recs.append([r["label"], f["source_key"], f["file"], f["window_start"], f["window_end"], f["rows_generated"],
                     f["malformed_injected"], f["template_rows"], round(f["growth_factor"], 4), base_rows[f["source_key"]]])
ov = pd.DataFrame(recs, columns=["Refresh", "Source", "File", "Window start", "Window end", "Rows generated", "Malformed injected",
                                 "Template rows (same qtr last yr)", "Growth factor g", "Base rows (2023-25)"])
ws["A1"] = "Oversampling — rows generated for each future refresh"; ws["A1"].font = Font(name=F, size=14, bold=True, color="1F3A5F")
ws["A2"] = "Rows = template rows × trailing-12-month growth g (clipped 0.90–1.25). Derived columns are formulas."; ws["A2"].font = Font(name=F, size=9, italic=True)
# write with header on row 1? Validation formulas reference columns A,B,G -> keep table at row 4 and columns as listed
last = write_table(ws, ov, 4, name="Oversampling", fmt={"Rows generated": "#,##0", "Template rows (same qtr last yr)": "#,##0",
                                                         "Base rows (2023-25)": "#,##0", "Growth factor g": "0.000"})
for j, h in enumerate(["Monthly rate vs base", "% of base volume", "Fault rate"], start=11):
    c = ws.cell(row=4, column=j, value=h); c.font = Font(name=F, bold=True, color="FFFFFF"); c.fill = HDR; c.border = BOX
    ws.column_dimensions[get_column_letter(j)].width = 20
for i in range(5, last + 1):
    ws.cell(row=i, column=11, value=f"=IFERROR((F{i}/3)/(J{i}/36),0)").number_format = "0.00x"
    ws.cell(row=i, column=12, value=f"=IFERROR(F{i}/J{i},0)").number_format = "0.0%"
    ws.cell(row=i, column=13, value=f"=IFERROR(G{i}/F{i},0)").number_format = "0.00%"
    for j in (11, 12, 13):
        ws.cell(row=i, column=j).font = Font(name=F, size=10); ws.cell(row=i, column=j).border = BOX
ws.cell(row=4, column=11).comment = Comment("Generated rows per month ÷ average base rows per month (36 months). ~1x means the new quarter "
                                            "has the same density as history, so time-series charts get no artificial spike.", "SDI")
r = last + 2
kpi(ws, r, "Total rows generated (both data sets)", f"=SUM(F5:F{last})")
kpi(ws, r + 1, "  of which set 1 (2026 Q1)", f'=SUMIFS(F5:F{last},A5:A{last},"refresh_01_2026Q1")')
kpi(ws, r + 2, "  of which set 2 (2026 Q2)", f'=SUMIFS(F5:F{last},A5:A{last},"refresh_02_2026Q2")')
kpi(ws, r + 3, "Total malformed rows injected", f"=SUM(G5:G{last})")
kpi(ws, r + 4, "Growth vs base dataset", f"=SUM(F5:F{last})/SUMIFS(J5:J{last},A5:A{last},\"refresh_01_2026Q1\")", "0.0%",
    "rows added by 2 new quarters relative to the 12 quarters of base history")
kpi(ws, r + 5, "Malformed rate setting", config.MALFORMED_RATE, "0.0%", "SDI_MALFORMED_RATE (input)")
ws.cell(row=r + 5, column=2).font = Font(name=F, color="0000FF", bold=True, size=11)

# ============================================================== 7 Batch ingestion
ws = wb.create_sheet("Batch_Ingestion")
bl = pg("""SELECT load_id, batch_label, stage, source_key, file_name, started_at, finished_at, round(duration_s::numeric,3)::float AS duration_s,
           rows_read, rows_loaded, rows_rejected, round(file_size_mb::numeric,2)::float AS file_size_mb, status, error_message
           FROM ops.batch_load_log WHERE batch_label IN ('base','fault_drill') ORDER BY load_id""")
bl["error_message"] = bl.error_message.str.slice(0, 120)
title(ws, "Batch ingestion metrics — base load", "ops.batch_load_log — Timestamp_end − Timestamp_start per file; rows = len(df); plus 1 deliberate fault-drill file")
kpi(ws, 4, "Batch success rate", '=COUNTIFS(M:M,"SUCCESS")/(COUNTA(M:M)-1)', "0.00%", "successful runs ÷ attempted runs (includes 1 deliberate fault drill)")
kpi(ws, 5, "Rows processed into ODS", '=SUMIFS(J:J,C:C,"ODS_LOAD")', "#,##0")
kpi(ws, 6, "Rows rejected at staging", '=SUMIFS(K:K,C:C,"STAGING_VALIDATE")', "#,##0")
kpi(ws, 7, "Slowest file load (s)", '=MAXIFS(H:H,C:C,"ODS_LOAD")'.replace("MAXIFS", "_xlfn.MAXIFS"), "0.00")
kpi(ws, 8, "Total ODS load time, all files (s)", '=SUMIFS(H:H,C:C,"ODS_LOAD")', "0.00")
last = write_table(ws, bl.rename(columns={"load_id": "Load ID", "batch_label": "Batch", "stage": "Stage", "source_key": "Source",
                                          "file_name": "File", "started_at": "Started (UTC)", "finished_at": "Finished (UTC)",
                                          "duration_s": "Duration (s)", "rows_read": "Rows read", "rows_loaded": "Rows loaded",
                                          "rows_rejected": "Rows rejected", "file_size_mb": "File MB", "status": "Status",
                                          "error_message": "Error"}), 10, name="BatchLog",
                   fmt={"Started (UTC)": "yyyy-mm-dd hh:mm:ss", "Finished (UTC)": "yyyy-mm-dd hh:mm:ss", "Rows read": "#,##0", "Rows loaded": "#,##0"})
# fix KPI formulas to exclude the KPI area: COUNTA over column M counts table header + rows; KPI rows have nothing in M.
ws["B4"] = f'=COUNTIFS(M11:M{last},"SUCCESS")/COUNTA(M11:M{last})'
ws["B5"] = f'=SUMIFS(J11:J{last},C11:C{last},"ODS_LOAD")'
ws["B6"] = f'=SUMIFS(K11:K{last},C11:C{last},"STAGING_VALIDATE")'
ws["B7"] = f'=_xlfn.MAXIFS(H11:H{last},C11:C{last},"ODS_LOAD")'
ws["B8"] = f'=SUMIFS(H11:H{last},C11:C{last},"ODS_LOAD")'
ws.column_dimensions["A"].width = 34
ch = BarChart(); ch.type = "bar"; ch.title = "Base load: ODS load duration per file (s)"; ch.style = 10
base_ods = [i for i, rr in enumerate(bl.itertuples(), start=11) if rr.batch_label == "base" and rr.stage == "ODS_LOAD"]
ch.add_data(Reference(ws, min_col=8, min_row=base_ods[0], max_row=base_ods[-1]), titles_from_data=False)
ch.set_categories(Reference(ws, min_col=4, min_row=base_ods[0], max_row=base_ods[-1]))
ch.legend = None; ch.roundedCorners = False; ch.x_axis.delete = False; ch.y_axis.delete = False
ch.height, ch.width = 7, 14
ws.add_chart(ch, "P2")

# ============================================================== 8 Streaming
ws = wb.create_sheet("Streaming")
sl = pg("""SELECT microbatch_id, stream_run_id, batch_label, received_at, committed_at, events_received, events_committed, events_malformed,
           round(duration_s::numeric,4)::float duration_s, round(throughput_eps::numeric,1)::float throughput_eps,
           round(latency_avg_s::numeric,4)::float latency_avg_s, round(latency_p95_s::numeric,4)::float latency_p95_s
           FROM ops.stream_batch_log ORDER BY microbatch_id""")
title(ws, "Streaming ingestion metrics", "ops.stream_batch_log — one row per 500-event micro-batch; test stream = the oversampled IoT files replayed through POST /ingest/stream/iot")
last = 10 + len(sl)
kpi(ws, 4, "Stream throughput (events/s)", f"=SUM(G11:G{last})/SUM(I11:I{last})", "#,##0", "Total records ingested ÷ total stream processing seconds")
kpi(ws, 5, "Avg end-to-end latency (s)", f"=AVERAGE(K11:K{last})", "0.000", "commit time − event emitted time")
kpi(ws, 6, "Max p95 latency (s)", f"=MAX(L11:L{last})", "0.000")
kpi(ws, 7, "Malformed / dropped events", f"=SUM(H11:H{last})", "#,##0")
kpi(ws, 8, "Drop rate", f"=SUM(H11:H{last})/SUM(F11:F{last})", "0.00%")
write_table(ws, sl.rename(columns={"microbatch_id": "Micro-batch", "stream_run_id": "Stream run", "batch_label": "Batch", "received_at": "Received",
                                   "committed_at": "Committed", "events_received": "Events in", "events_committed": "Committed events",
                                   "events_malformed": "Malformed", "duration_s": "Duration (s)", "throughput_eps": "Throughput (eps)",
                                   "latency_avg_s": "Latency avg (s)", "latency_p95_s": "Latency p95 (s)"}), 10, name="StreamLog",
            fmt={"Received": "hh:mm:ss.000", "Committed": "hh:mm:ss.000"})
ws.column_dimensions["A"].width = 32

# ============================================================== 9 Storage
ws = wb.create_sheet("Storage")
L = "captured_at = (SELECT max(captured_at) FROM ops.storage_snapshot WHERE batch_label='base')"
stg = pg(f"""SELECT store, layer, table_name, domain, row_count, round(size_mb::numeric,2)::float size_mb, min_date, max_date, distinct_months
             FROM ops.storage_snapshot WHERE {L} ORDER BY store DESC, layer, table_name""")
title(ws, "Storage & staging metrics (after the base load)", "ops.storage_snapshot — COUNT(*), pg_total_relation_size / os.path.getsize, MIN/MAX date")
last = 12 + len(stg)
kpi(ws, 4, "Rows in staging (all 7 domains)", f'=SUMIFS(E13:E{last},B13:B{last},"staging")')
kpi(ws, 5, "Rows in mart fact tables", f'=SUMIFS(E13:E{last},C13:C{last},"mart.fact*")')
kpi(ws, 6, "Postgres footprint (MB)", f'=SUMIFS(F13:F{last},A13:A{last},"postgres",B13:B{last},"database")', "#,##0.0")
kpi(ws, 7, "DuckDB mart file (MB)", f'=SUMIFS(F13:F{last},A13:A{last},"duckdb",B13:B{last},"database")', "#,##0.0")
kpi(ws, 8, "Pharmacy Sales share of staging rows", f'=SUMIFS(E13:E{last},B13:B{last},"staging",D13:D{last},"Pharmacy Sales")/B4', "0.0%")
kpi(ws, 9, "Earliest date covered", f'=MIN(G13:G{last})', "yyyy-mm-dd")
kpi(ws, 10, "Latest date covered", f'=MAX(H13:H{last})', "yyyy-mm-dd")
write_table(ws, stg.rename(columns={"store": "Store", "layer": "Layer", "table_name": "Table", "domain": "Domain", "row_count": "Rows",
                                    "size_mb": "Size MB", "min_date": "Min date", "max_date": "Max date", "distinct_months": "Months"}),
            12, name="StorageSnap", fmt={"Rows": "#,##0", "Min date": "yyyy-mm-dd", "Max date": "yyyy-mm-dd"})
c = ws.cell(row=12, column=10, value="Span (days)"); c.font = Font(name=F, bold=True, color="FFFFFF"); c.fill = HDR; c.border = BOX
for i in range(13, last + 1):
    ws.cell(row=i, column=10, value=f'=IF(G{i}="","",H{i}-G{i})').font = Font(name=F, size=10)
ws.column_dimensions["A"].width = 36; ws.column_dimensions["J"].width = 12
ws.freeze_panes = "A13"

# ============================================================== 10 Query performance
ws = wb.create_sheet("Query_Performance")
qp = pg("""SELECT batch_label, query_name, widget, dashboard, variant,
           round(percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms)::numeric,3)::float AS median_ms,
           round(max(duration_ms)::numeric,3)::float max_ms, max(rows_returned) rows_returned,
           count(*) executions, count(*) FILTER (WHERE status='FAILED') failed
           FROM ops.query_log WHERE batch_label='base' GROUP BY 1,2,3,4,5 ORDER BY 2,5,1""")
title(ws, "Analytical query layer — execution time per widget", "3 timed runs after a warm-up, after the base load · baseline = star join over facts · optimized = summary table")
last = 11 + len(qp)
kpi(ws, 4, "Query error rate", f"=SUM(J12:J{last})/SUM(I12:I{last})", "0.00%", "failed ÷ executed")
kpi(ws, 5, "Queries executed", f"=SUM(I12:I{last})", "#,##0", "active query count for this run")
kpi(ws, 6, "Median latency — baseline (ms)", f'=MEDIAN(IF(E12:E{last}="baseline",F12:F{last}))', "0.00", "array formula")
kpi(ws, 7, "Median latency — optimized (ms)", f'=MEDIAN(IF(E12:E{last}="optimized",F12:F{last}))', "0.00")
kpi(ws, 8, "Slowest single execution (ms)", f"=MAX(G12:G{last})", "0.00", "all sub-second")
from openpyxl.worksheet.formula import ArrayFormula
for cell, var in (("B6", "baseline"), ("B7", "optimized")):
    ws[cell] = ArrayFormula(cell, f'=MEDIAN(IF(E12:E{last}="{var}",F12:F{last}))')
write_table(ws, qp.rename(columns={"batch_label": "After load", "query_name": "Query", "widget": "Widget / KPI card", "dashboard": "Dashboard",
                                   "variant": "Variant", "median_ms": "Median ms", "max_ms": "Max ms", "rows_returned": "Rows",
                                   "executions": "Executions", "failed": "Failed"}), 11, name="QueryPerf",
            widths={"Widget / KPI card": 50, "Query": 36})
ws.column_dimensions["A"].width = 34
# speed-up table (formulas)
sp = qp[qp.variant == "optimized"].query_name.unique().tolist()
c0 = 13
ws.cell(row=3, column=c0, value="Speed-up from pre-aggregation").font = Font(name=F, bold=True)
for j, h in enumerate(["Query", "Avg baseline ms", "Avg optimized ms", "Speed-up ×"]):
    c = ws.cell(row=4, column=c0 + j, value=h); c.font = Font(name=F, bold=True, color="FFFFFF"); c.fill = HDR; c.border = BOX
for i, qn in enumerate(sp, start=5):
    ws.cell(row=i, column=c0, value=qn)
    ws.cell(row=i, column=c0 + 1, value=f'=AVERAGEIFS($F$12:$F${last},$B$12:$B${last},M{i},$E$12:$E${last},"baseline")').number_format = "0.00"
    ws.cell(row=i, column=c0 + 2, value=f'=AVERAGEIFS($F$12:$F${last},$B$12:$B${last},M{i},$E$12:$E${last},"optimized")').number_format = "0.00"
    ws.cell(row=i, column=c0 + 3, value=f"=IFERROR(N{i}/O{i},0)").number_format = "0.0\"×\""
    for j in range(4):
        ws.cell(row=i, column=c0 + j).font = Font(name=F, size=10); ws.cell(row=i, column=c0 + j).border = BOX
ws.column_dimensions["M"].width = 36
for col in "NOP":
    ws.column_dimensions[col].width = 16
ch = BarChart(); ch.type = "col"; ch.title = "Avg ms per widget: baseline vs optimized"; ch.style = 10
ch.add_data(Reference(ws, min_col=14, max_col=15, min_row=4, max_row=4 + len(sp)), titles_from_data=True)
ch.set_categories(Reference(ws, min_col=13, min_row=5, max_row=4 + len(sp)))
ch.roundedCorners = False; ch.x_axis.delete = False; ch.y_axis.delete = False; ch.legend.overlay = False
ch.height, ch.width = 8, 18
ws.add_chart(ch, "R3")

# ============================================================== 13 Data dictionary
ws = wb.create_sheet("Data_Dictionary")
dd = pd.read_csv(config.OVERSAMPLED_DIR / "data_dictionary.csv")
title(ws, "Data dictionary — source files and oversampled data", "How it was generated = treatment by pipeline/oversampling/generator.py")
dd.columns = ["File", "Source key", "Column", "SQL type", "Key", "Description", "Example", "Null % (generated)", "How it was generated"]
write_table(ws, dd, 4, name="DataDictionary", widths={"Description": 48, "How it was generated": 80, "Example": 26})

# ============================================================== 14 Assumptions
ws = wb.create_sheet("Assumptions")
title(ws, "Project assumptions")
assum = [
    "The 7 instructor CSVs are the 'operational source systems'; files dropped into data/landing simulate their nightly extracts.",
    "Each refresh = one new calendar quarter (2026Q1, 2026Q2) so that both refreshes cover different time periods, as required.",
    "Volume per refresh follows the data's own trailing-12-month growth, clipped to 0.90–1.25 to avoid runaway extrapolation.",
    "Sales spike days (82,764 sales on 2025-01-01 and 27,985 on 2024-01-01 vs a median of 288/day) are bulk-posting artefacts: kept in the DWH (they are valid rows) but excluded from oversampling templates.",
    "Hospital demand is a complete panel (12 hospitals × 63 medicines × month), so its row count is fixed at 2,268 per quarter.",
    "IoT telemetry is the streaming source; base IoT history is back-filled as a batch, new IoT arrives through the stream API.",
    "Retail vs Hospital vs E-commerce is derived: SalesChannel = Ecommerce → E-commerce; PharmacyType = Hospital Pharmacy → Hospital; else Retail.",
    "Prices are revised for 10% of SKUs per refresh (+3–8%) to exercise SCD Type 2; new sales use the new price.",
    "0.3% of generated rows are deliberately malformed to prove the validation layer works.",
    "Latency is measured from the moment the simulator emits a micro-batch to its DB commit (local deployment, so it is in milliseconds).",
    "All timestamps are handled in UTC.",
    "Milestone 1 stops at the query layer; refreshing the mart with the oversampled data and the BI dashboards are Milestone 2.",
]
for i, a in enumerate(assum, start=3):
    ws.cell(row=i, column=1, value=i - 2).font = Font(name=F, bold=True)
    c = ws.cell(row=i, column=2, value=a); c.font = Font(name=F, size=10); c.alignment = Alignment(wrap_text=True, vertical="top")
ws.column_dimensions["A"].width = 5; ws.column_dimensions["B"].width = 140

for s in wb.worksheets:
    s.sheet_view.showGridLines = False if s.title in ("README", "Assumptions", "Validation_Rules") else True
OUT.parent.mkdir(parents=True, exist_ok=True)
wb.save(OUT)
print("saved", OUT)
