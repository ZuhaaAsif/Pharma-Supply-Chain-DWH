"""
Staging (Postgres, silver) -> SDI data mart (DuckDB, gold).  INCREMENTAL: only staging rows with
_stg_id above the per-source watermark are pulled, so each refresh APPENDS to history
(the warehouse is never truncated).

Order: dim_date -> dim_medicine (SCD2) -> location -> hospital/pharmacy (SCD1) -> channel ->
junk dims -> fact_sales -> fact_hospital_demand -> aggregates (only months touched) ->
watermarks -> gold parquet export.
"""
from __future__ import annotations

import logging
import tempfile
import time
from pathlib import Path

from pipeline import config
from pipeline.db import duck, pg

log = logging.getLogger(__name__)
DDL = Path(__file__).resolve().parents[2] / "sql" / "02_mart_ddl.sql"

CENSUS = {**{s: "Northeast" for s in "CT ME MA NH RI VT NJ NY PA".split()},
          **{s: "Midwest" for s in "IL IN MI OH WI IA KS MN MO NE ND SD".split()},
          **{s: "South" for s in "DE FL GA MD NC SC VA DC WV AL KY MS TN AR LA OK TX".split()},
          **{s: "West" for s in "AZ CO ID MT NV NM UT WY AK CA HI OR WA".split()}}
SCD2_ATTRS = ("unitmanufacturingcostusd", "wholesalepriceusd", "recommendedretailpriceusd", "productstatus")


# --------------------------------------------------------------------------- setup
def init_mart(con) -> None:
    con.execute(DDL.read_text())
    if con.execute("SELECT count(*) FROM mart.dim_date").fetchone()[0] == 0:
        con.execute("""
            INSERT INTO mart.dim_date
            SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER), d::DATE, day(d), dayname(d), isodow(d), isodow(d) >= 6,
                   weekofyear(d), month(d), monthname(d), date_trunc('month', d)::DATE, quarter(d),
                   'Q' || quarter(d) || '-' || year(d), year(d), strftime(d, '%Y-%m')
            FROM range(DATE '2022-01-01', DATE '2028-01-01', INTERVAL 1 DAY) t(d)""")
        con.execute("INSERT INTO mart.dim_date VALUES (-1, NULL, NULL, 'Unknown', NULL, NULL, NULL, NULL, 'Unknown', NULL, NULL, 'Unknown', NULL, 'Unknown')")
    unknowns = {
        "dim_location": "(-1, 'Unknown', 'NA', 'NA', 'Unknown')",
        "dim_pharmacy": "(-1, 'UNKNOWN', NULL, 'Unknown', 'Unknown', NULL, -1, NULL)",
        "dim_hospital": "(-1, 'NONE', NULL, 'Not a hospital sale', 'N/A', NULL, NULL, NULL, NULL, NULL, NULL, -1, 'N/A', NULL)",
        "dim_channel": "(-1, 'Unknown', 'Unknown', 'Unknown', NULL)",
        "dim_sales_profile": "(-1, 'Unknown', 'Unknown', 'Unknown', 'U', 'NONE', 'Unknown', 'U', 'Unknown', 'UNKNOWN')",
        "dim_demand_profile": "(-1, 'Unknown', 'Unknown', 'UNKNOWN')",
    }
    for t, v in unknowns.items():
        if con.execute(f"SELECT count(*) FROM mart.{t} WHERE {t.replace('dim_', '')}_sk = -1").fetchone()[0] == 0:
            con.execute(f"INSERT INTO mart.{t} VALUES {v}")
    if con.execute("SELECT count(*) FROM mart.dim_medicine WHERE medicine_sk = -1").fetchone()[0] == 0:
        con.execute("""INSERT INTO mart.dim_medicine (medicine_sk, medicine_id, generic_name, disease_category,
                       therapeutic_class, effective_from, effective_to, is_current, version)
                       VALUES (-1, 'UNKNOWN', 'Unknown', 'Unknown', 'Unknown', DATE '1900-01-01', DATE '9999-12-31', TRUE, 0)""")
    con.execute("CREATE OR REPLACE MACRO mart.census(st) AS CASE " +
                " ".join(f"WHEN st = '{k}' THEN '{v}'" for k, v in CENSUS.items()) + " ELSE 'Other' END")


def _watermark(con, key: str) -> int:
    r = con.execute("SELECT last_stg_id FROM mart.etl_watermark WHERE source_key = ?", [key]).fetchone()
    return r[0] if r else 0


def _pull(con, key: str, tmpdir: str) -> tuple[int, int]:
    """Copy new staging rows (Postgres) into a DuckDB temp table inc_<key>. Returns (rows, max_stg_id)."""
    wm = _watermark(con, key)
    path = Path(tmpdir) / f"{key}.csv"
    with pg() as c, c.cursor() as cur, open(path, "w") as fh:
        cur.copy_expert(f"COPY (SELECT * FROM staging.stg_{config.SOURCES[key].table} WHERE _stg_id > {wm} "
                        f"ORDER BY _stg_id) TO STDOUT WITH CSV HEADER", fh)
    con.execute(f"CREATE OR REPLACE TEMP TABLE inc_{key} AS SELECT * FROM read_csv('{path}', header=true, "
                f"sample_size=-1, all_varchar=false)")
    n, mx = con.execute(f"SELECT count(*), coalesce(max(_stg_id), {wm}) FROM inc_{key}").fetchone()
    return n, mx


# --------------------------------------------------------------------------- dimensions
def load_dim_medicine(con) -> dict:
    """SCD Type 2 on price/cost/status; Type 1 overwrite on descriptive attributes."""
    stats = {"new_skus": 0, "new_versions": 0, "type1_updates": 0}
    if con.execute("SELECT count(*) FROM inc_master").fetchone()[0] == 0:
        return stats
    hash_expr = "md5(concat_ws('|', " + ", ".join(f"CAST({a} AS VARCHAR)" for a in SCD2_ATTRS) + "))"
    con.execute(f"CREATE OR REPLACE TEMP TABLE m_in AS SELECT *, {hash_expr} AS scd_hash FROM inc_master")
    for (eff,) in con.execute("SELECT DISTINCT effectivefromdate FROM m_in ORDER BY 1").fetchall():
        con.execute("CREATE OR REPLACE TEMP TABLE m_v AS SELECT * FROM m_in WHERE effectivefromdate = ?", [eff])
        # Type-1: descriptive attributes overwrite ALL versions
        stats["type1_updates"] += con.execute("""
            UPDATE mart.dim_medicine d SET generic_name = v.genericname, brand_name = v.fictionalbrandname,
                   therapeutic_class = v.therapeuticclass, dosage_form = v.dosageform, strength = v.strength
            FROM m_v v WHERE d.medicine_id = v.medicineid
              AND (d.generic_name IS DISTINCT FROM v.genericname OR d.brand_name IS DISTINCT FROM v.fictionalbrandname)
        """).fetchone()[0]
        changed = con.execute("""SELECT v.medicineid FROM m_v v JOIN mart.dim_medicine d
                                 ON d.medicine_id = v.medicineid AND d.is_current AND d.scd_hash <> v.scd_hash""").fetchall()
        if changed:  # expire current version
            con.execute("""UPDATE mart.dim_medicine d SET effective_to = v.effectivefromdate - INTERVAL 1 DAY, is_current = FALSE
                           FROM m_v v WHERE d.medicine_id = v.medicineid AND d.is_current AND d.scd_hash <> v.scd_hash""")
        stats["new_versions"] += len(changed)
        new_rows = con.execute(f"""
            INSERT INTO mart.dim_medicine
            SELECT nextval('mart.seq_medicine'), v.medicineid, v.internalsku, CAST(v.gtin14 AS VARCHAR), v.genericname,
                   v.fictionalbrandname, v.diseasecategory, v.icd10category, v.atccode, v.therapeuticclass, v.dosageform,
                   v.strength, v.routeofadministration, v.packsize, v.prescriptionclass, v.controlledmedicineflag,
                   v.coldchainflag, v.storagemintempc, v.storagemaxtempc, v.shelflifemonths, v.primarysupplierid,
                   v.primaryplantid, v.unitmanufacturingcostusd, v.wholesalepriceusd, v.recommendedretailpriceusd,
                   v.productstatus, v.scd_hash,
                   CASE WHEN prev.medicine_id IS NULL THEN DATE '1900-01-01' ELSE v.effectivefromdate END,
                   DATE '9999-12-31', TRUE, coalesce(prev.max_version, 0) + 1, v._batch_label
            FROM m_v v
            LEFT JOIN (SELECT medicine_id, max(version) AS max_version, bool_or(is_current) AS has_current
                       FROM mart.dim_medicine GROUP BY 1) prev ON prev.medicine_id = v.medicineid
            WHERE prev.medicine_id IS NULL OR NOT prev.has_current
        """).fetchone()[0]
        stats["new_skus"] += new_rows - len(changed)
    return stats


def load_dim_location_pharmacy_hospital(con) -> None:
    con.execute("""
        INSERT INTO mart.dim_location
        SELECT nextval('mart.seq_location'), city, state, cc, mart.census(state) FROM (
            SELECT DISTINCT pharmacycity AS city, pharmacystate AS state, countrycode AS cc FROM inc_sales
            UNION SELECT DISTINCT city, state, countrycode FROM inc_hospital_demand) n
        WHERE NOT EXISTS (SELECT 1 FROM mart.dim_location l WHERE l.city = n.city AND l.state = n.state)""")
    # dim_hospital  (SCD1 upsert from the latest month seen)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE h_in AS
        SELECT * FROM inc_hospital_demand QUALIFY row_number() OVER (PARTITION BY hospitalid ORDER BY demandmonth DESC) = 1""")
    con.execute("""
        UPDATE mart.dim_hospital d SET hospital_name = h.hospitalname, hospital_type = h.hospitaltype,
               licensed_beds = h.licensedbeds, icu_beds = h.icubeds, avg_occupancy_pct = h.averageoccupancyratepct,
               trauma_level = coalesce(h.traumalevel, 'None'), catchment_population = h.catchmentpopulation,
               updated_batch = h._batch_label
        FROM h_in h WHERE d.hospital_id = h.hospitalid""")
    con.execute("""
        INSERT INTO mart.dim_hospital
        SELECT nextval('mart.seq_hospital'), h.hospitalid, CAST(h.hospitalgln AS VARCHAR), h.hospitalname, h.hospitaltype,
               h.hospitalpharmacyid, h.licensedbeds, h.icubeds, h.averageoccupancyratepct, coalesce(h.traumalevel, 'None'),
               h.catchmentpopulation, coalesce(l.location_sk, -1),
               CASE WHEN h.licensedbeds >= 700 THEN 'Large (700+)' WHEN h.licensedbeds >= 450 THEN 'Medium (450-699)'
                    ELSE 'Small (<450)' END, h._batch_label
        FROM h_in h LEFT JOIN mart.dim_location l ON l.city = h.city AND l.state = h.state
        WHERE NOT EXISTS (SELECT 1 FROM mart.dim_hospital d WHERE d.hospital_id = h.hospitalid)""")
    # dim_pharmacy (SCD1)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE p_in AS
        SELECT * FROM inc_sales QUALIFY row_number() OVER (PARTITION BY pharmacyid ORDER BY salestimestamputc DESC) = 1""")
    con.execute("""
        UPDATE mart.dim_pharmacy d SET pharmacy_name = p.pharmacyname, pharmacy_type = p.pharmacytype,
               updated_batch = p._batch_label FROM p_in p WHERE d.pharmacy_id = p.pharmacyid""")
    con.execute("""
        INSERT INTO mart.dim_pharmacy
        SELECT nextval('mart.seq_pharmacy'), p.pharmacyid, CAST(p.pharmacygln AS VARCHAR), p.pharmacyname, p.pharmacytype,
               p.hospitalid, coalesce(l.location_sk, -1), p._batch_label
        FROM p_in p LEFT JOIN mart.dim_location l ON l.city = p.pharmacycity AND l.state = p.pharmacystate
        WHERE NOT EXISTS (SELECT 1 FROM mart.dim_pharmacy d WHERE d.pharmacy_id = p.pharmacyid)""")


def load_channel_and_junk(con) -> None:
    con.execute("""
        INSERT INTO mart.dim_channel
        SELECT nextval('mart.seq_channel'), saleschannel, pharmacytype,
               CASE WHEN saleschannel = 'Ecommerce' THEN 'E-commerce'
                    WHEN pharmacytype = 'Hospital Pharmacy' THEN 'Hospital' ELSE 'Retail' END, src
        FROM (SELECT DISTINCT saleschannel, pharmacytype, sourcesystem AS src FROM inc_sales) n
        WHERE NOT EXISTS (SELECT 1 FROM mart.dim_channel c WHERE c.sales_channel = n.saleschannel AND c.pharmacy_type = n.pharmacytype)""")
    con.execute("""
        INSERT INTO mart.dim_sales_profile
        SELECT nextval('mart.seq_sales_profile'), customersegment, payertype, paymentmethod, prescriptionrequiredflag,
               promo, ecommercedeliverystatus, returnflag, salestatus, h
        FROM (SELECT DISTINCT customersegment, payertype, paymentmethod, prescriptionrequiredflag,
                     coalesce(promotioncode, 'NONE') AS promo, ecommercedeliverystatus, returnflag, salestatus,
                     md5(concat_ws('|', customersegment, payertype, paymentmethod, prescriptionrequiredflag,
                         coalesce(promotioncode, 'NONE'), ecommercedeliverystatus, returnflag, salestatus)) AS h
              FROM inc_sales) n
        WHERE NOT EXISTS (SELECT 1 FROM mart.dim_sales_profile p WHERE p.profile_hash = n.h)""")
    con.execute("""
        INSERT INTO mart.dim_demand_profile
        SELECT nextval('mart.seq_demand_profile'), medicationrequeststatus, formularystatus, h
        FROM (SELECT DISTINCT medicationrequeststatus, formularystatus,
                     md5(concat_ws('|', medicationrequeststatus, formularystatus)) AS h FROM inc_hospital_demand) n
        WHERE NOT EXISTS (SELECT 1 FROM mart.dim_demand_profile p WHERE p.profile_hash = n.h)""")


# --------------------------------------------------------------------------- facts
def load_fact_sales(con) -> int:
    return con.execute("""
        INSERT INTO mart.fact_sales
        SELECT s.salestransactionid,
               CAST(strftime(s.salesdate, '%Y%m%d') AS INTEGER),
               coalesce(CAST(strftime(s.pharmacydeliverydate, '%Y%m%d') AS INTEGER), -1),
               coalesce(CAST(strftime(s.batchexpirydate, '%Y%m%d') AS INTEGER), -1),
               coalesce(m.medicine_sk, -1), coalesce(p.pharmacy_sk, -1), coalesce(p.location_sk, -1),
               coalesce(h.hospital_sk, -1), coalesce(c.channel_sk, -1), coalesce(sp.sales_profile_sk, -1),
               s.productionbatchid, s.warehouselotid, s.sourcewarehouseid, s.pharmacydeliveryid, s.prescriptionid,
               s.quantitysoldpacks, s.unitretailpriceusd, s.grosssalesamountusd, s.discountamountusd,
               s.netsalesamountusd, s.salestaxusd, s.costofgoodssoldusd, s.grossmarginusd,
               s.returnedquantitypacks, s.daystoexpiryatsale, s._batch_label, current_timestamp
        FROM inc_sales s
        LEFT JOIN mart.dim_medicine m ON m.medicine_id = s.medicineid
             AND s.salesdate BETWEEN m.effective_from AND m.effective_to          -- SCD2 point-in-time lookup
        LEFT JOIN mart.dim_pharmacy p ON p.pharmacy_id = s.pharmacyid
        LEFT JOIN mart.dim_hospital h ON h.hospital_id = s.hospitalid
        LEFT JOIN mart.dim_channel c ON c.sales_channel = s.saleschannel AND c.pharmacy_type = s.pharmacytype
        LEFT JOIN mart.dim_sales_profile sp ON sp.profile_hash = md5(concat_ws('|', s.customersegment, s.payertype,
             s.paymentmethod, s.prescriptionrequiredflag, coalesce(s.promotioncode, 'NONE'), s.ecommercedeliverystatus,
             s.returnflag, s.salestatus))
        ORDER BY s.salesdate, s.salestransactionid                                 -- clustered by date -> zone maps prune
    """).fetchone()[0]


def load_fact_hospital_demand(con) -> int:
    return con.execute("""
        INSERT INTO mart.fact_hospital_demand
        SELECT d.hospitaldemandrecordid, CAST(strftime(d.demandmonth, '%Y%m%d') AS INTEGER),
               coalesce(h.hospital_sk, -1), coalesce(m.medicine_sk, -1), coalesce(h.location_sk, -1),
               coalesce(dp.demand_profile_sk, -1),
               d.estimateddiseasecases, d.diseaserelatedadmissions, d.outpatientencounters, d.forecastdemandpacks,
               d.requestedquantitypacks, d.approvedquantitypacks, d.receivedquantitypacks, d.emergencyrequestquantitypacks,
               d.openingstockpacks, d.dispensedquantitypacks, d.consumedquantitypacks, d.wastagequantitypacks,
               d.closingstockpacks, d.unfulfilleddemandpacks, d.stockoutdays, d.servicelevelpct, d.averageleadtimedays,
               d.demandtrendindex, d._batch_label, current_timestamp
        FROM inc_hospital_demand d
        LEFT JOIN mart.dim_hospital h ON h.hospital_id = d.hospitalid
        LEFT JOIN mart.dim_medicine m ON m.medicine_id = d.medicineid AND d.demandmonth BETWEEN m.effective_from AND m.effective_to
        LEFT JOIN mart.dim_demand_profile dp ON dp.profile_hash = md5(concat_ws('|', d.medicationrequeststatus, d.formularystatus))
        ORDER BY d.demandmonth
    """).fetchone()[0]


# --------------------------------------------------------------------------- aggregates (incremental by month)
def refresh_aggregates(con) -> dict:
    con.execute("CREATE OR REPLACE TEMP TABLE touched_s AS SELECT DISTINCT strftime(salesdate, '%Y-%m') ym FROM inc_sales")
    con.execute("CREATE OR REPLACE TEMP TABLE touched_h AS SELECT DISTINCT strftime(demandmonth, '%Y-%m') ym FROM inc_hospital_demand")
    con.execute("DELETE FROM mart.agg_sales_monthly WHERE year_month IN (SELECT ym FROM touched_s)")
    s = con.execute("""
        INSERT INTO mart.agg_sales_monthly
        SELECT dd.year_month, dd.month_start, m.medicine_id, m.generic_name, m.disease_category, m.therapeutic_class,
               c.channel_group, c.pharmacy_type, l.census_region,
               count(*), sum(f.quantity_packs), sum(f.gross_sales_usd), sum(f.discount_usd), sum(f.net_sales_usd),
               sum(f.cogs_usd), sum(f.gross_margin_usd), sum(f.returned_packs)
        FROM mart.fact_sales f
        JOIN mart.dim_date dd ON dd.date_key = f.sale_date_key
        JOIN mart.dim_medicine m ON m.medicine_sk = f.medicine_sk
        JOIN mart.dim_channel c ON c.channel_sk = f.channel_sk
        JOIN mart.dim_location l ON l.location_sk = f.location_sk
        WHERE dd.year_month IN (SELECT ym FROM touched_s)
        GROUP BY ALL""").fetchone()[0]
    con.execute("DELETE FROM mart.agg_hospital_monthly WHERE year_month IN (SELECT ym FROM touched_h)")
    h = con.execute("""
        INSERT INTO mart.agg_hospital_monthly
        SELECT dd.year_month, dd.month_start, ho.hospital_id, ho.hospital_name, ho.hospital_type, l.census_region,
               m.medicine_id, m.generic_name, m.disease_category,
               sum(forecast_packs), sum(consumed_packs), sum(requested_packs), sum(received_packs),
               sum(unfulfilled_packs), sum(stockout_days), sum(emergency_request_packs), sum(wastage_packs)
        FROM mart.fact_hospital_demand f
        JOIN mart.dim_date dd ON dd.date_key = f.month_date_key
        JOIN mart.dim_hospital ho ON ho.hospital_sk = f.hospital_sk
        JOIN mart.dim_location l ON l.location_sk = f.location_sk
        JOIN mart.dim_medicine m ON m.medicine_sk = f.medicine_sk
        WHERE dd.year_month IN (SELECT ym FROM touched_h)
        GROUP BY ALL""").fetchone()[0]
    return {"agg_sales_rows": s, "agg_hospital_rows": h}


def export_gold_parquet(con) -> None:
    gold = config.WAREHOUSE_DIR / "gold"
    gold.mkdir(parents=True, exist_ok=True)
    con.execute(f"""COPY (SELECT f.*, sale_date_key // 10000 AS year, (sale_date_key // 100) % 100 AS month
                          FROM mart.fact_sales f)
                    TO '{gold / 'fact_sales'}' (FORMAT parquet, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE)""")
    con.execute(f"COPY mart.agg_sales_monthly TO '{gold / 'agg_sales_monthly.parquet'}' (FORMAT parquet)")


# --------------------------------------------------------------------------- entry point
def load_mart(export_parquet: bool = True) -> dict:
    t0 = time.perf_counter()
    out: dict = {}
    with duck() as con, tempfile.TemporaryDirectory() as tmp:
        init_mart(con)
        marks = {}
        for key in ("master", "sales", "hospital_demand"):
            n, mx = _pull(con, key, tmp)
            out[f"in_{key}"], marks[key] = n, mx
        labels = ",".join(r[0] for r in con.execute(
            "SELECT DISTINCT _batch_label FROM inc_sales UNION SELECT DISTINCT _batch_label FROM inc_hospital_demand "
            "UNION SELECT DISTINCT _batch_label FROM inc_master").fetchall() if r[0])
        con.execute("BEGIN TRANSACTION")
        try:
            out["dim_medicine"] = load_dim_medicine(con)
            load_dim_location_pharmacy_hospital(con)
            load_channel_and_junk(con)
            out["fact_sales_rows"] = load_fact_sales(con)
            out["fact_hospital_rows"] = load_fact_hospital_demand(con)
            out.update(refresh_aggregates(con))
            for k, mx in marks.items():
                con.execute("INSERT OR REPLACE INTO mart.etl_watermark VALUES (?, ?, current_timestamp)", [k, mx])
            con.execute("INSERT INTO mart.etl_load_log VALUES (current_timestamp, 'load_mart', ?, ?, ?, ?)",
                        [labels, out["in_sales"] + out["in_hospital_demand"],
                         out["fact_sales_rows"] + out["fact_hospital_rows"], time.perf_counter() - t0])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        if export_parquet and out["fact_sales_rows"]:
            export_gold_parquet(con)
        con.execute("CHECKPOINT")
    out["duration_s"] = round(time.perf_counter() - t0, 2)
    out["batch_labels"] = labels
    log.info("mart load: %s", out)
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(load_mart())
