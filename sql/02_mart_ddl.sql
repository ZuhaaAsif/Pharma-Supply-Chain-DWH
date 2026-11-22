-- =====================================================================
-- SDI data mart (DuckDB) - Sales & Demand Intelligence
-- Two fact tables sharing conformed dimensions (mini fact constellation):
--   fact_sales            transaction grain         (transactional fact)
--   fact_hospital_demand  hospital x medicine x month (periodic snapshot fact)
-- DWH techniques used
--   SCD Type 2     : dim_medicine (prices / cost / status are versioned)
--   SCD Type 1     : dim_pharmacy, dim_hospital, dim_location (overwrite)
--   Role-playing   : dim_date as sale date, pharmacy-delivery date, batch-expiry date, demand month
--   Junk dims      : dim_sales_profile, dim_demand_profile (low-cardinality flags/codes)
--   Degenerate dims: sales_transaction_id, production_batch_id, warehouse_lot_id, prescription_id
--   Unknown member : surrogate key -1 in every dimension (no NULL FKs in facts)
--   Aggregates     : agg_sales_monthly, agg_hospital_monthly (pre-aggregated summary tables)
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SEQUENCE IF NOT EXISTS mart.seq_medicine START 1;
CREATE SEQUENCE IF NOT EXISTS mart.seq_location START 1;
CREATE SEQUENCE IF NOT EXISTS mart.seq_pharmacy START 1;
CREATE SEQUENCE IF NOT EXISTS mart.seq_hospital START 1;
CREATE SEQUENCE IF NOT EXISTS mart.seq_channel START 1;
CREATE SEQUENCE IF NOT EXISTS mart.seq_sales_profile START 1;
CREATE SEQUENCE IF NOT EXISTS mart.seq_demand_profile START 1;

CREATE TABLE IF NOT EXISTS mart.dim_date (
    date_key INTEGER PRIMARY KEY, full_date DATE, day_of_month TINYINT, day_name VARCHAR, day_of_week TINYINT,
    is_weekend BOOLEAN, iso_week TINYINT, month_num TINYINT, month_name VARCHAR, month_start DATE,
    quarter_num TINYINT, quarter_label VARCHAR, year_num SMALLINT, year_month VARCHAR);

CREATE TABLE IF NOT EXISTS mart.dim_medicine (
    medicine_sk INTEGER PRIMARY KEY, medicine_id VARCHAR, internal_sku VARCHAR, gtin14 VARCHAR,
    generic_name VARCHAR, brand_name VARCHAR, disease_category VARCHAR, icd10_category VARCHAR, atc_code VARCHAR,
    therapeutic_class VARCHAR, dosage_form VARCHAR, strength VARCHAR, route VARCHAR, pack_size INTEGER,
    prescription_class VARCHAR, controlled_flag VARCHAR, cold_chain_flag VARCHAR, storage_min_temp_c DOUBLE,
    storage_max_temp_c DOUBLE, shelf_life_months INTEGER, primary_supplier_id VARCHAR, primary_plant_id VARCHAR,
    -- SCD-2 tracked attributes
    unit_mfg_cost_usd DOUBLE, wholesale_price_usd DOUBLE, retail_price_usd DOUBLE, product_status VARCHAR,
    scd_hash VARCHAR, effective_from DATE, effective_to DATE, is_current BOOLEAN, version INTEGER,
    loaded_batch VARCHAR);

CREATE TABLE IF NOT EXISTS mart.dim_location (
    location_sk INTEGER PRIMARY KEY, city VARCHAR, state VARCHAR, country_code VARCHAR, census_region VARCHAR);

CREATE TABLE IF NOT EXISTS mart.dim_pharmacy (
    pharmacy_sk INTEGER PRIMARY KEY, pharmacy_id VARCHAR UNIQUE, pharmacy_gln VARCHAR, pharmacy_name VARCHAR,
    pharmacy_type VARCHAR, hospital_id VARCHAR, location_sk INTEGER, updated_batch VARCHAR);

CREATE TABLE IF NOT EXISTS mart.dim_hospital (
    hospital_sk INTEGER PRIMARY KEY, hospital_id VARCHAR UNIQUE, hospital_gln VARCHAR, hospital_name VARCHAR,
    hospital_type VARCHAR, hospital_pharmacy_id VARCHAR, licensed_beds INTEGER, icu_beds INTEGER,
    avg_occupancy_pct DOUBLE, trauma_level VARCHAR, catchment_population BIGINT, location_sk INTEGER,
    bed_size_band VARCHAR, updated_batch VARCHAR);

CREATE TABLE IF NOT EXISTS mart.dim_channel (
    channel_sk INTEGER PRIMARY KEY, sales_channel VARCHAR, pharmacy_type VARCHAR, channel_group VARCHAR,
    source_system VARCHAR);

CREATE TABLE IF NOT EXISTS mart.dim_sales_profile (          -- junk dimension
    sales_profile_sk INTEGER PRIMARY KEY, customer_segment VARCHAR, payer_type VARCHAR, payment_method VARCHAR,
    prescription_required VARCHAR, promotion_code VARCHAR, ecommerce_delivery_status VARCHAR,
    return_flag VARCHAR, sale_status VARCHAR, profile_hash VARCHAR UNIQUE);

CREATE TABLE IF NOT EXISTS mart.dim_demand_profile (         -- junk dimension
    demand_profile_sk INTEGER PRIMARY KEY, medication_request_status VARCHAR, formulary_status VARCHAR,
    profile_hash VARCHAR UNIQUE);

CREATE TABLE IF NOT EXISTS mart.fact_sales (
    sales_transaction_id VARCHAR,                 -- degenerate dimension
    sale_date_key INTEGER, delivery_date_key INTEGER, expiry_date_key INTEGER,   -- role-playing dim_date
    medicine_sk INTEGER, pharmacy_sk INTEGER, location_sk INTEGER, hospital_sk INTEGER, channel_sk INTEGER,
    sales_profile_sk INTEGER,
    production_batch_id VARCHAR, warehouse_lot_id VARCHAR, source_warehouse_id VARCHAR,
    pharmacy_delivery_id VARCHAR, prescription_id VARCHAR,                        -- degenerate dims
    quantity_packs INTEGER, unit_price_usd DOUBLE, gross_sales_usd DOUBLE, discount_usd DOUBLE,
    net_sales_usd DOUBLE, sales_tax_usd DOUBLE, cogs_usd DOUBLE, gross_margin_usd DOUBLE,
    returned_packs INTEGER, days_to_expiry_at_sale INTEGER,
    batch_label VARCHAR, loaded_at TIMESTAMP DEFAULT current_timestamp);

CREATE TABLE IF NOT EXISTS mart.fact_hospital_demand (
    hospital_demand_record_id VARCHAR,            -- degenerate
    month_date_key INTEGER, hospital_sk INTEGER, medicine_sk INTEGER, location_sk INTEGER, demand_profile_sk INTEGER,
    estimated_disease_cases INTEGER, disease_admissions INTEGER, outpatient_encounters INTEGER,
    forecast_packs INTEGER, requested_packs INTEGER, approved_packs INTEGER, received_packs INTEGER,
    emergency_request_packs INTEGER, opening_stock_packs INTEGER, dispensed_packs INTEGER, consumed_packs INTEGER,
    wastage_packs INTEGER, closing_stock_packs INTEGER,           -- semi-additive (stock levels)
    unfulfilled_packs INTEGER, stockout_days INTEGER,
    service_level_pct DOUBLE, avg_lead_time_days DOUBLE, demand_trend_index DOUBLE,  -- non-additive
    batch_label VARCHAR, loaded_at TIMESTAMP DEFAULT current_timestamp);

CREATE TABLE IF NOT EXISTS mart.agg_sales_monthly (
    year_month VARCHAR, month_start DATE, medicine_id VARCHAR, generic_name VARCHAR, disease_category VARCHAR,
    therapeutic_class VARCHAR, channel_group VARCHAR, pharmacy_type VARCHAR, census_region VARCHAR,
    txn_count BIGINT, quantity_packs BIGINT, gross_sales_usd DOUBLE, discount_usd DOUBLE, net_sales_usd DOUBLE,
    cogs_usd DOUBLE, gross_margin_usd DOUBLE, returned_packs BIGINT);

CREATE TABLE IF NOT EXISTS mart.agg_hospital_monthly (
    year_month VARCHAR, month_start DATE, hospital_id VARCHAR, hospital_name VARCHAR, hospital_type VARCHAR,
    census_region VARCHAR, medicine_id VARCHAR, generic_name VARCHAR, disease_category VARCHAR,
    forecast_packs BIGINT, consumed_packs BIGINT, requested_packs BIGINT, received_packs BIGINT,
    unfulfilled_packs BIGINT, stockout_days BIGINT, emergency_request_packs BIGINT, wastage_packs BIGINT);

CREATE TABLE IF NOT EXISTS mart.etl_watermark (source_key VARCHAR PRIMARY KEY, last_stg_id BIGINT, updated_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS mart.etl_load_log (
    loaded_at TIMESTAMP DEFAULT current_timestamp, step VARCHAR, batch_labels VARCHAR, rows_in BIGINT,
    rows_out BIGINT, duration_s DOUBLE);
