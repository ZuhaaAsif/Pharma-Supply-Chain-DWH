"""
Central configuration for the SDI (Sales & Demand Intelligence) pipeline.

Everything that depends on the environment (paths, connection strings) is read
from environment variables so the same code runs inside docker-compose and on a
laptop. Everything that depends on the SOURCE DATA (file names, keys, date
columns) lives in SOURCES below - change it here if the files ever change.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# ----------------------------------------------------------------------------- paths
PROJECT_ROOT = Path(os.getenv("SDI_PROJECT_ROOT", Path(__file__).resolve().parents[1]))
DATA_DIR = Path(os.getenv("SDI_DATA_DIR", PROJECT_ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"                 # original instructor dataset (refresh 0)
LANDING_DIR = DATA_DIR / "landing"         # watched by the file watcher (bronze drop zone)
PROCESSED_DIR = DATA_DIR / "processed"     # archive of ingested files
STREAM_QUEUE_DIR = DATA_DIR / "stream_queue"  # IoT events waiting to be replayed as a stream
OVERSAMPLED_DIR = DATA_DIR / "oversampled"  # every generated refresh is also kept here
WAREHOUSE_DIR = DATA_DIR / "warehouse"     # DuckDB mart + parquet gold exports
DUCKDB_PATH = Path(os.getenv("SDI_DUCKDB_PATH", WAREHOUSE_DIR / "sdi_mart.duckdb"))

# ----------------------------------------------------------------------------- connections
PG_DSN = os.getenv("SDI_PG_DSN", "postgresql://pharma:pharma@localhost:5432/pharma_dwh")
INGEST_API_URL = os.getenv("SDI_INGEST_API_URL", "http://localhost:8000")
# "api" -> Airflow tasks call the FastAPI ingestion service over HTTP (default, as required)
# "inprocess" -> call the same functions directly (unit tests / no API running)
INGEST_MODE = os.getenv("SDI_INGEST_MODE", "api")

# ----------------------------------------------------------------------------- sources
@dataclass(frozen=True)
class Source:
    file: str                 # CSV file name
    table: str                # short table name used in ods./staging.
    domain: str               # business domain label for dashboards
    pk: tuple[str, ...]       # natural primary key
    event_date: str | None    # column that places a row in time (date coverage / refresh windows)
    in_sdi_mart: bool = False
    medicine_fk: bool = True  # has MedicineID -> validate against master
    positive_cols: tuple[str, ...] = field(default_factory=tuple)  # must be > 0
    date_cols: tuple[str, ...] = field(default_factory=tuple)      # must parse as date


SOURCES: dict[str, Source] = {
    "master": Source("01_master_data.csv", "master", "Master Data", ("MedicineID",), "EffectiveFromDate",
                     in_sdi_mart=True, medicine_fk=False,
                     positive_cols=("WholesalePriceUSD", "RecommendedRetailPriceUSD", "UnitManufacturingCostUSD"),
                     date_cols=("EffectiveFromDate",)),
    "manufacturing": Source("02_manufacturing_data.csv", "manufacturing", "Manufacturing", ("ManufacturingOrderID",),
                            "ActualCompletionDate", date_cols=("OrderDate", "ActualCompletionDate", "ExpiryDate")),
    "delivery": Source("03_manufacturer_delivery_data.csv", "delivery", "Manufacturer Delivery",
                       ("ShipmentID", "ShipmentLineNumber"), "DispatchDate",
                       positive_cols=("ShippedQuantityPacks",), date_cols=("DispatchDate", "ActualDeliveryDate")),
    "warehouse": Source("04_warehouse_data.csv", "warehouse", "Warehouse", ("InventorySnapshotID",), "SnapshotDate",
                        date_cols=("SnapshotDate", "BatchExpiryDate")),
    "sales": Source("05_pharmacy_sales_data.csv", "sales", "Pharmacy Sales", ("SalesTransactionID",), "SalesDate",
                    in_sdi_mart=True,
                    positive_cols=("UnitRetailPriceUSD", "QuantitySoldPacks", "GrossSalesAmountUSD"),
                    date_cols=("SalesDate",)),
    "hospital_demand": Source("06_hospital_demand_data.csv", "hospital_demand", "Hospital Demand",
                              ("HospitalDemandRecordID",), "DemandMonth", in_sdi_mart=True,
                              positive_cols=("ForecastDemandPacks",), date_cols=("DemandMonth",)),
    "iot": Source("07_iot_streaming_data.csv", "iot", "IoT Telemetry", ("SensorReadingID",), "EventTimestampUTC",
                  date_cols=("EventTimestampUTC",)),
}
FILE_TO_SOURCE = {s.file: k for k, s in SOURCES.items()}

# ----------------------------------------------------------------------------- refresh plan
# Base data covers 2023-01 .. 2025-12. Each refresh appends one new calendar quarter.
REFRESHES = {
    1: {"label": "refresh_01_2026Q1", "start": "2026-01-01", "end": "2026-03-31"},
    2: {"label": "refresh_02_2026Q2", "start": "2026-04-01", "end": "2026-06-30"},
}
MALFORMED_RATE = float(os.getenv("SDI_MALFORMED_RATE", "0.003"))  # injected bad rows to exercise validation
RANDOM_SEED = int(os.getenv("SDI_SEED", "42"))

# streaming simulation
STREAM_MICROBATCH_SIZE = int(os.getenv("SDI_STREAM_MICROBATCH", "500"))
STREAM_MAX_EVENTS = int(os.getenv("SDI_STREAM_MAX_EVENTS", "0"))  # 0 = replay whole queue file


def ensure_dirs() -> None:
    for d in (RAW_DIR, LANDING_DIR, PROCESSED_DIR, STREAM_QUEUE_DIR, OVERSAMPLED_DIR, WAREHOUSE_DIR):
        d.mkdir(parents=True, exist_ok=True)
