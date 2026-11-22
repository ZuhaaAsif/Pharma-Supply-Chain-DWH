"""
Builds the data dictionary for the oversampled data folder:
  data/oversampled/DATA_DICTIONARY.md   (human readable)
  data/oversampled/data_dictionary.csv  (also loaded into the Excel workbook)
For every column: type (frozen contract), meaning, example, null % in the generated files, and HOW the
oversampling engine produced it.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0, str(ROOT))
from pipeline import config  # noqa: E402
from pipeline.schemas import SOURCE_SCHEMAS  # noqa: E402

OV = ROOT / "data" / "oversampled"
REFRESHES = [r["label"] for r in config.REFRESHES.values()]

DESC = {
    # identifiers
    "MedicineID": "Medicine SKU identifier (conformed key to master data)", "InternalSKU": "Internal ERP SKU code",
    "GTIN14": "GS1 Global Trade Item Number of the pack", "ManufacturingOrderID": "Manufacturing order identifier",
    "ProductionBatchID": "Production batch identifier (traceability key)", "ShipmentID": "Manufacturer-to-warehouse shipment identifier",
    "ShipmentLineNumber": "Line number inside a shipment", "SSCC18": "GS1 Serial Shipping Container Code",
    "InventorySnapshotID": "Daily inventory snapshot row identifier", "WarehouseLotID": "Warehouse lot identifier (batch received into a warehouse)",
    "InboundShipmentID": "Shipment that delivered the lot", "InboundShipmentLineNumber": "Line of the inbound shipment",
    "SalesTransactionID": "Sales transaction identifier", "PharmacyID": "Selling pharmacy identifier", "HospitalID": "Hospital identifier",
    "HospitalPharmacyID": "Pharmacy attached to the hospital", "PharmacyOrderID": "Pharmacy replenishment order",
    "PharmacyDeliveryID": "Pharmacy replenishment delivery", "PrescriptionID": "Prescription reference (null for OTC sales)",
    "HospitalDemandRecordID": "Hospital x medicine x month record identifier", "SensorReadingID": "IoT reading identifier",
    "DeviceID": "IoT device identifier", "WarehouseID": "Regional warehouse identifier", "SourceWarehouseID": "Warehouse that supplied the sold lot",
    "DestinationWarehouseID": "Receiving warehouse", "PlantID": "Manufacturing plant", "OriginPlantID": "Shipping plant",
    "PrimaryPlantID": "Default manufacturing plant", "PrimarySupplierID": "Main API/material supplier", "CarrierID": "Logistics carrier",
    "StorageZoneID": "Warehouse storage zone", "SealNumber": "Trailer/container seal number", "PromotionCode": "Promotion applied (null = none)",
    # descriptive
    "GenericName": "International non-proprietary name", "FictionalBrandName": "Brand name (fictional)", "DiseaseCategory": "One of 7 disease categories",
    "ICD10Category": "ICD-10 chapter/range of the disease category", "ATCCode": "WHO ATC classification code", "TherapeuticClass": "Pharmacological class",
    "DosageForm": "Dosage form", "Strength": "Strength per unit", "RouteOfAdministration": "Route of administration", "PackSize": "Units per pack",
    "BaseUnitOfMeasure": "Selling unit (pack / device)", "PrescriptionClass": "Prescription or OTC", "ControlledMedicineFlag": "Y = controlled substance",
    "ColdChainFlag": "Y = requires 2-8 C cold chain", "ProductStatus": "Active / inactive (SCD-2 tracked)", "EffectiveFromDate": "Date this master version became valid (SCD-2)",
    "SourceSystem": "Originating operational system", "PlantName": "Plant name", "OriginPlantName": "Shipping plant name", "OriginPlantGLN": "GS1 location number of plant",
    "DestinationWarehouseName": "Receiving warehouse name", "DestinationWarehouseGLN": "GS1 location number of warehouse",
    "DestinationCity": "Warehouse city", "DestinationState": "Warehouse state", "DestinationCountryCode": "Country code",
    "CarrierName": "Carrier name", "TransportMode": "Road / Refrigerated road / Air", "WarehouseName": "Warehouse name", "WarehouseGLN": "GS1 location number",
    "StorageCondition": "Required storage band", "InventoryStatus": "Available / Near expiry / Quarantined / Depleted / Expired",
    "WarehouseActivityStatus": "Main activity of the lot that day", "SalesChannel": "Physical Pharmacy or Ecommerce",
    "PharmacyGLN": "GS1 location number of pharmacy", "PharmacyName": "Pharmacy name", "PharmacyType": "Chain / Independent / Hospital Pharmacy",
    "PharmacyCity": "Pharmacy city", "PharmacyState": "Pharmacy state", "CountryCode": "Country code", "CustomerSegment": "Customer segment",
    "PayerType": "Who pays (cash, insurance, government, formulary)", "PaymentMethod": "Payment instrument", "EcommerceDeliveryStatus": "E-commerce fulfilment outcome",
    "SaleStatus": "Completed / Partially returned", "HospitalGLN": "GS1 location number of hospital", "HospitalName": "Hospital name",
    "HospitalType": "Academic / General / Community / Specialty", "City": "City", "State": "State", "TraumaLevel": "Trauma centre level (null = none)",
    "MedicationRequestStatus": "Completed / Partially fulfilled", "FormularyStatus": "Formulary / Restricted / Non-formulary exception",
    "DeviceType": "Logger type", "AssetType": "WarehouseZone or Shipment", "AlertSeverity": "Normal / Warning / Critical",
    "ReadingQuality": "Valid / Suspect", "SourceStream": "MQTT topic", "DemandSignalSource": "What triggered the manufacturing order",
    "PlanningRunType": "MRP run type", "ManufacturingPriority": "Normal / High / Urgent", "BatchStatus": "Released / Rejected",
    "QualityTestStatus": "Passed / Conditional pass / Failed", "DelayReason": "Reason for production delay (null = on time)",
    "DeviationFlag": "Y = GMP deviation raised", "DeviationCategory": "Deviation type (null = none)", "ShipmentStatus": "Delivery status",
    "DeliveryExceptionReason": "Exception reason (null = none)", "EPCISEventType": "GS1 EPCIS event type", "DiseaseCategoryName": "Disease category",
}
PATTERNS = [
    (r"Date$|^DemandMonth$", "Calendar date"), (r"TimestampUTC$", "UTC timestamp"), (r"Flag$", "Y/N indicator"),
    (r"USD$", "Amount in US dollars"), (r"Pct$", "Percentage"), (r"Packs$", "Quantity in packs"), (r"Days$", "Number of days"),
    (r"Hours$", "Number of hours"), (r"TempC$|^TemperatureC$", "Temperature in degrees Celsius"), (r"Miles$", "Distance in miles"),
]

DERIVED = {
    "manufacturing": {"ActualQuantityPacks", "RejectedQuantityPacks", "ReleasedQuantityPacks", "YieldPct", "LaborCostUSD", "MaterialCostUSD",
                      "OverheadCostUSD", "TotalManufacturingCostUSD", "CostPerReleasedPackUSD", "RecordCreatedTimestampUTC"},
    "delivery": {"ShippedQuantityPacks", "ReceivedQuantityPacks", "FreightCostUSD", "FreightCostPerPackUSD"},
    "warehouse": {"DaysToExpiry", "OpeningQuantityPacks", "AvailableQuantityPacks", "ClosingQuantityPacks"},
    "sales": {"UnitRetailPriceUSD", "GrossSalesAmountUSD", "DiscountAmountUSD", "NetSalesAmountUSD", "SalesTaxUSD", "CostOfGoodsSoldUSD",
              "GrossMarginUSD", "DaysToExpiryAtSale"},
    "hospital_demand": {"EstimatedDiseaseCases", "DiseaseRelatedAdmissions", "OutpatientEncounters", "ForecastDemandPacks", "RequestedQuantityPacks",
                        "ApprovedQuantityPacks", "ReceivedQuantityPacks", "DispensedQuantityPacks", "ConsumedQuantityPacks", "OpeningStockPacks",
                        "ClosingStockPacks", "UnfulfilledDemandPacks", "StockoutDays", "ServiceLevelPct", "MedicationRequestStatus"},
    "iot": {"TemperatureC", "RelativeHumidityPct", "TemperatureBreachFlag", "HumidityBreachFlag", "ThresholdBreachFlag", "AlertSeverity",
            "IngestionTimestampUTC"},
    "master": {"WholesalePriceUSD", "RecommendedRetailPriceUSD", "UnitManufacturingCostUSD", "EffectiveFromDate"},
}
REKEY = {
    "manufacturing": {"ManufacturingOrderID", "ProductionBatchID"},
    "delivery": {"ShipmentID", "SSCC18", "SealNumber", "ManufacturingOrderID", "ProductionBatchID"},
    "warehouse": {"InventorySnapshotID", "WarehouseLotID", "InboundShipmentID", "InboundShipmentLineNumber", "ProductionBatchID"},
    "sales": {"SalesTransactionID", "PrescriptionID", "PharmacyDeliveryID", "PharmacyOrderID", "WarehouseLotID", "ProductionBatchID",
              "BatchExpiryDate", "SourceWarehouseID"},
    "hospital_demand": {"HospitalDemandRecordID"}, "iot": {"SensorReadingID", "ShipmentID"},
}
RULES = {
    "sales": {"UnitRetailPriceUSD": "= current master RecommendedRetailPriceUSD (SCD-2 price flows through)",
              "GrossSalesAmountUSD": "= Qty x UnitRetailPrice", "DiscountAmountUSD": "= Gross x template discount rate",
              "NetSalesAmountUSD": "= Gross - Discount", "SalesTaxUSD": "= 6% x Net", "CostOfGoodsSoldUSD": "= Qty x current WholesalePrice",
              "GrossMarginUSD": "= Net - COGS", "DaysToExpiryAtSale": "= BatchExpiryDate - SalesDate"},
    "hospital_demand": {"OpeningStockPacks": "= previous month ClosingStock (roll-forward)",
                        "ClosingStockPacks": "= Opening + Received - Dispensed - Wastage",
                        "UnfulfilledDemandPacks": "template value + any shortfall when stock runs out",
                        "ServiceLevelPct": "= 100 x (1 - Unfulfilled / (Dispensed + Unfulfilled))"},
    "warehouse": {"DaysToExpiry": "= BatchExpiryDate - SnapshotDate"},
    "manufacturing": {"ReleasedQuantityPacks": "= Actual - Rejected (0 if batch rejected)", "YieldPct": "= Released / Actual x 100",
                      "TotalManufacturingCostUSD": "= Labor + Material + Overhead"},
    "delivery": {"ReceivedQuantityPacks": "= Shipped - Damaged - Short", "FreightCostPerPackUSD": "= Freight / Shipped"},
    "iot": {"TemperatureBreachFlag": "= Temperature outside [StorageMinTempC, StorageMaxTempC]", "HumidityBreachFlag": "= Humidity > MaxHumidityPct",
            "AlertSeverity": "Critical if deviation > 2 C, Warning if any breach", "IngestionTimestampUTC": "= Event + DataLatencySeconds"},
}


def describe(col: str) -> str:
    if col in DESC:
        return DESC[col]
    for pat, d in PATTERNS:
        if re.search(pat, col):
            words = re.sub(r"(?<!^)(?=[A-Z][a-z])", " ", col)
            return f"{d}: {words}"
    return re.sub(r"(?<!^)(?=[A-Z][a-z])", " ", col)


def treatment(key: str, col: str, typ: str) -> str:
    if key in RULES and col in RULES[key]:
        return "re-derived: " + RULES[key][col]
    if col in REKEY.get(key, set()):
        return "re-keyed: new ID continuing the sequence / re-pointed to an entity generated in the same refresh"
    if col in DERIVED.get(key, set()):
        if key == "master":
            return "SCD-2 seed: +3..8% for 6 re-priced SKUs; EffectiveFromDate = refresh start"
        if key == "hospital_demand":
            return "scaled by YoY growth x N(1, 0.04) noise"
        return "jittered (small multiplicative/additive noise), then constrained by business rules"
    if key == "hospital_demand" and col == "DemandMonth":
        return "shifted +12 months (calendar-aligned quarter)"
    if typ in ("date", "timestamptz"):
        return "shifted +364 days (52 weeks, keeps weekday pattern); intra-row intervals preserved"
    if key == "master":
        return "copied from current master snapshot"
    return "bootstrapped from the template row (joint distribution preserved)"


def main():
    rows = []
    for key, src in config.SOURCES.items():
        frames = [pd.read_csv(OV / r / src.file, dtype=str) for r in REFRESHES if (OV / r / src.file).exists()]
        gen = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        for col, typ in SOURCE_SCHEMAS[src.file].items():
            ex = gen[col].dropna().iloc[0] if len(gen) and gen[col].notna().any() else ""
            rows.append(dict(file=src.file, source_key=key, column=col, sql_type=typ,
                             is_key="PK" if col in src.pk else ("FK" if col.endswith("ID") or col in ("GTIN14",) else ""),
                             description=describe(col), example=str(ex)[:40],
                             null_pct=round(100 * gen[col].isna().mean(), 2) if len(gen) else None,
                             generation=treatment(key, col, typ)))
    dd = pd.DataFrame(rows)
    dd.to_csv(OV / "data_dictionary.csv", index=False)
    stats = {r: json.loads((OV / r / "generation_stats.json").read_text()) for r in REFRESHES}
    md = ["# Oversampled data - data dictionary", "",
          "Two refreshes were generated by `pipeline/oversampling/generator.py` and orchestrated by the `sdi_oversampling` Airflow DAG.",
          "Each refresh appends one calendar quarter to the 2023-2025 base dataset; nothing in the base is modified.", "",
          "| Refresh | Window | Template window (same quarter last year) | SKUs re-priced (SCD-2) |", "|---|---|---|---|"]
    for r, s in stats.items():
        md.append(f"| `{r}` | {s['window'][0]} → {s['window'][1]} | {s['template_window'][0]} → {s['template_window'][1]} | {', '.join(s['price_changes'])} |")
    md += ["", "## Rows generated per file", "", "| File | " + " | ".join(REFRESHES) + " | Growth factor (R1 / R2) | Malformed injected (R1 / R2) |",
           "|---|" + "---|" * (len(REFRESHES) + 2)]
    for key, src in config.SOURCES.items():
        f = [next(x for x in stats[r]["files"] if x["source_key"] == key) for r in REFRESHES]
        md.append(f"| `{src.file}` | " + " | ".join(f"{x['rows_generated']:,}" for x in f) +
                  f" | {f[0]['growth_factor']:.3f} / {f[1]['growth_factor']:.3f} | {f[0]['malformed_injected']} / {f[1]['malformed_injected']} |")
    md += ["", "Malformed rows are deliberate (null SKU, negative price/quantity, impossible date `2026-13-45`, duplicate key) and are",
           "caught by the staging validation rules R2/R4/R5/R7. IoT files are replayed through the streaming API instead of the landing zone.",
           "", "## Column dictionary", ""]
    for key, src in config.SOURCES.items():
        md += [f"### `{src.file}`  ({src.domain}; PK: {', '.join(src.pk)})", "", "| Column | Type | Key | Description | Example | Null % | How it was generated |",
               "|---|---|---|---|---|---|---|"]
        for r in dd[dd.source_key == key].itertuples():
            md.append(f"| {r.column} | {r.sql_type} | {r.is_key} | {r.description} | `{r.example}` | {r.null_pct} | {r.generation} |")
        md.append("")
    (OV / "DATA_DICTIONARY.md").write_text("\n".join(md))
    print(f"{len(dd)} columns documented")


if __name__ == "__main__":
    main()
