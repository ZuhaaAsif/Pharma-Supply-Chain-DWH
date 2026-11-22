"""
Oversampling layer - synthesises one new calendar quarter of data for all 7 source files.

Method: SEASONAL TEMPORAL BOOTSTRAP with growth scaling, lineage re-keying and rule re-derivation
------------------------------------------------------------------------------------------------
1. Template window  = the same calendar quarter one year earlier (keeps seasonality:
   2026Q1 is grown from 2025Q1, 2026Q2 from 2025Q2).
2. Volume           = template rows x YoY growth factor g, where g is measured from the data
   itself (rows in template quarter / rows in the same quarter the year before), clipped to
   [0.90, 1.25]. Hospital demand is a complete panel (12 hospitals x 63 medicines x month),
   so its volume is fixed and growth is applied to the quantities instead.
3. Bootstrap        = rows (or whole entities - shipments, warehouse lots) are resampled with
   replacement, which preserves the joint distribution of categorical attributes
   (channel x pharmacy x customer segment x payer ...), something independent column
   sampling or SMOTE-style interpolation would destroy.
4. Time shift       = every date/timestamp in a row is moved by the same offset (364 days =
   52 weeks, so weekday patterns survive). Intra-row intervals (sale -> expiry,
   dispatch -> delivery) are therefore preserved.
5. Jitter           = small multiplicative / additive noise on continuous measures so that new
   rows are not exact copies.
6. Re-keying        = new primary keys continue each file's own ID sequence; foreign keys are
   re-pointed to entities generated in the same refresh (sales -> new warehouse lots ->
   new shipments -> new production batches) so lineage stays end-to-end.
7. Re-derivation    = business rules observed in the base data are re-applied
   (Gross = Qty x RRP, COGS = Qty x Wholesale, Tax = 6% Net, Margin = Net - COGS,
   Closing = Opening + Received - Dispensed - Wastage, breach flags vs. limits ...).
8. SCD seed         = ~10% of SKUs receive a price revision effective at the window start,
   which exercises the Type-2 dimension in the mart; new sales use the new price.
9. Fault injection  = MALFORMED_RATE of rows are deliberately corrupted (null SKU, negative
   price, impossible date, duplicate key) so the validation layer has something to catch.
"""
from __future__ import annotations

import json
import logging
import os
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline import config
from pipeline.schemas import SOURCE_SCHEMAS

log = logging.getLogger(__name__)
SHIFT = pd.Timedelta(days=364)  # 52 weeks


# ----------------------------------------------------------------------------- helpers
def _date_cols(file: str) -> list[str]:
    return [c for c, t in SOURCE_SCHEMAS[file].items() if t == "date"]


def _ts_cols(file: str) -> list[str]:
    return [c for c, t in SOURCE_SCHEMAS[file].items() if t == "timestamptz"]


def _next_ids(existing: pd.Series, n: int) -> list[str]:
    """Continue an ID sequence like 'SALE-000500000' -> 'SALE-000500001'."""
    sample = str(existing.dropna().iloc[-1])
    m = re.match(r"^(.*?)(\d+)$", sample)
    prefix, width = m.group(1), len(m.group(2))
    mx = existing.dropna().astype(str).str.extract(r"(\d+)$")[0].astype("int64").max()
    return [f"{prefix}{i:0{width}d}" for i in range(mx + 1, mx + 1 + n)]


def _shift_dates(df: pd.DataFrame, file: str, offset: pd.Timedelta) -> pd.DataFrame:
    for c in _date_cols(file):
        if c in df:
            d = pd.to_datetime(df[c], errors="coerce") + offset
            df[c] = d.dt.strftime("%Y-%m-%d")
    for c in _ts_cols(file):
        if c in df:
            d = pd.to_datetime(df[c], errors="coerce", utc=True) + offset
            df[c] = d.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return df


def _in_window(s: pd.Series, start, end) -> pd.Series:
    d = pd.to_datetime(s.astype(str).str[:10], errors="coerce")
    return (d >= pd.Timestamp(start)) & (d <= pd.Timestamp(end))


def spike_days(dates: pd.Series, k: float = 10.0) -> set:
    """Calendar days whose row count exceeds k x the median daily count (bulk-posting artefacts,
    e.g. 82,764 sales stamped 2025-01-01 vs a median of ~288/day)."""
    d = dates.astype(str).str[:10]
    c = d.value_counts()
    return set(c[c > k * c.median()].index)


def _growth(dates: pd.Series, t_start: pd.Timestamp, t_end: pd.Timestamp, exclude: set | None = None) -> float:
    """Trailing-12-month YoY growth measured from the data itself.

    rows in the 12 months ending at the template window's end
    / rows in the 12 months before that, clipped to [0.90, 1.25].
    (Quarter-on-quarter ratios proved too noisy for this dataset.)"""
    ds = dates.astype(str).str[:10]
    if exclude:
        ds = ds[~ds.isin(exclude)]
    d = pd.to_datetime(ds, errors="coerce")
    end = t_end
    cur = ((d > end - pd.DateOffset(years=1)) & (d <= end)).sum()
    prev = ((d > end - pd.DateOffset(years=2)) & (d <= end - pd.DateOffset(years=1))).sum()
    if prev == 0:
        return 1.0
    return float(np.clip(cur / prev, 0.90, 1.25))


@dataclass
class GenStats:
    source_key: str
    file: str
    window_start: str
    window_end: str
    template_rows: int
    growth_factor: float
    rows_generated: int
    malformed_injected: int = 0
    malformed_breakdown: dict = field(default_factory=dict)
    notes: str = ""


# ----------------------------------------------------------------------------- engine
class OversamplingEngine:
    def __init__(self, refresh_id: int, raw_dir: Path | None = None, out_root: Path | None = None,
                 malformed_rate: float | None = None, seed: int | None = None):
        if refresh_id not in config.REFRESHES:
            raise ValueError(f"unknown refresh {refresh_id}; configured: {list(config.REFRESHES)}")
        self.refresh_id = refresh_id
        r = config.REFRESHES[refresh_id]
        self.label, self.start, self.end = r["label"], pd.Timestamp(r["start"]), pd.Timestamp(r["end"])
        self.t_start, self.t_end = self.start - SHIFT, self.end - SHIFT  # template window
        self.raw_dir = Path(raw_dir or config.RAW_DIR)
        self.out_root = Path(out_root or config.OVERSAMPLED_DIR)
        self.out_dir = self.out_root / self.label
        self.rate = config.MALFORMED_RATE if malformed_rate is None else malformed_rate
        self.rng = np.random.default_rng((seed or config.RANDOM_SEED) + refresh_id)
        self.stats: list[GenStats] = []
        self.history: dict[str, pd.DataFrame] = {}
        self.generated: dict[str, pd.DataFrame] = {}

    # -------------------------------------------------- history = base + earlier refreshes
    def _load_history(self, key: str) -> pd.DataFrame:
        if key in self.history:
            return self.history[key]
        f = config.SOURCES[key].file
        frames = [pd.read_csv(self.raw_dir / f, dtype=str, keep_default_na=False, na_values=[""])]
        for rid in sorted(config.REFRESHES):
            if rid >= self.refresh_id:
                break
            p = self.out_root / config.REFRESHES[rid]["label"] / f
            if p.exists():
                frames.append(pd.read_csv(p, dtype=str, keep_default_na=False, na_values=[""]))
            else:
                log.warning("refresh %s output missing for %s - IDs may collide", rid, f)
        df = pd.concat(frames, ignore_index=True)
        df = self._clean_history(key, df)
        self.history[key] = df
        return df

    @staticmethod
    def _clean_history(key: str, df: pd.DataFrame) -> pd.DataFrame:
        """History = what the validation layer would accept. Earlier refresh files still contain the
        deliberately injected faults, so the same rules (R1/R2/R4/R5/R7) are applied before use."""
        src = config.SOURCES[key]
        ok = pd.Series(True, index=df.index)
        for p in src.pk:
            ok &= df[p].notna()
        if "MedicineID" in df and key != "master":
            ok &= df["MedicineID"].notna()
        for c in src.positive_cols:
            ok &= pd.to_numeric(df[c], errors="coerce") > 0
        for c in src.date_cols:
            ok &= pd.to_datetime(df[c].astype(str).str[:10], errors="coerce", format="%Y-%m-%d").notna()
        df = df[ok]
        keys = list(src.pk) + (["EffectiveFromDate"] if key == "master" else [])
        return df.drop_duplicates(subset=keys, keep="first").reset_index(drop=True)

    def _num(self, s: pd.Series) -> pd.Series:
        return pd.to_numeric(s, errors="coerce")

    # -------------------------------------------------- 01 master (SCD-2 seed)
    def gen_master(self) -> pd.DataFrame:
        hist = self._load_history("master")
        cur = hist.drop_duplicates("MedicineID", keep="last").copy()
        n_change = max(1, int(round(len(cur) * 0.10)))
        idx = self.rng.choice(cur.index, n_change, replace=False)
        up = self.rng.uniform(1.03, 1.08, n_change)
        for col in ("WholesalePriceUSD", "RecommendedRetailPriceUSD"):
            cur.loc[idx, col] = (self._num(cur.loc[idx, col]) * up).round(2).astype(str)
        cur.loc[idx, "UnitManufacturingCostUSD"] = (self._num(cur.loc[idx, "UnitManufacturingCostUSD"]) *
                                                    self.rng.uniform(1.02, 1.06, n_change)).round(2).astype(str)
        cur.loc[idx, "EffectiveFromDate"] = self.start.strftime("%Y-%m-%d")
        self.price_changes = cur.loc[idx, "MedicineID"].tolist()
        self.master_now = cur.copy()
        self.stats.append(GenStats("master", config.SOURCES["master"].file, str(self.start.date()), str(self.end.date()),
                                   len(cur), 1.0, len(cur),
                                   notes=f"full snapshot; {n_change} SKUs re-priced (+3..8%) -> SCD2 versions: "
                                         + ", ".join(self.price_changes)))
        return cur

    # -------------------------------------------------- 02 manufacturing
    def gen_manufacturing(self) -> pd.DataFrame:
        key, f = "manufacturing", config.SOURCES["manufacturing"].file
        hist = self._load_history(key)
        dcol = config.SOURCES[key].event_date
        g = _growth(hist[dcol], self.t_start, self.t_end)
        tmpl = hist[_in_window(hist[dcol], self.t_start, self.t_end)]
        n = int(round(len(tmpl) * g))
        out = tmpl.sample(n, replace=True, random_state=self.rng.integers(1e9)).reset_index(drop=True)
        out = _shift_dates(out, f, SHIFT)
        yr = self.start.year
        prev_this_year = hist.ManufacturingOrderID.str.startswith(f"MO-{yr}-")
        seq0 = hist.loc[prev_this_year, "ManufacturingOrderID"].str[-5:].astype(int).max() if prev_this_year.any() else 0
        out["ManufacturingOrderID"] = [f"MO-{yr}-{seq0 + i + 1:05d}" for i in range(n)]
        bseq = hist.ProductionBatchID.str[-6:].astype(int).max()
        out["ProductionBatchID"] = [f"B{str(yr)[2:]}-{p}-{bseq + i + 1:06d}" for i, p in enumerate(out.PlantID)]
        q = self._num(out.PlannedQuantityPacks) * self.rng.normal(1.0, 0.02, n)
        rej = self._num(out.RejectedQuantityPacks) * self.rng.uniform(0.8, 1.2, n)
        failed = out.BatchStatus.eq("Rejected")
        out["ActualQuantityPacks"] = q.round().astype(int).astype(str)
        out["RejectedQuantityPacks"] = rej.round().astype(int).astype(str)
        rel = np.where(failed, 0, q.round() - rej.round()).astype(int)
        out["ReleasedQuantityPacks"] = rel.astype(str)
        out["YieldPct"] = np.where(failed, 0.0, (rel / q.round() * 100).round(1)).astype(str)
        for c in ("LaborCostUSD", "MaterialCostUSD", "OverheadCostUSD"):
            out[c] = (self._num(out[c]) * self.rng.normal(1.03, 0.02, n)).round(2)
        tot = out.LaborCostUSD + out.MaterialCostUSD + out.OverheadCostUSD
        out["TotalManufacturingCostUSD"] = tot.round(2)
        out["CostPerReleasedPackUSD"] = np.where(rel > 0, (tot / np.maximum(rel, 1)).round(4), np.nan)
        out["RecordCreatedTimestampUTC"] = (pd.to_datetime(out.BatchReleaseDate) + pd.Timedelta(hours=30)).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        self.stats.append(GenStats(key, f, str(self.start.date()), str(self.end.date()), len(tmpl), g, n))
        return out

    # -------------------------------------------------- 03 manufacturer delivery (sample whole shipments)
    def gen_delivery(self, mfg_new: pd.DataFrame) -> pd.DataFrame:
        key, f = "delivery", config.SOURCES["delivery"].file
        hist = self._load_history(key)
        g = _growth(hist.DispatchDate, self.t_start, self.t_end)
        tmpl = hist[_in_window(hist.DispatchDate, self.t_start, self.t_end)]
        ships = tmpl.ShipmentID.unique()
        n_ship = int(round(len(ships) * g))
        pick = self.rng.choice(ships, n_ship, replace=True)
        parts = []
        new_ship_ids = _next_ids(hist.ShipmentID, n_ship)
        sscc0 = self._num(hist.SSCC18).max()
        seal0 = hist.SealNumber.str[-8:].astype(int).max()
        for i, sid in enumerate(pick):
            p = tmpl[tmpl.ShipmentID == sid].copy()
            p["ShipmentID"] = new_ship_ids[i]
            p["SSCC18"] = f"{int(sscc0) + i + 1}"
            p["SealNumber"] = f"SEAL-{seal0 + i + 1:08d}"
            parts.append(p)
        out = pd.concat(parts, ignore_index=True)
        out = _shift_dates(out, f, SHIFT)
        # re-point each line to a NEW released batch of the same medicine (fallback: latest historic batch)
        rel_new = mfg_new[mfg_new.BatchStatus.eq("Released")]
        mfg_hist = self._load_history("manufacturing")
        rel_hist = mfg_hist[mfg_hist.BatchStatus.eq("Released")].sort_values("BatchReleaseDate")
        bmap_new = rel_new.groupby("MedicineID")[["ManufacturingOrderID", "ProductionBatchID"]].apply(lambda x: x.values.tolist()).to_dict()
        bmap_old = rel_hist.groupby("MedicineID")[["ManufacturingOrderID", "ProductionBatchID"]].apply(lambda x: x.values[-3:].tolist()).to_dict()
        mo, pb = [], []
        for med in out.MedicineID:
            pool = bmap_new.get(med) or bmap_old.get(med)
            choice = pool[self.rng.integers(len(pool))]
            mo.append(choice[0]); pb.append(choice[1])
        out["ManufacturingOrderID"], out["ProductionBatchID"] = mo, pb
        shipped = (self._num(out.ShippedQuantityPacks) * self.rng.normal(1.0, 0.03, len(out))).round().astype(int)
        dmg, short = self._num(out.DamagedQuantityPacks).astype(int), self._num(out.ShortQuantityPacks).astype(int)
        out["ShippedQuantityPacks"] = shipped
        out["ReceivedQuantityPacks"] = shipped - dmg - short
        out["FreightCostUSD"] = (self._num(out.FreightCostUSD) * self.rng.normal(1.04, 0.03, len(out))).round(1)
        out["FreightCostPerPackUSD"] = (out.FreightCostUSD / shipped).round(4)
        self.ship_map = dict(zip(pick, new_ship_ids))  # template shipment -> new shipment (used by IoT)
        self.stats.append(GenStats(key, f, str(self.start.date()), str(self.end.date()), len(tmpl), g, len(out),
                                   notes=f"{n_ship} shipments resampled as whole units (multi-line shipments preserved)"))
        return out

    # -------------------------------------------------- 04 warehouse (sample whole lot trajectories)
    def gen_warehouse(self, deliv_new: pd.DataFrame) -> pd.DataFrame:
        key, f = "warehouse", config.SOURCES["warehouse"].file
        hist = self._load_history(key)
        g = _growth(hist.SnapshotDate, self.t_start, self.t_end)
        win = _in_window(hist.SnapshotDate, self.t_start, self.t_end)
        tmpl = hist[win]
        target = int(round(len(tmpl) * g))
        lots = tmpl.WarehouseLotID.value_counts()
        lot_ids = lots.index.to_numpy()
        lot_seq0 = hist.WarehouseLotID.str[-7:].astype(int).max()
        groups = {k: v for k, v in tmpl.groupby("WarehouseLotID")}
        dnew = deliv_new.groupby(["MedicineID", "DestinationWarehouseID"])
        dnew_med = deliv_new.groupby("MedicineID")
        parts, total, i = [], 0, 0
        self.lot_map: dict[str, str] = {}
        while total < target:
            lid = lot_ids[self.rng.integers(len(lot_ids))]
            p = groups[lid].copy()
            new_lot = f"WHLOT-{lot_seq0 + i + 1:07d}"
            self.lot_map.setdefault(lid, new_lot)
            p["WarehouseLotID"] = new_lot
            med, wh = p.MedicineID.iloc[0], p.WarehouseID.iloc[0]
            try:
                src = dnew.get_group((med, wh))
            except KeyError:
                src = dnew_med.get_group(med) if med in dnew_med.groups else None
            if src is not None:
                r = src.iloc[self.rng.integers(len(src))]
                p["InboundShipmentID"], p["InboundShipmentLineNumber"] = r.ShipmentID, str(r.ShipmentLineNumber)
                p["ProductionBatchID"] = r.ProductionBatchID
            parts.append(p)
            total += len(p); i += 1
        out = pd.concat(parts, ignore_index=True).iloc[:target]
        out = _shift_dates(out, f, SHIFT)
        out["DaysToExpiry"] = (pd.to_datetime(out.BatchExpiryDate) - pd.to_datetime(out.SnapshotDate)).dt.days.clip(lower=0)
        jit = self.rng.normal(1.0, 0.02, len(out))
        for c in ("OpeningQuantityPacks", "AvailableQuantityPacks", "ClosingQuantityPacks"):
            out[c] = (self._num(out[c]) * jit).round().astype(int)
        out = out.sort_values(["SnapshotDate", "WarehouseLotID"]).reset_index(drop=True)
        out["InventorySnapshotID"] = _next_ids(hist.InventorySnapshotID, len(out))
        self.lot_lookup = out.drop_duplicates("WarehouseLotID")[["WarehouseLotID", "MedicineID", "WarehouseID",
                                                                 "ProductionBatchID", "BatchExpiryDate"]]
        self.stats.append(GenStats(key, f, str(self.start.date()), str(self.end.date()), len(tmpl), g, len(out),
                                   notes=f"{i} lot trajectories resampled; DaysToExpiry recomputed"))
        return out

    # -------------------------------------------------- 05 pharmacy sales
    def gen_sales(self) -> pd.DataFrame:
        key, f = "sales", config.SOURCES["sales"].file
        hist = self._load_history(key)
        spikes = spike_days(hist.SalesDate)
        self.sales_spikes = sorted(spikes)
        g = _growth(hist.SalesDate, self.t_start, self.t_end, exclude=spikes)
        tmpl = hist[_in_window(hist.SalesDate, self.t_start, self.t_end) & ~hist.SalesDate.isin(spikes)]
        n = int(round(len(tmpl) * g))
        out = tmpl.sample(n, replace=True, random_state=self.rng.integers(1e9)).reset_index(drop=True)
        out = _shift_dates(out, f, SHIFT)
        # keep inside window (364-day shift can leave 1-2 days at the edge)
        out = out[_in_window(out.SalesDate, self.start, self.end)].reset_index(drop=True)
        n = len(out)
        # re-point lineage to lots generated in this refresh (same medicine, prefer same warehouse)
        lk = self.lot_lookup
        by_mw = {k: v for k, v in lk.groupby(["MedicineID", "WarehouseID"])}
        by_m = {k: v for k, v in lk.groupby("MedicineID")}
        lot, pb, exp, wh = [], [], [], []
        for med, w in zip(out.MedicineID, out.SourceWarehouseID):
            pool = by_mw.get((med, w))
            if pool is None:
                pool = by_m.get(med)
            if pool is None:
                lot.append(None); pb.append(None); exp.append(None); wh.append(w); continue
            r = pool.iloc[self.rng.integers(len(pool))]
            lot.append(r.WarehouseLotID); pb.append(r.ProductionBatchID); exp.append(r.BatchExpiryDate); wh.append(r.WarehouseID)
        keep_old = pd.isna(pd.Series(lot))
        out["WarehouseLotID"] = np.where(keep_old, out.WarehouseLotID, lot)
        out["ProductionBatchID"] = np.where(keep_old, out.ProductionBatchID, pb)
        out["BatchExpiryDate"] = np.where(keep_old, out.BatchExpiryDate, exp)
        out["SourceWarehouseID"] = wh
        out["DaysToExpiryAtSale"] = (pd.to_datetime(out.BatchExpiryDate) - pd.to_datetime(out.SalesDate)).dt.days.clip(lower=1)
        # new pharmacy replenishment deliveries: one new ID per template delivery
        old_del = out.PharmacyDeliveryID.unique()
        new_del = _next_ids(hist.PharmacyDeliveryID, len(old_del))
        dmap = dict(zip(old_del, new_del))
        out["PharmacyDeliveryID"] = out.PharmacyDeliveryID.map(dmap)
        out["PharmacyOrderID"] = out.PharmacyDeliveryID.str.replace("PHD-", "PHO-", regex=False)
        # keys
        out = out.sort_values("SalesTimestampUTC").reset_index(drop=True)
        out["SalesTransactionID"] = _next_ids(hist.SalesTransactionID, n)
        has_rx = out.PrescriptionID.notna()
        out.loc[has_rx, "PrescriptionID"] = _next_ids(hist.PrescriptionID, int(has_rx.sum()))
        # price re-derivation with CURRENT master (SCD-2 price changes flow into sales)
        m = self.master_now.set_index("MedicineID")
        rrp = self._num(out.MedicineID.map(m.RecommendedRetailPriceUSD))
        whs = self._num(out.MedicineID.map(m.WholesalePriceUSD))
        qty = self._num(out.QuantitySoldPacks)
        disc_rate = (self._num(out.DiscountAmountUSD) / self._num(out.GrossSalesAmountUSD)).fillna(0)
        gross = (qty * rrp).round(2)
        disc = (gross * disc_rate).round(2)
        net = (gross - disc).round(2)
        cogs = (qty * whs).round(2)
        out["UnitRetailPriceUSD"], out["GrossSalesAmountUSD"], out["DiscountAmountUSD"] = rrp, gross, disc
        out["NetSalesAmountUSD"], out["SalesTaxUSD"] = net, (net * 0.06).round(2)
        out["CostOfGoodsSoldUSD"], out["GrossMarginUSD"] = cogs, (net - cogs).round(2)
        self.stats.append(GenStats(key, f, str(self.start.date()), str(self.end.date()), len(tmpl), g, n,
                                   notes="price/COGS re-derived from current master; lineage re-pointed to new lots; "
                                         f"anomalous spike days excluded from template: {self.sales_spikes}"))
        return out

    # -------------------------------------------------- 06 hospital demand (complete panel + stock roll-forward)
    def gen_hospital_demand(self) -> pd.DataFrame:
        key, f = "hospital_demand", config.SOURCES["hospital_demand"].file
        hist = self._load_history(key)
        hist_m = pd.to_datetime(hist.DemandMonth)
        # monthly grain -> calendar-aligned template (same 3 months last year), NOT the 52-week shift
        ts, te = self.start - pd.DateOffset(years=1), self.end - pd.DateOffset(years=1)
        tmpl = hist[(hist_m >= ts) & (hist_m <= te)].copy()
        fc = self._num(hist.ForecastDemandPacks)
        e = te
        cur = fc[(hist_m > e - pd.DateOffset(years=1)) & (hist_m <= e)].sum()
        prev = fc[(hist_m > e - pd.DateOffset(years=2)) & (hist_m <= e - pd.DateOffset(years=1))].sum()
        g = float(np.clip(cur / prev if prev else 1.0, 0.9, 1.25))
        out = tmpl.copy()
        out["DemandMonth"] = (pd.to_datetime(out.DemandMonth) + pd.DateOffset(years=1)).dt.strftime("%Y-%m-%d")
        out = out.sort_values(["HospitalID", "MedicineID", "DemandMonth"]).reset_index(drop=True)
        n = len(out)
        fac = g * self.rng.normal(1.0, 0.04, n)
        for c in ("EstimatedDiseaseCases", "DiseaseRelatedAdmissions", "OutpatientEncounters", "ForecastDemandPacks",
                  "RequestedQuantityPacks", "ApprovedQuantityPacks", "ReceivedQuantityPacks", "DispensedQuantityPacks",
                  "ConsumedQuantityPacks"):
            out[c] = (self._num(out[c]) * fac).round().astype(int)
        # stock roll-forward: opening = last closing for the same hospital x medicine
        last_close = (hist.assign(_m=hist_m).sort_values("_m").groupby(["HospitalID", "MedicineID"]).ClosingStockPacks.last())
        opening, closing, unf, sod = [], [], [], []
        prev_key, prev_close = None, None
        waste = self._num(out.WastageQuantityPacks).astype(int).to_numpy()
        recv, disp = out.ReceivedQuantityPacks.to_numpy(), out.DispensedQuantityPacks.to_numpy().copy()
        base_unf = self._num(out.UnfulfilledDemandPacks).astype(int).to_numpy()
        for i, k in enumerate(zip(out.HospitalID, out.MedicineID)):
            op = int(self._num(pd.Series([last_close.get(k, 0)])).iloc[0]) if k != prev_key else prev_close
            avail = op + recv[i] - waste[i]
            short = max(0, disp[i] - avail)
            disp[i] -= short
            cl = avail - disp[i]
            opening.append(op); closing.append(cl); unf.append(base_unf[i] + short)
            sod.append(0 if short == 0 else int(min(8, 1 + short // max(1, disp[i] // 30 + 1))))
            prev_key, prev_close = k, cl
        out["OpeningStockPacks"], out["DispensedQuantityPacks"], out["ClosingStockPacks"] = opening, disp, closing
        out["UnfulfilledDemandPacks"] = unf
        out["StockoutDays"] = np.maximum(self._num(out.StockoutDays).astype(int), sod)
        req = out.RequestedQuantityPacks.clip(lower=1)
        out["ServiceLevelPct"] = (100 * (1 - out.UnfulfilledDemandPacks / (out.DispensedQuantityPacks + out.UnfulfilledDemandPacks).clip(lower=1))).round(2)
        out["MedicationRequestStatus"] = np.where(out.UnfulfilledDemandPacks > 0, "Partially fulfilled", "Completed")
        out = out.sort_values(["DemandMonth", "HospitalID", "MedicineID"]).reset_index(drop=True)
        out["HospitalDemandRecordID"] = _next_ids(hist.HospitalDemandRecordID, n)
        self.stats.append(GenStats(key, f, str(self.start.date()), str(self.end.date()), len(tmpl), g, n,
                                   notes="complete 12x63xmonth panel; quantities x growth; stock rolled forward from last closing"))
        return out

    # -------------------------------------------------- 07 IoT telemetry (goes to the STREAM queue)
    def gen_iot(self) -> pd.DataFrame:
        key, f = "iot", config.SOURCES["iot"].file
        hist = self._load_history(key)
        g = _growth(hist.EventTimestampUTC, self.t_start, self.t_end)
        tmpl = hist[_in_window(hist.EventTimestampUTC, self.t_start, self.t_end)]
        n = int(round(len(tmpl) * g))
        out = tmpl.sample(n, replace=True, random_state=self.rng.integers(1e9)).reset_index(drop=True)
        out = _shift_dates(out, f, SHIFT)
        lat = self._num(out.DataLatencySeconds)
        out["IngestionTimestampUTC"] = (pd.to_datetime(out.EventTimestampUTC, utc=True) + pd.to_timedelta(lat, "s")).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        ship = out.ShipmentID.notna()
        out.loc[ship, "ShipmentID"] = out.loc[ship, "ShipmentID"].map(lambda s: self.ship_map.get(s, s))
        t = (self._num(out.TemperatureC) + self.rng.normal(0, 0.3, n)).round(2)
        h = (self._num(out.RelativeHumidityPct) + self.rng.normal(0, 0.8, n)).round(2)
        tmin, tmax, hmax = self._num(out.StorageMinTempC), self._num(out.StorageMaxTempC), self._num(out.MaxHumidityPct)
        tb, hb = (t < tmin) | (t > tmax), h > hmax
        dev = np.maximum(tmin - t, t - tmax)
        out["TemperatureC"], out["RelativeHumidityPct"] = t, h
        out["TemperatureBreachFlag"], out["HumidityBreachFlag"] = np.where(tb, "Y", "N"), np.where(hb, "Y", "N")
        out["ThresholdBreachFlag"] = np.where(tb | hb, "Y", "N")
        out["AlertSeverity"] = np.where(dev > 2, "Critical", np.where(tb | hb, "Warning", "Normal"))
        out = out.sort_values("EventTimestampUTC").reset_index(drop=True)
        out["SensorReadingID"] = _next_ids(hist.SensorReadingID, n)
        self.stats.append(GenStats(key, f, str(self.start.date()), str(self.end.date()), len(tmpl), g, n,
                                   notes="routed to stream_queue (replayed by the streaming DAG); breach flags recomputed"))
        return out

    # -------------------------------------------------- fault injection
    def inject_malformed(self, key: str, df: pd.DataFrame) -> pd.DataFrame:
        if key == "master" or self.rate <= 0 or len(df) < 50:
            return df
        src = config.SOURCES[key]
        k = max(1, int(len(df) * self.rate))
        idx = self.rng.choice(df.index, k, replace=False)
        kinds = self.rng.choice(["null_sku", "bad_number", "bad_date", "duplicate_pk"], k)
        df = df.copy()
        for c in df.columns:
            df[c] = df[c].astype(object)
        counts: dict[str, int] = {}
        for i, kind in zip(idx, kinds):
            if kind == "null_sku" and "MedicineID" in df:
                df.at[i, "MedicineID"] = None
            elif kind == "bad_number" and src.positive_cols:
                c = src.positive_cols[0]
                df.at[i, c] = -abs(float(df.at[i, c] or 1))
            elif kind == "bad_date" and src.date_cols:
                df.at[i, src.date_cols[0]] = "2026-13-45"
            elif kind == "duplicate_pk":
                j = df.index[0] if i != df.index[0] else df.index[1]
                for p in src.pk:
                    df.at[i, p] = df.at[j, p]
            else:
                continue
            counts[str(kind)] = counts.get(str(kind), 0) + 1
        st = next(s for s in self.stats if s.source_key == key)
        st.malformed_injected, st.malformed_breakdown = sum(counts.values()), counts
        return df

    # -------------------------------------------------- orchestration
    def run(self, publish: bool = True) -> list[GenStats]:
        config.ensure_dirs()
        self.out_dir.mkdir(parents=True, exist_ok=True)
        master = self.gen_master()
        mfg = self.gen_manufacturing()
        deliv = self.gen_delivery(mfg)
        wh = self.gen_warehouse(deliv)
        sales = self.gen_sales()
        hosp = self.gen_hospital_demand()
        iot = self.gen_iot()
        frames = {"master": master, "manufacturing": mfg, "delivery": deliv, "warehouse": wh,
                  "sales": sales, "hospital_demand": hosp, "iot": iot}
        for key, df in frames.items():
            cols = list(SOURCE_SCHEMAS[config.SOURCES[key].file])
            df = self.inject_malformed(key, df[cols])
            self.generated[key] = df
            path = self.out_dir / config.SOURCES[key].file
            df.to_csv(path, index=False)
            st = next(s for s in self.stats if s.source_key == key)
            st.output = str(path)
        self._write_stats()
        if publish:
            self.publish()
        return self.stats

    def publish(self) -> list[str]:
        """Atomically drop files into the landing zone (batch) and the stream queue (IoT)."""
        published = []
        for key, src in config.SOURCES.items():
            p = self.out_dir / src.file
            target_dir = config.STREAM_QUEUE_DIR if key == "iot" else config.LANDING_DIR
            target_dir.mkdir(parents=True, exist_ok=True)
            final = target_dir / f"{self.label}__{src.file}"
            tmp = final.with_suffix(".csv.part")
            shutil.copyfile(p, tmp)
            os.replace(tmp, final)  # atomic rename -> the watcher never sees half-written files
            published.append(str(final))
        return published

    def _write_stats(self):
        payload = {"refresh": self.label, "window": [str(self.start.date()), str(self.end.date())],
                   "template_window": [str(self.t_start.date()), str(self.t_end.date())],
                   "malformed_rate": self.rate, "price_changes": getattr(self, "price_changes", []),
                   "files": [s.__dict__ for s in self.stats]}
        (self.out_dir / "generation_stats.json").write_text(json.dumps(payload, indent=2, default=str))


def run_refresh(refresh_id: int, publish: bool = True) -> dict:
    eng = OversamplingEngine(refresh_id)
    stats = eng.run(publish=publish)
    try:  # best effort: record to ops.oversampling_log for the dashboards
        from pipeline.db import pg_insert_many
        pg_insert_many("ops.oversampling_log", [dict(
            refresh_label=eng.label, source_key=s.source_key, window_start=s.window_start, window_end=s.window_end,
            template_rows=s.template_rows, growth_factor=s.growth_factor, rows_generated=s.rows_generated,
            malformed_injected=s.malformed_injected, output_file=getattr(s, "output", "")) for s in stats])
    except Exception as e:  # pragma: no cover
        log.warning("could not write oversampling_log: %s", e)
    return {"refresh": eng.label, "rows": {s.source_key: s.rows_generated for s in stats}}


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO)
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", type=int, required=True)
    ap.add_argument("--no-publish", action="store_true")
    a = ap.parse_args()
    print(run_refresh(a.refresh, publish=not a.no_publish))
