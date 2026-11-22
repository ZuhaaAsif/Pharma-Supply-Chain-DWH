"""Light test-suite:  pytest -q   (DB-dependent tests skip automatically when Postgres/DuckDB are absent)."""
from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from pipeline import config
from pipeline.oversampling import generator as g

RAW = config.RAW_DIR
needs_raw = pytest.mark.skipif(not (RAW / "05_pharmacy_sales_data.csv").exists(), reason="base dataset not present")


def test_next_ids_continue_sequence():
    s = pd.Series(["SALE-000000001", "SALE-000500000"])
    assert g._next_ids(s, 2) == ["SALE-000500001", "SALE-000500002"]


def test_spike_days_detects_bulk_posting():
    d = pd.Series(["2025-01-01"] * 5000 + [f"2025-02-{i:02d}" for i in range(1, 29)] * 10)
    assert g.spike_days(d) == {"2025-01-01"}


def test_growth_is_clipped():
    dates = pd.Series(["2024-06-01"] * 10 + ["2025-06-01"] * 100)
    assert g._growth(dates, pd.Timestamp("2025-04-01"), pd.Timestamp("2025-07-01")) == 1.25


@needs_raw
def test_refresh_generation_rules(tmp_path):
    eng = g.OversamplingEngine(1, out_root=tmp_path, malformed_rate=0.0)
    eng.run(publish=False)
    sales = eng.generated["sales"]
    for c in ("QuantitySoldPacks", "UnitRetailPriceUSD", "GrossSalesAmountUSD", "DiscountAmountUSD",
              "NetSalesAmountUSD", "CostOfGoodsSoldUSD", "GrossMarginUSD"):
        sales[c] = pd.to_numeric(sales[c])
    assert np.allclose(sales.GrossSalesAmountUSD, (sales.QuantitySoldPacks * sales.UnitRetailPriceUSD).round(2), atol=0.011)
    assert np.allclose(sales.GrossMarginUSD, sales.NetSalesAmountUSD - sales.CostOfGoodsSoldUSD, atol=0.011)
    d = pd.to_datetime(sales.SalesDate)
    assert d.min() >= pd.Timestamp("2026-01-01") and d.max() <= pd.Timestamp("2026-03-31")
    hosp = eng.generated["hospital_demand"]
    assert len(hosp) == 12 * 63 * 3
    assert sorted(hosp.DemandMonth.unique()) == ["2026-01-01", "2026-02-01", "2026-03-01"]
    h = hosp[["ClosingStockPacks", "OpeningStockPacks", "ReceivedQuantityPacks", "DispensedQuantityPacks",
              "WastageQuantityPacks"]].apply(pd.to_numeric)
    assert (h.ClosingStockPacks == h.OpeningStockPacks + h.ReceivedQuantityPacks - h.DispensedQuantityPacks - h.WastageQuantityPacks).all()
    base_max = pd.read_csv(RAW / "05_pharmacy_sales_data.csv", usecols=["SalesTransactionID"]).SalesTransactionID.max()
    assert sales.SalesTransactionID.min() > base_max          # keys continue, never collide


@needs_raw
def test_malformed_injection_rate(tmp_path):
    eng = g.OversamplingEngine(1, out_root=tmp_path, malformed_rate=0.01)
    eng.run(publish=False)
    st = {s.source_key: s for s in eng.stats}
    assert st["sales"].malformed_injected == pytest.approx(0.01 * st["sales"].rows_generated, rel=0.05)


def _mart_ready() -> bool:
    return Path(config.DUCKDB_PATH).exists()


@pytest.mark.skipif(not _mart_ready(), reason="mart not built yet")
def test_query_catalog_runs_sub_second():
    import time
    from pipeline.db import duck
    from pipeline.query.catalog import CATALOG
    with duck(read_only=True) as con:
        for q in CATALOG:
            for v in ("baseline", "optimized"):
                if q.get(v):
                    t = time.perf_counter()
                    con.execute(q[v]).fetchall()
                    assert time.perf_counter() - t < 1.0, q["name"]


@pytest.mark.skipif(not _mart_ready(), reason="mart not built yet")
def test_scd2_has_one_current_row_per_sku():
    from pipeline.db import duck
    with duck(read_only=True) as con:
        bad = con.execute("""SELECT medicine_id FROM mart.dim_medicine WHERE medicine_sk > 0
                             GROUP BY 1 HAVING sum(is_current::INT) <> 1""").fetchall()
        assert bad == []
