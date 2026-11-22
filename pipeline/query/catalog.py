"""
Analytical query catalog for the SDI mart. Every entry feeds one dashboard widget / KPI card.
Where a pre-aggregated summary table can answer the question, two variants are kept:
  baseline  - star-join over the fact table (what you would write first)
  optimized - same answer from mart.agg_* summary tables
The runner times both, which is how the query-performance dashboard shows the speed-up.
"""

# reusable predicate: the most recent 3 months present in the data ("current quarter" for movers)
LAST_90 = "(SELECT max(full_date) FROM mart.fact_sales f JOIN mart.dim_date d ON d.date_key = f.sale_date_key) - INTERVAL 90 DAY"

CATALOG = [
    # ------------------------------------------------------------ Dashboard A: Hospital forecast vs actual
    dict(name="A1_forecast_vs_actual_by_hospital", widget="Forecast vs Actual ratio by hospital (bar)",
         dashboard="A Hospital Forecast vs Actual",
         baseline="""
            SELECT h.hospital_name, sum(f.forecast_packs) forecast, sum(f.consumed_packs) consumed,
                   round(sum(f.consumed_packs) / sum(f.forecast_packs), 3) AS actual_to_forecast_ratio,
                   sum(f.unfulfilled_packs) unfulfilled
            FROM mart.fact_hospital_demand f JOIN mart.dim_hospital h USING (hospital_sk)
            GROUP BY 1 ORDER BY actual_to_forecast_ratio DESC""",
         optimized="""
            SELECT hospital_name, sum(forecast_packs) forecast, sum(consumed_packs) consumed,
                   round(sum(consumed_packs) / sum(forecast_packs), 3) AS actual_to_forecast_ratio,
                   sum(unfulfilled_packs) unfulfilled
            FROM mart.agg_hospital_monthly GROUP BY 1 ORDER BY actual_to_forecast_ratio DESC"""),
    dict(name="A2_unfulfilled_by_category_month", widget="Unfulfilled demand heatmap (category x month)",
         dashboard="A Hospital Forecast vs Actual",
         baseline="""
            SELECT d.year_month, m.disease_category, sum(f.unfulfilled_packs) unfulfilled, sum(f.stockout_days) stockout_days
            FROM mart.fact_hospital_demand f JOIN mart.dim_date d ON d.date_key = f.month_date_key
            JOIN mart.dim_medicine m USING (medicine_sk) GROUP BY 1, 2 ORDER BY 1, 2""",
         optimized="""
            SELECT year_month, disease_category, sum(unfulfilled_packs) unfulfilled, sum(stockout_days) stockout_days
            FROM mart.agg_hospital_monthly GROUP BY 1, 2 ORDER BY 1, 2"""),
    dict(name="A3_forecast_error_trend", widget="Monthly forecast bias & MAPE (line)",
         dashboard="A Hospital Forecast vs Actual",
         baseline="""
            SELECT d.year_month, round(100 * sum(f.consumed_packs - f.forecast_packs) / sum(f.forecast_packs), 2) bias_pct,
                   round(100 * avg(abs(f.consumed_packs - f.forecast_packs) / nullif(f.forecast_packs, 0)), 2) mape_pct
            FROM mart.fact_hospital_demand f JOIN mart.dim_date d ON d.date_key = f.month_date_key GROUP BY 1 ORDER BY 1""",
         optimized="""
            SELECT year_month, round(100 * sum(consumed_packs - forecast_packs) / sum(forecast_packs), 2) bias_pct,
                   round(100 * avg(abs(consumed_packs - forecast_packs) / nullif(forecast_packs, 0)), 2) mape_pct
            FROM mart.agg_hospital_monthly GROUP BY 1 ORDER BY 1"""),
    # ------------------------------------------------------------ Dashboard B: Channel revenue & margin
    dict(name="B1_channel_kpis", widget="KPI cards: Net revenue / GM% / Units / Transactions",
         dashboard="B Channel Revenue & Margin",
         baseline="""
            SELECT c.channel_group, count(*) txns, sum(f.quantity_packs) units, round(sum(f.net_sales_usd), 2) net_sales,
                   round(sum(f.gross_margin_usd), 2) gross_margin, round(100 * sum(f.gross_margin_usd) / sum(f.net_sales_usd), 2) gm_pct,
                   round(100 * sum(f.discount_usd) / sum(f.gross_sales_usd), 2) discount_pct
            FROM mart.fact_sales f JOIN mart.dim_channel c USING (channel_sk) GROUP BY 1 ORDER BY net_sales DESC""",
         optimized="""
            SELECT channel_group, sum(txn_count) txns, sum(quantity_packs) units, round(sum(net_sales_usd), 2) net_sales,
                   round(sum(gross_margin_usd), 2) gross_margin, round(100 * sum(gross_margin_usd) / sum(net_sales_usd), 2) gm_pct,
                   round(100 * sum(discount_usd) / sum(gross_sales_usd), 2) discount_pct
            FROM mart.agg_sales_monthly GROUP BY 1 ORDER BY net_sales DESC"""),
    dict(name="B2_channel_monthly_trend", widget="Monthly net revenue by channel (stacked area)",
         dashboard="B Channel Revenue & Margin",
         baseline="""
            SELECT d.year_month, c.channel_group, round(sum(f.net_sales_usd), 2) net_sales, round(sum(f.gross_margin_usd), 2) gm
            FROM mart.fact_sales f JOIN mart.dim_date d ON d.date_key = f.sale_date_key
            JOIN mart.dim_channel c USING (channel_sk) GROUP BY 1, 2 ORDER BY 1, 2""",
         optimized="""
            SELECT year_month, channel_group, round(sum(net_sales_usd), 2) net_sales, round(sum(gross_margin_usd), 2) gm
            FROM mart.agg_sales_monthly GROUP BY 1, 2 ORDER BY 1, 2"""),
    dict(name="B3_margin_by_category_channel", widget="Gross margin % matrix (disease category x channel)",
         dashboard="B Channel Revenue & Margin",
         baseline="""
            SELECT m.disease_category, c.channel_group, round(100 * sum(f.gross_margin_usd) / sum(f.net_sales_usd), 2) gm_pct,
                   round(sum(f.net_sales_usd), 2) net_sales
            FROM mart.fact_sales f JOIN mart.dim_medicine m USING (medicine_sk) JOIN mart.dim_channel c USING (channel_sk)
            GROUP BY 1, 2 ORDER BY 1, 2""",
         optimized="""
            SELECT disease_category, channel_group, round(100 * sum(gross_margin_usd) / sum(net_sales_usd), 2) gm_pct,
                   round(sum(net_sales_usd), 2) net_sales
            FROM mart.agg_sales_monthly GROUP BY 1, 2 ORDER BY 1, 2"""),
    # ------------------------------------------------------------ Dashboard C: fast vs slow movers
    dict(name="C1_top10_fast_movers", widget="Top-10 fast-moving medicines (last 90 days)",
         dashboard="C Fast vs Slow Movers",
         baseline=f"""
            SELECT m.medicine_id, m.generic_name, m.disease_category, m.therapeutic_class, m.cold_chain_flag,
                   sum(f.quantity_packs) units_90d, round(sum(f.net_sales_usd), 2) net_90d
            FROM mart.fact_sales f JOIN mart.dim_date d ON d.date_key = f.sale_date_key JOIN mart.dim_medicine m USING (medicine_sk)
            WHERE d.full_date > {LAST_90}
            GROUP BY ALL ORDER BY units_90d DESC LIMIT 10"""),
    dict(name="C2_bottom10_slow_movers", widget="Bottom-10 slow-moving medicines (last 90 days)",
         dashboard="C Fast vs Slow Movers",
         baseline=f"""
            SELECT m.medicine_id, m.generic_name, m.disease_category, m.therapeutic_class, m.cold_chain_flag,
                   sum(f.quantity_packs) units_90d, round(sum(f.net_sales_usd), 2) net_90d
            FROM mart.fact_sales f JOIN mart.dim_date d ON d.date_key = f.sale_date_key JOIN mart.dim_medicine m USING (medicine_sk)
            WHERE d.full_date > {LAST_90}
            GROUP BY ALL ORDER BY units_90d ASC LIMIT 10"""),
    dict(name="C3_velocity_rank_by_category", widget="Velocity rank within each disease category (table)",
         dashboard="C Fast vs Slow Movers",
         baseline="""
            SELECT disease_category, generic_name, units, rank_in_category,
                   CASE WHEN pct <= 0.2 THEN 'Fast (top 20%)' WHEN pct >= 0.8 THEN 'Slow (bottom 20%)' ELSE 'Medium' END AS velocity_class
            FROM (SELECT m.disease_category, m.generic_name, sum(f.quantity_packs) units,
                         rank() OVER (PARTITION BY m.disease_category ORDER BY sum(f.quantity_packs) DESC) rank_in_category,
                         percent_rank() OVER (ORDER BY sum(f.quantity_packs) DESC) pct
                  FROM mart.fact_sales f JOIN mart.dim_medicine m USING (medicine_sk) GROUP BY 1, 2)
            ORDER BY 1, 4""",
         optimized="""
            SELECT disease_category, generic_name, units, rank_in_category,
                   CASE WHEN pct <= 0.2 THEN 'Fast (top 20%)' WHEN pct >= 0.8 THEN 'Slow (bottom 20%)' ELSE 'Medium' END AS velocity_class
            FROM (SELECT disease_category, generic_name, sum(quantity_packs) units,
                         rank() OVER (PARTITION BY disease_category ORDER BY sum(quantity_packs) DESC) rank_in_category,
                         percent_rank() OVER (ORDER BY sum(quantity_packs) DESC) pct
                  FROM mart.agg_sales_monthly GROUP BY 1, 2)
            ORDER BY 1, 4"""),
    # ------------------------------------------------------------ OLAP operations
    dict(name="O1_rollup_region_state", widget="OLAP roll-up: region > state > city",
         dashboard="OLAP",
         baseline="""
            SELECT l.census_region, l.state, l.city, round(sum(f.net_sales_usd), 2) net_sales, GROUPING_ID(l.census_region, l.state, l.city) lvl
            FROM mart.fact_sales f JOIN mart.dim_location l USING (location_sk)
            GROUP BY ROLLUP (l.census_region, l.state, l.city) ORDER BY 1 NULLS LAST, 2 NULLS LAST, 3 NULLS LAST"""),
    dict(name="O2_cube_channel_category", widget="OLAP cube: channel x disease category",
         dashboard="OLAP",
         baseline="""
            SELECT c.channel_group, m.disease_category, round(sum(f.net_sales_usd), 2) net_sales
            FROM mart.fact_sales f JOIN mart.dim_channel c USING (channel_sk) JOIN mart.dim_medicine m USING (medicine_sk)
            GROUP BY CUBE (c.channel_group, m.disease_category) ORDER BY 1 NULLS LAST, 2 NULLS LAST"""),
    dict(name="O3_slice_dice_2025_hospital_cold", widget="OLAP slice/dice: 2025, Hospital channel, cold-chain SKUs",
         dashboard="OLAP",
         baseline="""
            SELECT d.quarter_label, m.generic_name, sum(f.quantity_packs) units, round(sum(f.net_sales_usd), 2) net_sales
            FROM mart.fact_sales f JOIN mart.dim_date d ON d.date_key = f.sale_date_key
            JOIN mart.dim_channel c USING (channel_sk) JOIN mart.dim_medicine m USING (medicine_sk)
            WHERE d.year_num = 2025 AND c.channel_group = 'Hospital' AND m.cold_chain_flag = 'Y'
            GROUP BY 1, 2 ORDER BY 1, 2"""),
    dict(name="O4_pivot_year_channel", widget="OLAP pivot: net sales, years as columns",
         dashboard="OLAP",
         baseline="""
            PIVOT (SELECT d.year_num::VARCHAR AS yr, c.channel_group, f.net_sales_usd
                   FROM mart.fact_sales f JOIN mart.dim_date d ON d.date_key = f.sale_date_key JOIN mart.dim_channel c USING (channel_sk))
            ON yr USING round(sum(net_sales_usd), 0) GROUP BY channel_group ORDER BY channel_group"""),
    dict(name="O5_drill_across_sales_vs_hospital", widget="Drill-across: pharmacy units vs hospital consumption per month",
         dashboard="OLAP",
         baseline="""
            WITH s AS (SELECT d.year_month, m.disease_category, sum(f.quantity_packs) pharmacy_units
                       FROM mart.fact_sales f JOIN mart.dim_date d ON d.date_key = f.sale_date_key JOIN mart.dim_medicine m USING (medicine_sk)
                       GROUP BY 1, 2),
                 h AS (SELECT d.year_month, m.disease_category, sum(f.consumed_packs) hospital_consumed, sum(f.forecast_packs) hospital_forecast
                       FROM mart.fact_hospital_demand f JOIN mart.dim_date d ON d.date_key = f.month_date_key JOIN mart.dim_medicine m USING (medicine_sk)
                       GROUP BY 1, 2)
            SELECT coalesce(s.year_month, h.year_month) year_month, coalesce(s.disease_category, h.disease_category) disease_category,
                   s.pharmacy_units, h.hospital_consumed, h.hospital_forecast
            FROM s FULL OUTER JOIN h ON s.year_month = h.year_month AND s.disease_category = h.disease_category ORDER BY 1, 2""",
         optimized="""
            WITH s AS (SELECT year_month, disease_category, sum(quantity_packs) pharmacy_units FROM mart.agg_sales_monthly GROUP BY 1, 2),
                 h AS (SELECT year_month, disease_category, sum(consumed_packs) hospital_consumed, sum(forecast_packs) hospital_forecast
                       FROM mart.agg_hospital_monthly GROUP BY 1, 2)
            SELECT coalesce(s.year_month, h.year_month) year_month, coalesce(s.disease_category, h.disease_category) disease_category,
                   s.pharmacy_units, h.hospital_consumed, h.hospital_forecast
            FROM s FULL OUTER JOIN h ON s.year_month = h.year_month AND s.disease_category = h.disease_category ORDER BY 1, 2"""),
    dict(name="O6_scd2_price_versions", widget="SCD-2 audit: revenue by price version",
         dashboard="OLAP",
         baseline="""
            SELECT m.medicine_id, m.generic_name, m.version, m.retail_price_usd, m.effective_from, m.effective_to, m.is_current,
                   count(f.sales_transaction_id) txns, round(sum(f.net_sales_usd), 2) net_sales
            FROM mart.dim_medicine m LEFT JOIN mart.fact_sales f USING (medicine_sk)
            WHERE m.medicine_id IN (SELECT medicine_id FROM mart.dim_medicine GROUP BY 1 HAVING count(*) > 1)
            GROUP BY ALL ORDER BY 1, 3"""),
]
