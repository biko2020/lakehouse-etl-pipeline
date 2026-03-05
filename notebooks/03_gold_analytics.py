# Databricks notebook source
# =============================================================================
# notebooks/03_gold_analytics.py
# Retail Intelligence Lakehouse — Gold Layer Notebook
# Entry point for Databricks Workflows (or manual sequential execution)
#
# Execution order:
#   01_bronze_ingest.py  →  02_silver_transform.py  →  03_gold_analytics.py
# =============================================================================

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🥇 Gold Layer — Business Analytics
# MAGIC **Purpose:** Build analytics-ready aggregation tables from Silver,
# MAGIC optimized for BI tools, SQL dashboards, and stakeholder reporting.
# MAGIC
# MAGIC | Gold Table                  | Description                              |
# MAGIC |-----------------------------|------------------------------------------|
# MAGIC | `gold_sales_aggregations`   | Monthly revenue & top products by rank   |
# MAGIC | `gold_customer_lifetime_value` | LTV segments + churn risk per customer|
# MAGIC | `gold_daily_sales_trend`    | Daily time-series with 7-day rolling avg |

# COMMAND ----------
# MAGIC %md ### 0 · Setup — Imports

# COMMAND ----------

import sys

sys.path.insert(0, "/Workspace/Repos/<your-username>/lakehouse-etl-pipeline")

from config.pipeline_config import (
    CATALOG, SCHEMA,
    UC_SILVER_ORDERS, UC_SILVER_CUSTOMERS, UC_SILVER_PRODUCTS,
    GOLD_SALES_AGG,   GOLD_CUSTOMER_LTV,   GOLD_DAILY_TREND,
    UC_GOLD_SALES_AGG, UC_GOLD_CUSTOMER_LTV, UC_GOLD_DAILY_TREND,
)
from src.gold_pipeline import (
    build_and_write_sales_aggregations,
    build_and_write_customer_ltv,
    build_and_write_daily_trend,
)

# COMMAND ----------
# MAGIC %md ### 1 · Sales Aggregations — Monthly revenue by product

# COMMAND ----------

build_and_write_sales_aggregations(
    spark            = spark,
    silver_orders    = UC_SILVER_ORDERS,
    silver_products  = UC_SILVER_PRODUCTS,
    gold_table_path  = GOLD_SALES_AGG,
    gold_table_name  = UC_GOLD_SALES_AGG,
)

# COMMAND ----------
# MAGIC %md ### 2 · Customer Lifetime Value + Churn Risk

# COMMAND ----------

build_and_write_customer_ltv(
    spark                = spark,
    silver_orders        = UC_SILVER_ORDERS,
    silver_customers     = UC_SILVER_CUSTOMERS,
    gold_table_path      = GOLD_CUSTOMER_LTV,
    gold_table_name      = UC_GOLD_CUSTOMER_LTV,
    churn_threshold_days = 90,              # Flag customers inactive > 90 days
)

# COMMAND ----------
# MAGIC %md ### 3 · Daily Sales Trend — Time-series for stakeholders

# COMMAND ----------

build_and_write_daily_trend(
    spark           = spark,
    silver_orders   = UC_SILVER_ORDERS,
    gold_table_path = GOLD_DAILY_TREND,
    gold_table_name = UC_GOLD_DAILY_TREND,
)

# COMMAND ----------
# MAGIC %md ### 4 · Validate — Row counts across all Gold tables

# COMMAND ----------

for table in [UC_GOLD_SALES_AGG, UC_GOLD_CUSTOMER_LTV, UC_GOLD_DAILY_TREND]:
    count = spark.table(table).count()
    print(f"📊 {table:<60} → {count:>8,} rows")

# COMMAND ----------
# MAGIC %md ### 5 · Insight — Top 5 Products by Revenue (latest month)

# COMMAND ----------

spark.sql(f"""
    SELECT
        year_month,
        product_name,
        category,
        total_orders,
        total_quantity,
        total_revenue,
        revenue_rank
    FROM {UC_GOLD_SALES_AGG}
    WHERE year_month = (SELECT MAX(year_month) FROM {UC_GOLD_SALES_AGG})
      AND revenue_rank <= 5
    ORDER BY revenue_rank
""").display()

# COMMAND ----------
# MAGIC %md ### 6 · Insight — Customer Churn Risk Summary

# COMMAND ----------

spark.sql(f"""
    SELECT
        churn_risk,
        ltv_segment,
        COUNT(*)                          AS customer_count,
        ROUND(AVG(total_revenue), 2)      AS avg_revenue,
        ROUND(AVG(days_since_last_order)) AS avg_days_inactive
    FROM {UC_GOLD_CUSTOMER_LTV}
    GROUP BY churn_risk, ltv_segment
    ORDER BY churn_risk, ltv_segment
""").display()

# COMMAND ----------
# MAGIC %md ### 7 · Insight — Last 30 Days Sales Trend

# COMMAND ----------

spark.sql(f"""
    SELECT
        order_date,
        total_orders,
        total_revenue,
        revenue_7d_avg,
        revenue_growth_pct
    FROM {UC_GOLD_DAILY_TREND}
    WHERE order_date >= DATE_SUB(CURRENT_DATE(), 30)
    ORDER BY order_date
""").display()

# COMMAND ----------
# MAGIC %md ### 8 · Insight — Monthly Revenue Summary

# COMMAND ----------

spark.sql(f"""
    SELECT
        year_month,
        COUNT(DISTINCT product_id)        AS products_sold,
        SUM(total_orders)                 AS total_orders,
        ROUND(SUM(total_revenue), 2)      AS monthly_revenue,
        ROUND(AVG(avg_order_value), 2)    AS avg_order_value
    FROM {UC_GOLD_SALES_AGG}
    GROUP BY year_month
    ORDER BY year_month DESC
""").display()

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ✅ **Gold layer complete. Pipeline finished.**
# MAGIC
# MAGIC All three layers are ready:
# MAGIC | Layer  | Status |
# MAGIC |--------|--------|
# MAGIC | 🥉 Bronze | Raw ingestion complete |
# MAGIC | 🥈 Silver | Cleansed & deduplicated |
# MAGIC | 🥇 Gold   | Analytics-ready ✅     |