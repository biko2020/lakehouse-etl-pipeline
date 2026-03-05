# Databricks notebook source
# =============================================================================
# notebooks/02_silver_transform.py
# Retail Intelligence Lakehouse — Silver Layer Notebook
# Entry point for Databricks Workflows (or manual sequential execution)
#
# Execution order:
#   01_bronze_ingest.py  →  02_silver_transform.py  →  03_gold_analytics.py
# =============================================================================

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🥈 Silver Layer — Cleanse & Model
# MAGIC **Purpose:** Read from Bronze Delta tables, apply cleansing, deduplication,
# MAGIC and data quality checks, then upsert into Silver via Delta Lake MERGE.
# MAGIC
# MAGIC | Property       | Value                              |
# MAGIC |----------------|------------------------------------|
# MAGIC | Input          | Bronze Delta tables (Unity Catalog) |
# MAGIC | Output         | Silver Delta tables (Unity Catalog) |
# MAGIC | Write mode     | MERGE (idempotent upsert)           |
# MAGIC | Optimization   | OPTIMIZE + ZORDER after each MERGE  |
# MAGIC | Dedup strategy | Latest record per primary key       |

# COMMAND ----------
# MAGIC %md ### 0 · Setup — Imports

# COMMAND ----------

import sys

sys.path.insert(0, "/Workspace/Repos/aitoufkirbrahimab@gmail.com/lakehouse-etl-pipeline")

from config.pipeline_config import (
    CATALOG, SCHEMA,
    SILVER_ORDERS,   SILVER_CUSTOMERS,   SILVER_PRODUCTS,
    UC_BRONZE_ORDERS, UC_BRONZE_CUSTOMERS, UC_BRONZE_PRODUCTS,
    UC_SILVER_ORDERS, UC_SILVER_CUSTOMERS, UC_SILVER_PRODUCTS,
)
from src.silver_pipeline import transform_source

# COMMAND ----------
# MAGIC %md ### 1 · Transform Orders

# COMMAND ----------

transform_source(
    spark              = spark,
    source             = "orders",
    bronze_table_name  = UC_BRONZE_ORDERS,
    silver_table_path  = SILVER_ORDERS,
    silver_table_name  = UC_SILVER_ORDERS,
    primary_key        = "order_id",
    zorder_columns     = ["customer_id", "order_date"],  # Most common filter cols
    run_quality        = True,
)

# COMMAND ----------
# MAGIC %md ### 2 · Transform Customers

# COMMAND ----------

transform_source(
    spark              = spark,
    source             = "customers",
    bronze_table_name  = UC_BRONZE_CUSTOMERS,
    silver_table_path  = SILVER_CUSTOMERS,
    silver_table_name  = UC_SILVER_CUSTOMERS,
    primary_key        = "customer_id",
    zorder_columns     = ["customer_id", "country"],
    run_quality        = True,
)

# COMMAND ----------
# MAGIC %md ### 3 · Transform Products

# COMMAND ----------

transform_source(
    spark              = spark,
    source             = "products",
    bronze_table_name  = UC_BRONZE_PRODUCTS,
    silver_table_path  = SILVER_PRODUCTS,
    silver_table_name  = UC_SILVER_PRODUCTS,
    primary_key        = "product_id",
    zorder_columns     = ["product_id", "category"],
    run_quality        = True,
)

# COMMAND ----------
# MAGIC %md ### 4 · Validate — Row counts & schema

# COMMAND ----------

for table in [UC_SILVER_ORDERS, UC_SILVER_CUSTOMERS, UC_SILVER_PRODUCTS]:
    df = spark.table(table)
    print(f"📊 {table:<55} → {df.count():>8,} rows  |  {len(df.columns)} columns")

# COMMAND ----------
# MAGIC %md ### 5 · Preview — Sample rows from each Silver table

# COMMAND ----------

print("── Silver Orders ──────────────────────────────────")
spark.table(UC_SILVER_ORDERS).limit(5).display()

print("── Silver Customers ───────────────────────────────")
spark.table(UC_SILVER_CUSTOMERS).limit(5).display()

print("── Silver Products ────────────────────────────────")
spark.table(UC_SILVER_PRODUCTS).limit(5).display()

# COMMAND ----------
# MAGIC %md ### 6 · Delta Table History — Audit log

# COMMAND ----------

for table in [UC_SILVER_ORDERS, UC_SILVER_CUSTOMERS, UC_SILVER_PRODUCTS]:
    print(f"\n── History: {table}")
    spark.sql(f"DESCRIBE HISTORY {table}").select(
        "version", "timestamp", "operation", "operationMetrics"
    ).limit(5).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ✅ **Silver transformation complete.**
# MAGIC Proceed to → `03_gold_analytics.py`