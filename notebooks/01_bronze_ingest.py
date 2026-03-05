# Databricks notebook source
# =============================================================================
# notebooks/01_bronze_ingest.py
# Retail Intelligence Lakehouse — Bronze Layer Notebook
# Entry point for Databricks Workflows (or manual sequential execution)
#
# Execution order:
#   01_bronze_ingest.py  →  02_silver_transform.py  →  03_gold_analytics.py
# =============================================================================

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🥉 Bronze Layer — Raw Ingestion
# MAGIC **Purpose:** Ingest raw CSV files (Orders, Customers, Products) from Unity Catalog
# MAGIC Volumes into Delta Lake Bronze tables using Databricks Autoloader.
# MAGIC
# MAGIC | Property       | Value                        |
# MAGIC |----------------|------------------------------|
# MAGIC | Mode           | Streaming (Autoloader)       |
# MAGIC | Output format  | Delta Lake                   |
# MAGIC | Write mode     | Append (immutable raw layer) |
# MAGIC | Schema control | Explicit + Evolution enabled |

# COMMAND ----------
# MAGIC %md ### 0 · Setup — Install dependencies and import modules

# COMMAND ----------

import sys
import os

# Add project root to path so we can import from src/ and config/
sys.path.insert(0, "/Workspace/Repos/aitoufkirbrahimab@gmail.com/lakehouse-etl-pipeline")

from config.pipeline_config import (
    CATALOG, SCHEMA,
    RAW_ORDERS,     RAW_CUSTOMERS,     RAW_PRODUCTS,
    BRONZE_ORDERS,  BRONZE_CUSTOMERS,  BRONZE_PRODUCTS,
    CHECKPOINT_ORDERS, CHECKPOINT_CUSTOMERS, CHECKPOINT_PRODUCTS,
    UC_BRONZE_ORDERS,  UC_BRONZE_CUSTOMERS,  UC_BRONZE_PRODUCTS,
)
from src.bronze_pipeline import ingest_source

# COMMAND ----------
# MAGIC %md ### 1 · Initialize Catalog & Schema
# MAGIC Creates the Unity Catalog schema if it doesn't already exist.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"✅ Using catalog: {CATALOG}  |  schema: {SCHEMA}")

# COMMAND ----------
# MAGIC %md ### 2 · Ingest Orders

# COMMAND ----------

ingest_source(
    spark            = spark,
    source           = "orders",
    raw_path         = RAW_ORDERS,
    bronze_table_path= BRONZE_ORDERS,
    checkpoint_path  = CHECKPOINT_ORDERS,
    table_name       = UC_BRONZE_ORDERS,
    file_format      = "csv",
    partition_by     = None,           # Partitioning applied at Silver layer
)

# COMMAND ----------
# MAGIC %md ### 3 · Ingest Customers

# COMMAND ----------

ingest_source(
    spark            = spark,
    source           = "customers",
    raw_path         = RAW_CUSTOMERS,
    bronze_table_path= BRONZE_CUSTOMERS,
    checkpoint_path  = CHECKPOINT_CUSTOMERS,
    table_name       = UC_BRONZE_CUSTOMERS,
    file_format      = "csv",
)

# COMMAND ----------
# MAGIC %md ### 4 · Ingest Products

# COMMAND ----------

ingest_source(
    spark            = spark,
    source           = "products",
    raw_path         = RAW_PRODUCTS,
    bronze_table_path= BRONZE_PRODUCTS,
    checkpoint_path  = CHECKPOINT_PRODUCTS,
    table_name       = UC_BRONZE_PRODUCTS,
    file_format      = "csv",
)

# COMMAND ----------
# MAGIC %md ### 5 · Validate — Row counts per Bronze table

# COMMAND ----------

for table in [UC_BRONZE_ORDERS, UC_BRONZE_CUSTOMERS, UC_BRONZE_PRODUCTS]:
    count = spark.table(table).count()
    print(f"📊 {table:<55} → {count:>8,} rows")

# COMMAND ----------
# MAGIC %md ### 6 · Preview — Sample rows from each Bronze table

# COMMAND ----------

print("── Bronze Orders ──────────────────────────────────")
spark.table(UC_BRONZE_ORDERS).limit(5).display()

print("── Bronze Customers ───────────────────────────────")
spark.table(UC_BRONZE_CUSTOMERS).limit(5).display()

print("── Bronze Products ────────────────────────────────")
spark.table(UC_BRONZE_PRODUCTS).limit(5).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ✅ **Bronze ingestion complete.**
# MAGIC Proceed to → `02_silver_transform.py`