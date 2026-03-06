# =============================================================================
# config/pipeline_config.py
# Retail Intelligence Lakehouse — Databricks Free Edition
# Storage: Unity Catalog Volumes (no external cloud required)
# =============================================================================
# HOW TO SET UP (one-time, in Databricks):
#   1. Go to Catalog Explorer → Create Catalog → name it "retail_lakehouse"
#   2. Inside it, create Schema → name it "etl"
#   3. Inside it, create Volume → name it "storage"
#   Your base path will then be: /Volumes/retail_lakehouse/etl/storage
# =============================================================================

# ── Unity Catalog identifiers ─────────────────────────────────────────────────
CATALOG   = "retail_lakehouse"   # Your catalog name in Databricks Free Edition
SCHEMA    = "etl"                # Schema (database) inside the catalog
VOLUME    = "storage"            # Volume for raw file storage

# ── Base volume path (Unity Catalog standard format) ─────────────────────────
# Format: /Volumes/<catalog>/<schema>/<volume>
VOLUME_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# ── Raw data paths (CSV source files go here) ────────────────────────────────
RAW_PATH       = f"{VOLUME_BASE}/raw"
RAW_ORDERS     = f"{RAW_PATH}/orders.csv"
RAW_CUSTOMERS  = f"{RAW_PATH}/customers.csv"
RAW_PRODUCTS   = f"{RAW_PATH}/products.csv"

# ── Delta Lake paths (medallion layers) ──────────────────────────────────────
DELTA_BASE     = f"{VOLUME_BASE}/delta"

BRONZE_PATH    = f"{DELTA_BASE}/bronze"
SILVER_PATH    = f"{DELTA_BASE}/silver"
GOLD_PATH      = f"{DELTA_BASE}/gold"

# ── Table paths per layer ─────────────────────────────────────────────────────
# Bronze — raw ingested tables
BRONZE_ORDERS    = f"{BRONZE_PATH}/orders"
BRONZE_CUSTOMERS = f"{BRONZE_PATH}/customers"
BRONZE_PRODUCTS  = f"{BRONZE_PATH}/products"

# Silver — cleansed & deduplicated tables
SILVER_ORDERS    = f"{SILVER_PATH}/orders"
SILVER_CUSTOMERS = f"{SILVER_PATH}/customers"
SILVER_PRODUCTS  = f"{SILVER_PATH}/products"

# Gold — analytics-ready tables
GOLD_SALES_AGG   = f"{GOLD_PATH}/sales_aggregations"
GOLD_CUSTOMER_LTV= f"{GOLD_PATH}/customer_lifetime_value"
GOLD_DAILY_TREND = f"{GOLD_PATH}/daily_sales_trend"

# ── Unity Catalog table names (for SQL / Delta Lake MERGE) ────────────────────
# Format: <catalog>.<schema>.<table>
UC_BRONZE_ORDERS    = f"{CATALOG}.{SCHEMA}.bronze_orders"
UC_BRONZE_CUSTOMERS = f"{CATALOG}.{SCHEMA}.bronze_customers"
UC_BRONZE_PRODUCTS  = f"{CATALOG}.{SCHEMA}.bronze_products"

UC_SILVER_ORDERS    = f"{CATALOG}.{SCHEMA}.silver_orders"
UC_SILVER_CUSTOMERS = f"{CATALOG}.{SCHEMA}.silver_customers"
UC_SILVER_PRODUCTS  = f"{CATALOG}.{SCHEMA}.silver_products"

UC_GOLD_SALES_AGG   = f"{CATALOG}.{SCHEMA}.gold_sales_aggregations"
UC_GOLD_CUSTOMER_LTV= f"{CATALOG}.{SCHEMA}.gold_customer_lifetime_value"
UC_GOLD_DAILY_TREND = f"{CATALOG}.{SCHEMA}.gold_daily_sales_trend"

# ── Autoloader checkpoint paths (for incremental ingestion) ──────────────────
CHECKPOINT_BASE      = f"{VOLUME_BASE}/checkpoints"
CHECKPOINT_ORDERS    = f"{CHECKPOINT_BASE}/orders"
CHECKPOINT_CUSTOMERS = f"{CHECKPOINT_BASE}/customers"
CHECKPOINT_PRODUCTS  = f"{CHECKPOINT_BASE}/products"

# ── Runtime settings ─────────────────────────────────────────────────────────
DATABRICKS_RUNTIME_VERSION = "13.3 LTS"   # Minimum required for Unity Catalog volumes