# =============================================================================
# src/silver_pipeline.py
# Retail Intelligence Lakehouse — Silver Layer (Reusable Module)
# Responsibilities:
#   - Deduplication
#   - Type casting & null handling
#   - Data quality checks
#   - Idempotent upserts via Delta Lake MERGE
#   - Z-Ordering for query performance
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from delta.tables import DeltaTable


# =============================================================================
# 1 · CLEANSING FUNCTIONS (per source)
# =============================================================================

def cleanse_orders(df: DataFrame) -> DataFrame:
    """
    Cleanses Bronze orders:
      - Drops rows missing primary key (order_id) or foreign keys
      - Casts types explicitly
      - Derives total_amount = quantity * unit_price
      - Standardizes status to lowercase
    """
    df = (
        df
        # Drop records missing critical keys
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("product_id").isNotNull())

        # Type safety (Bronze ingests everything as strings with schema,
        # but re-casting ensures downstream consistency)
        .withColumn("quantity",    F.col("quantity").cast("int"))
        .withColumn("unit_price",  F.col("unit_price").cast("double"))
        .withColumn("order_date",  F.to_date(F.col("order_date")))
        .withColumn("status",      F.lower(F.trim(F.col("status"))))

        # Derived column
        .withColumn("total_amount", F.round(
            F.col("quantity") * F.col("unit_price"), 2
        ))

        # Partition helper columns
        .withColumn("year_month",  F.date_format(F.col("order_date"), "yyyy-MM"))
        .withColumn("order_year",  F.year(F.col("order_date")))

        # Silver metadata
        .withColumn("_transformed_at", F.current_timestamp())
    )

    return df


def cleanse_customers(df: DataFrame) -> DataFrame:
    """
    Cleanses Bronze customers:
      - Drops rows missing customer_id
      - Normalizes email to lowercase
      - Derives full_name
      - Flags recently signed-up customers
    """
    df = (
        df
        .filter(F.col("customer_id").isNotNull())

        .withColumn("email",       F.lower(F.trim(F.col("email"))))
        .withColumn("first_name",  F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name",   F.initcap(F.trim(F.col("last_name"))))
        .withColumn("country",     F.upper(F.trim(F.col("country"))))
        .withColumn("signup_date", F.to_date(F.col("signup_date")))

        # Derived columns
        .withColumn("full_name", F.concat_ws(" ",
            F.col("first_name"), F.col("last_name")
        ))
        .withColumn("days_since_signup", F.datediff(
            F.current_date(), F.col("signup_date")
        ))

        # Silver metadata
        .withColumn("_transformed_at", F.current_timestamp())
    )

    return df


def cleanse_products(df: DataFrame) -> DataFrame:
    """
    Cleanses Bronze products:
      - Drops rows missing product_id
      - Normalizes category to title case
      - Ensures unit_price is non-negative
    """
    df = (
        df
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("unit_price") >= 0)

        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("category",     F.initcap(F.trim(F.col("category"))))
        .withColumn("unit_price",   F.round(F.col("unit_price").cast("double"), 2))

        # Silver metadata
        .withColumn("_transformed_at", F.current_timestamp())
    )

    return df


# Source → cleansing function mapping
CLEANSE_FN = {
    "orders":    cleanse_orders,
    "customers": cleanse_customers,
    "products":  cleanse_products,
}


# =============================================================================
# 2 · DEDUPLICATION
# =============================================================================

def deduplicate(df: DataFrame, primary_key: str, order_by: str = "_ingested_at") -> DataFrame:
    """
    Removes duplicates by keeping the latest record per primary key.
    Uses a window function to rank rows by ingestion timestamp.

    Args:
        df          : Input DataFrame (from Bronze)
        primary_key : Column name to deduplicate on (e.g. 'order_id')
        order_by    : Tiebreaker column (default: '_ingested_at')
    """
    from pyspark.sql.window import Window

    window = Window.partitionBy(primary_key).orderBy(F.col(order_by).desc())

    df_deduped = (
        df
        .withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    return df_deduped


# =============================================================================
# 3 · DATA QUALITY CHECKS
# =============================================================================

def run_quality_checks(df: DataFrame, source: str) -> DataFrame:
    """
    Runs source-specific data quality assertions.
    Raises a ValueError if critical thresholds are breached.
    Non-blocking checks print warnings only.

    Args:
        df     : Cleansed DataFrame
        source : 'orders', 'customers', or 'products'
    """
    total = df.count()

    if total == 0:
        raise ValueError(f"❌ Data quality FAILED for {source}: DataFrame is empty after cleansing.")

    checks = {
        "orders": [
            ("order_id nulls",    df.filter(F.col("order_id").isNull()).count(),    0,     "critical"),
            ("negative quantity", df.filter(F.col("quantity") < 0).count(),         0,     "critical"),
            ("null order_date",   df.filter(F.col("order_date").isNull()).count(),  total * 0.05, "warning"),
        ],
        "customers": [
            ("customer_id nulls", df.filter(F.col("customer_id").isNull()).count(), 0,     "critical"),
            ("null email",        df.filter(F.col("email").isNull()).count(),        total * 0.10, "warning"),
        ],
        "products": [
            ("product_id nulls",  df.filter(F.col("product_id").isNull()).count(),  0,     "critical"),
            ("zero price",        df.filter(F.col("unit_price") == 0).count(),      total * 0.05, "warning"),
        ],
    }

    for check_name, failed_count, threshold, severity in checks.get(source, []):
        if failed_count > threshold:
            msg = f"[{severity.upper()}] {source}.{check_name}: {failed_count} records failed (threshold: {threshold})"
            if severity == "critical":
                raise ValueError(f"❌ Data quality FAILED — {msg}")
            else:
                print(f"⚠️  {msg}")
        else:
            print(f"✅ {source}.{check_name}: passed")

    return df


# =============================================================================
# 4 · IDEMPOTENT UPSERT (MERGE)
# =============================================================================

MERGE_KEYS = {
    "orders":    "order_id",
    "customers": "customer_id",
    "products":  "product_id",
}


def upsert_to_silver(
    spark: SparkSession,
    df: DataFrame,
    silver_table_path: str,
    table_name: str,
    source: str,
) -> None:
    """
    Idempotent upsert using Delta Lake MERGE.
    - If the record exists → UPDATE all columns
    - If the record is new → INSERT

    This ensures pipeline retries never create duplicates.
    The Silver layer is the Single Source of Truth.

    Args:
        spark             : Active SparkSession
        df                : Cleansed & deduplicated DataFrame
        silver_table_path : Delta path for Silver table
        table_name        : Unity Catalog table name (catalog.schema.table)
        source            : 'orders', 'customers', or 'products'
    """
    merge_key = MERGE_KEYS[source]

    if DeltaTable.isDeltaTable(spark, silver_table_path):
        # ── Table exists → MERGE ──────────────────────────────────────────────
        delta_table = DeltaTable.forPath(spark, silver_table_path)

        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                f"target.{merge_key} = source.{merge_key}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"🔀 MERGE complete → {table_name}")

    else:
        # ── First run → CREATE table via initial write ────────────────────────
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .saveAsTable(table_name)
        )

        print(f"🆕 Silver table created → {table_name}")


# =============================================================================
# 5 · PERFORMANCE OPTIMIZATION
# =============================================================================

def optimize_silver_table(
    spark: SparkSession,
    table_name: str,
    zorder_columns: list,
) -> None:
    """
    Runs OPTIMIZE + ZORDER on the Silver table for query acceleration.
    Should be run after each MERGE to compact small Delta files.

    Args:
        spark          : Active SparkSession
        table_name     : Unity Catalog table name
        zorder_columns : Columns to co-locate data on (e.g. ['customer_id', 'order_date'])
    """
    zorder_cols = ", ".join(zorder_columns)
    print(f"⚡ Optimizing {table_name} with ZORDER({zorder_cols}) ...")

    spark.sql(f"""
        OPTIMIZE {table_name}
        ZORDER BY ({zorder_cols})
    """)

    print(f"✅ OPTIMIZE complete → {table_name}")


# =============================================================================
# 6 · ORCHESTRATOR — single source end-to-end
# =============================================================================

def transform_source(
    spark: SparkSession,
    source: str,
    bronze_table_name: str,
    silver_table_path: str,
    silver_table_name: str,
    primary_key: str,
    zorder_columns: list = None,
    run_quality: bool = True,
) -> None:
    """
    End-to-end Silver transformation for a single source.
    Combines: read Bronze → cleanse → deduplicate → quality checks → MERGE → OPTIMIZE

    Args:
        spark              : Active SparkSession
        source             : 'orders', 'customers', or 'products'
        bronze_table_name  : Unity Catalog Bronze table to read from
        silver_table_path  : Delta path for Silver table
        silver_table_name  : Unity Catalog Silver table name
        primary_key        : Dedup & merge key column
        zorder_columns     : Columns for Z-Ordering (optional)
        run_quality        : Whether to run data quality checks (default: True)
    """
    print(f"\n{'='*60}")
    print(f"🔄 Silver transformation: {source.upper()}")
    print(f"   Source : {bronze_table_name}")
    print(f"   Target : {silver_table_name}")
    print(f"{'='*60}")

    # Step 1 — Read from Bronze
    df = spark.table(bronze_table_name)
    print(f"📥 Bronze rows read: {df.count():,}")

    # Step 2 — Deduplicate
    df = deduplicate(df, primary_key=primary_key)
    print(f"🧹 After deduplication: {df.count():,} rows")

    # Step 3 — Cleanse
    cleanse_fn = CLEANSE_FN.get(source)
    if not cleanse_fn:
        raise ValueError(f"No cleanse function for source: {source}")
    df = cleanse_fn(df)

    # Step 4 — Data quality checks
    if run_quality:
        df = run_quality_checks(df, source)

    # Step 5 — Upsert to Silver
    upsert_to_silver(spark, df, silver_table_path, silver_table_name, source)

    # Step 6 — Optimize
    if zorder_columns:
        optimize_silver_table(spark, silver_table_name, zorder_columns)

    print(f"✅ Silver transformation complete → {silver_table_name}\n")