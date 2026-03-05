# =============================================================================
# src/gold_pipeline.py
# Retail Intelligence Lakehouse — Gold Layer (Reusable Module)
# Responsibilities:
#   - Sales aggregations (Top products, daily trends, monthly revenue)
#   - Customer Lifetime Value (LTV) & churn risk scoring
#   - Star schema joins (Fact Orders + Dim Customers + Dim Products)
#   - Write analytics-ready Delta tables for BI consumption
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable


# =============================================================================
# 1 · SALES AGGREGATIONS
# =============================================================================

def build_sales_aggregations(
    spark: SparkSession,
    silver_orders: str,
    silver_products: str,
) -> DataFrame:
    """
    Builds a monthly sales aggregation table joining Orders + Products.
    Output: one row per (year_month, product_id, category) with revenue KPIs.

    Columns produced:
        year_month, product_id, product_name, category,
        total_orders, total_quantity, total_revenue,
        avg_order_value, revenue_rank (within month)
    """
    orders   = spark.table(silver_orders)
    products = spark.table(silver_products)

    # Join fact + dimension
    df = (
        orders
        .filter(F.col("status") != "cancelled")
        .join(
            products.select("product_id", "product_name", "category"),
            on="product_id",
            how="left"
        )
    )

    # Aggregate per month + product
    agg = (
        df.groupBy("year_month", "product_id", "product_name", "category")
          .agg(
              F.countDistinct("order_id").alias("total_orders"),
              F.sum("quantity").alias("total_quantity"),
              F.round(F.sum("total_amount"), 2).alias("total_revenue"),
              F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
          )
    )

    # Revenue rank within each month (Top N products)
    window_month = Window.partitionBy("year_month").orderBy(F.col("total_revenue").desc())
    agg = agg.withColumn("revenue_rank", F.rank().over(window_month))

    # Gold metadata
    agg = agg.withColumn("_computed_at", F.current_timestamp())

    return agg


# =============================================================================
# 2 · CUSTOMER LIFETIME VALUE + CHURN RISK
# =============================================================================

def build_customer_ltv(
    spark: SparkSession,
    silver_orders: str,
    silver_customers: str,
    churn_threshold_days: int = 90,
) -> DataFrame:
    """
    Builds a Customer Lifetime Value table combining order history + customer profile.

    Columns produced:
        customer_id, full_name, email, country,
        first_order_date, last_order_date, days_since_last_order,
        total_orders, total_revenue, avg_order_value,
        ltv_segment (High / Medium / Low),
        churn_risk  (High / Low — based on inactivity threshold)
    """
    orders    = spark.table(silver_orders).filter(F.col("status") != "cancelled")
    customers = spark.table(silver_customers)

    # Aggregate order history per customer
    order_stats = (
        orders
        .groupBy("customer_id")
        .agg(
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
        )
        .withColumn("days_since_last_order", F.datediff(
            F.current_date(), F.col("last_order_date")
        ))
    )

    # Join with customer dimension
    df = customers.join(order_stats, on="customer_id", how="left")

    # ── LTV Segmentation ─────────────────────────────────────────────────────
    # Based on total revenue quartiles (simple threshold-based segmentation)
    df = df.withColumn(
        "ltv_segment",
        F.when(F.col("total_revenue") >= 1000, "High")
         .when(F.col("total_revenue") >= 300,  "Medium")
         .otherwise("Low")
    )

    # ── Churn Risk Scoring ────────────────────────────────────────────────────
    # Customers inactive for > churn_threshold_days are flagged as High risk
    df = df.withColumn(
        "churn_risk",
        F.when(
            F.col("days_since_last_order") > churn_threshold_days, "High"
        ).otherwise("Low")
    )

    # Handle customers who have never ordered
    df = df.withColumn(
        "churn_risk",
        F.when(F.col("total_orders").isNull(), "High")
         .otherwise(F.col("churn_risk"))
    )

    # Gold metadata
    df = df.withColumn("_computed_at", F.current_timestamp())

    return df


# =============================================================================
# 3 · DAILY SALES TREND
# =============================================================================

def build_daily_sales_trend(
    spark: SparkSession,
    silver_orders: str,
) -> DataFrame:
    """
    Builds a daily time-series table for stakeholder trend reporting.

    Columns produced:
        order_date, total_orders, total_revenue, total_quantity,
        avg_order_value, revenue_7d_avg (rolling 7-day average),
        revenue_growth_pct (day-over-day % change)
    """
    orders = spark.table(silver_orders).filter(F.col("status") != "cancelled")

    # Daily aggregation
    daily = (
        orders
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.sum("quantity").alias("total_quantity"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
        )
        .orderBy("order_date")
    )

    # ── Rolling 7-day revenue average ────────────────────────────────────────
    window_7d = (
        Window
        .orderBy(F.col("order_date").cast("long"))
        .rowsBetween(-6, 0)    # Current row + 6 preceding rows
    )
    daily = daily.withColumn(
        "revenue_7d_avg",
        F.round(F.avg("total_revenue").over(window_7d), 2)
    )

    # ── Day-over-day revenue growth % ────────────────────────────────────────
    window_lag = Window.orderBy("order_date")
    daily = daily.withColumn(
        "prev_day_revenue", F.lag("total_revenue", 1).over(window_lag)
    )
    daily = daily.withColumn(
        "revenue_growth_pct",
        F.round(
            (F.col("total_revenue") - F.col("prev_day_revenue"))
            / F.col("prev_day_revenue") * 100,
            2
        )
    ).drop("prev_day_revenue")

    # Gold metadata
    daily = daily.withColumn("_computed_at", F.current_timestamp())

    return daily


# =============================================================================
# 4 · WRITE GOLD TABLE (overwrite — Gold is always fully recomputed)
# =============================================================================

def write_gold_table(
    spark: SparkSession,
    df: DataFrame,
    gold_table_path: str,
    table_name: str,
    partition_by: str = None,
) -> None:
    """
    Writes a Gold DataFrame to a Delta Lake managed table.
    Uses overwrite mode — Gold tables are fully recomputed on each run.
    This is safe because Gold reads from the immutable Silver layer.

    Args:
        spark           : Active SparkSession
        df              : Aggregated Gold DataFrame
        gold_table_path : Delta storage path
        table_name      : Unity Catalog table name (catalog.schema.table)
        partition_by    : Optional partition column (e.g. 'year_month')
    """
    writer = (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
    )

    if partition_by:
        writer = writer.partitionBy(partition_by)

    writer.saveAsTable(table_name)

    count = spark.table(table_name).count()
    print(f"✅ Gold table written → {table_name}  ({count:,} rows)")


# =============================================================================
# 5 · OPTIMIZE GOLD TABLE
# =============================================================================

def optimize_gold_table(
    spark: SparkSession,
    table_name: str,
    zorder_columns: list = None,
) -> None:
    """
    Compacts small Delta files and applies Z-Ordering for BI query performance.

    Args:
        spark          : Active SparkSession
        table_name     : Unity Catalog table name
        zorder_columns : Columns to co-locate data on (BI filter columns)
    """
    if zorder_columns:
        zorder_cols = ", ".join(zorder_columns)
        print(f"⚡ Optimizing {table_name} with ZORDER({zorder_cols}) ...")
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({zorder_cols})")
    else:
        print(f"⚡ Optimizing {table_name} ...")
        spark.sql(f"OPTIMIZE {table_name}")

    print(f"✅ OPTIMIZE complete → {table_name}")


# =============================================================================
# 6 · ORCHESTRATORS — one per Gold table
# =============================================================================

def build_and_write_sales_aggregations(
    spark: SparkSession,
    silver_orders: str,
    silver_products: str,
    gold_table_path: str,
    gold_table_name: str,
) -> None:
    print(f"\n{'='*60}")
    print(f"🏆 Building: SALES AGGREGATIONS")
    print(f"   Target: {gold_table_name}")
    print(f"{'='*60}")

    df = build_sales_aggregations(spark, silver_orders, silver_products)
    write_gold_table(spark, df, gold_table_path, gold_table_name, partition_by="year_month")
    optimize_gold_table(spark, gold_table_name, zorder_columns=["year_month", "revenue_rank"])


def build_and_write_customer_ltv(
    spark: SparkSession,
    silver_orders: str,
    silver_customers: str,
    gold_table_path: str,
    gold_table_name: str,
    churn_threshold_days: int = 90,
) -> None:
    print(f"\n{'='*60}")
    print(f"👤 Building: CUSTOMER LIFETIME VALUE")
    print(f"   Target: {gold_table_name}")
    print(f"{'='*60}")

    df = build_customer_ltv(spark, silver_orders, silver_customers, churn_threshold_days)
    write_gold_table(spark, df, gold_table_path, gold_table_name)
    optimize_gold_table(spark, gold_table_name, zorder_columns=["customer_id", "ltv_segment", "churn_risk"])


def build_and_write_daily_trend(
    spark: SparkSession,
    silver_orders: str,
    gold_table_path: str,
    gold_table_name: str,
) -> None:
    print(f"\n{'='*60}")
    print(f"📈 Building: DAILY SALES TREND")
    print(f"   Target: {gold_table_name}")
    print(f"{'='*60}")

    df = build_daily_sales_trend(spark, silver_orders)
    write_gold_table(spark, df, gold_table_path, gold_table_name)
    optimize_gold_table(spark, gold_table_name, zorder_columns=["order_date"])