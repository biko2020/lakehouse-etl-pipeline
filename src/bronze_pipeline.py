# =============================================================================
# src/bronze_pipeline.py
# Retail Intelligence Lakehouse — Bronze Layer (Reusable Module)
# NOTE: Uses batch spark.read instead of Autoloader streaming.
#       Autoloader streaming is not supported on Databricks Free Edition Serverless.
# =============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


def get_bronze_schema(source: str) -> StructType:
    """
    Returns all-StringType schemas per source.
    Bronze never casts — Silver handles all type casting.
    """
    schemas = {
        "orders": StructType([
            StructField("order_id",    StringType(), nullable=True),
            StructField("customer_id", StringType(), nullable=True),
            StructField("product_id",  StringType(), nullable=True),
            StructField("quantity",    StringType(), nullable=True),
            StructField("unit_price",  StringType(), nullable=True),
            StructField("order_date",  StringType(), nullable=True),
            StructField("status",      StringType(), nullable=True),
        ]),
        "customers": StructType([
            StructField("customer_id",  StringType(), nullable=True),
            StructField("first_name",   StringType(), nullable=True),
            StructField("last_name",    StringType(), nullable=True),
            StructField("email",        StringType(), nullable=True),
            StructField("country",      StringType(), nullable=True),
            StructField("signup_date",  StringType(), nullable=True),
        ]),
        "products": StructType([
            StructField("product_id",   StringType(), nullable=True),
            StructField("product_name", StringType(), nullable=True),
            StructField("category",     StringType(), nullable=True),
            StructField("unit_price",   StringType(), nullable=True),
            StructField("supplier_id",  StringType(), nullable=True),
        ]),
    }

    if source not in schemas:
        raise ValueError(f"No schema defined for source: '{source}'. Expected: {list(schemas.keys())}")

    return schemas[source]


def read_batch(
    spark: SparkSession,
    raw_path: str,
    source: str,
    file_format: str = "csv",
) -> DataFrame:
    """
    Reads raw CSV files from a Volume path using batch spark.read.
    Injects _ingested_at and _source_file metadata columns.

    Args:
        spark      : Active SparkSession
        raw_path   : Full Volume path to the source folder
        source     : 'orders', 'customers', or 'products'
        file_format: 'csv' or 'json' (default: 'csv')
    """
    schema = get_bronze_schema(source)

    df = (
        spark.read
             .format(file_format)
             .option("header", "true")
             .option("inferSchema", "false")
             .schema(schema)
             .load(raw_path)
    )

    # Metadata injection
    df = (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file",  F.lit(raw_path))
    )

    return df


def write_bronze_table(
    df: DataFrame,
    table_name: str,
    partition_by: str = None,
) -> None:
    """
    Writes a batch DataFrame to a Bronze Delta managed table.
    Uses overwrite mode — Bronze is fully reloaded on each run.

    Args:
        df           : DataFrame from read_batch()
        table_name   : Unity Catalog table name (catalog.schema.table)
        partition_by : Optional column to partition by
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

    print(f"✅ Bronze table written → {table_name}")


def ingest_source(
    spark: SparkSession,
    source: str,
    raw_path: str,
    bronze_table_path: str,
    checkpoint_path: str,
    table_name: str,
    file_format: str = "csv",
    partition_by: str = None,
) -> None:
    """
    End-to-end Bronze ingestion for a single source.

    Args:
        spark             : Active SparkSession
        source            : 'orders', 'customers', or 'products'
        raw_path          : Volume path to raw CSV folder
        bronze_table_path : Unused (kept for API compatibility)
        checkpoint_path   : Unused (kept for API compatibility)
        table_name        : Unity Catalog table name
        file_format       : 'csv' or 'json'
        partition_by      : Optional partition column
    """
    print(f"🔄 Starting Bronze ingestion for: {source.upper()}")
    print(f"   Source path  : {raw_path}")
    print(f"   Target table : {table_name}")

    df = read_batch(
        spark=spark,
        raw_path=raw_path,
        source=source,
        file_format=file_format,
    )

    row_count = df.count()
    print(f"   Rows read    : {row_count:,}")

    if row_count == 0:
        raise ValueError(f"❌ No rows read from {raw_path} — check the file path and CSV content.")

    write_bronze_table(
        df=df,
        table_name=table_name,
        partition_by=partition_by,
    )