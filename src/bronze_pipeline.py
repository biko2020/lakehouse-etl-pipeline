# =============================================================================
# src/bronze_pipeline.py
# Retail Intelligence Lakehouse — Bronze Layer (Reusable Module)
# Responsibilities:
#   - Autoloader (cloudFiles) setup for incremental ingestion
#   - Schema inference & enforcement
#   - Metadata injection (_ingested_at, _source_file)
#   - Write raw data to Delta Lake Bronze tables
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from delta.tables import DeltaTable


def get_autoloader_schema(source: str) -> StructType:
    """
    Returns explicit schemas per source to enforce types at ingestion time.
    Using explicit schemas avoids schema inference cost on every run.
    """
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, IntegerType, DoubleType, DateType
    )

    schemas = {
        "orders": StructType([
            StructField("order_id",    StringType(),  nullable=False),
            StructField("customer_id", StringType(),  nullable=False),
            StructField("product_id",  StringType(),  nullable=False),
            StructField("quantity",    IntegerType(), nullable=True),
            StructField("unit_price",  DoubleType(),  nullable=True),
            StructField("order_date",  DateType(),    nullable=True),
            StructField("status",      StringType(),  nullable=True),
        ]),
        "customers": StructType([
            StructField("customer_id",  StringType(), nullable=False),
            StructField("first_name",   StringType(), nullable=True),
            StructField("last_name",    StringType(), nullable=True),
            StructField("email",        StringType(), nullable=True),
            StructField("country",      StringType(), nullable=True),
            StructField("signup_date",  DateType(),   nullable=True),
        ]),
        "products": StructType([
            StructField("product_id",   StringType(), nullable=False),
            StructField("product_name", StringType(), nullable=True),
            StructField("category",     StringType(), nullable=True),
            StructField("unit_price",   DoubleType(), nullable=True),
            StructField("supplier_id",  StringType(), nullable=True),
        ]),
    }

    if source not in schemas:
        raise ValueError(f"No schema defined for source: '{source}'. Expected one of {list(schemas.keys())}")

    return schemas[source]


def read_with_autoloader(
    spark: SparkSession,
    raw_path: str,
    checkpoint_path: str,
    source: str,
    file_format: str = "csv",
    schema_evolution: bool = True,
) -> object:
    """
    Configures and returns an Autoloader (cloudFiles) streaming reader.

    Args:
        spark           : Active SparkSession
        raw_path        : Source path where raw files land (Volume path)
        checkpoint_path : Path for Autoloader to track processed files
        source          : One of 'orders', 'customers', 'products'
        file_format     : 'csv' or 'json' (default: 'csv')
        schema_evolution: If True, merges new columns automatically

    Returns:
        A streaming DataFrame with injected metadata columns
    """
    schema = get_autoloader_schema(source)

    # When an explicit schema is provided, evolution mode must be "none"
    # "addNewColumns" is only compatible with schema inference (no .schema())
    reader = (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", file_format)
             .option("cloudFiles.schemaLocation", checkpoint_path)
             .option("cloudFiles.schemaEvolutionMode", "none")
             .option("header", "true")          # CSV only
             .option("inferSchema", "false")     # We always use explicit schema
             .schema(schema)
             .load(raw_path)
    )

    # ── Metadata injection ────────────────────────────────────────────────────
    # _ingested_at : timestamp when the record was written to Bronze
    # _source_file : original filename for lineage & debugging
    df = (
        reader
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    return df


def write_bronze_stream(
    df,
    bronze_table_path: str,
    checkpoint_path: str,
    table_name: str,
    partition_by: str = None,
) -> None:
    """
    Writes an Autoloader streaming DataFrame to a Bronze Delta table.
    Uses 'append' mode — Bronze is an immutable raw layer (no upserts here).

    Args:
        df               : Streaming DataFrame from read_with_autoloader()
        bronze_table_path: Delta storage path for this Bronze table
        checkpoint_path  : Autoloader checkpoint path (same as reader)
        table_name       : Unity Catalog table name (catalog.schema.table)
        partition_by     : Optional column to partition Delta table by
    """
    writer = (
        df.writeStream
          .format("delta")
          .outputMode("append")
          .option("checkpointLocation", checkpoint_path)
          .option("mergeSchema", "true")         # Allow schema evolution writes
    )

    if partition_by:
        writer = writer.partitionBy(partition_by)

    (
        writer
        .trigger(availableNow=True)              # Process all available files, then stop
        .toTable(table_name)                     # Writes to Unity Catalog managed table
        .awaitTermination()
    )

    print(f"✅ Bronze ingestion complete → {table_name}")


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
    End-to-end ingestion for a single source (orders / customers / products).
    Combines read_with_autoloader() + write_bronze_stream() in one call.

    Args:
        spark             : Active SparkSession
        source            : 'orders', 'customers', or 'products'
        raw_path          : Raw files landing path
        bronze_table_path : Delta path for Bronze table
        checkpoint_path   : Autoloader checkpoint path
        table_name        : Unity Catalog table name
        file_format       : 'csv' or 'json'
        partition_by      : Optional partition column
    """
    print(f"🔄 Starting Bronze ingestion for: {source.upper()}")
    print(f"   Source path  : {raw_path}")
    print(f"   Target table : {table_name}")

    df = read_with_autoloader(
        spark=spark,
        raw_path=raw_path,
        checkpoint_path=checkpoint_path,
        source=source,
        file_format=file_format,
    )

    write_bronze_stream(
        df=df,
        bronze_table_path=bronze_table_path,
        checkpoint_path=checkpoint_path,
        table_name=table_name,
        partition_by=partition_by,
    )