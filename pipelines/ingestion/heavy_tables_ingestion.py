# Databricks notebook source
"""
Heavy Tables Ingestion Job
Standalone Spark job for ingesting large PostgreSQL tables (15M+ rows)
Writes directly to Bronze layer (no RAW intermediary)

Architecture:
PostgreSQL → JDBC (watermark incremental) → Delta Bronze
"""

import yaml
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, max as spark_max, lit

# COMMAND ----------
# Configuration
ENVIRONMENT = spark.conf.get("job.env", "dev").lower()
CHECKPOINT_LOCATION = spark.conf.get(
    "job.checkpoint_location", f"/mnt/checkpoints/heavy_ingestion/{ENVIRONMENT}"
)
WATERMARK_DAYS_DEFAULT = 180  # First load: last 6 months
CATALOG_PUBLIC = spark.conf.get("catalog_public", f"{ENVIRONMENT}_vcn_public")
CATALOG_FINANCIAL = spark.conf.get("catalog_financial", f"{ENVIRONMENT}_vcn_financial")

# COMMAND ----------
# Load configuration (same YAML as DLT pipeline)
try:
    import os

    config_path = f"{os.getcwd()}/config/tables_vcn_public.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
except:
    config_path = f"/Workspace/Repos/sp_b2b_ops_bot/b2b-databricks-dlt-{ENVIRONMENT}/config/tables_vcn_public.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

SOURCE_CATALOG = config["source_catalog"]
# Filter only tables marked as heavy: true
HEAVY_TABLES = [t for t in config.get("tables", []) if t.get("heavy", False)]

print(f"Found {len(HEAVY_TABLES)} heavy tables to ingest")
for t in HEAVY_TABLES:
    print(f"  - {t['name']} (schema: {t['schema']})")


# COMMAND ----------
def get_jdbc_url(catalog_name: str) -> str:
    """Extract JDBC URL from federated catalog configuration"""
    try:
        return spark.conf.get(
            f"spark.databricks.sql.federation.catalog.{catalog_name}.url"
        )
    except:
        raise ValueError(f"JDBC URL not found for catalog {catalog_name}")


def with_jdbc_timeouts(jdbc_url: str) -> str:
    """Add connection and socket timeouts to JDBC URL"""
    separator = "&" if "?" in jdbc_url else "?"
    return f"{jdbc_url}{separator}socketTimeout=0&connectTimeout=10"


def format_timestamp(value) -> str:
    """Format datetime to SQL timestamp literal string"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def get_last_loaded_watermark(target_table: str, watermark_column: str):
    """Get maximum watermark value from target Delta table for incremental loading"""
    try:
        df = spark.read.table(target_table)
        row = df.select(spark_max(watermark_column).alias("wm")).collect()[0]
        return row["wm"]
    except Exception as e:
        print(f"Could not read watermark from {target_table}: {str(e)}")
        return None


def get_filtered_partition_bounds(
    jdbc_url: str,
    schema_name: str,
    table_name: str,
    partition_column: str,
    watermark_column: str,
    cutoff_ts: str,
):
    """Calculate MIN/MAX bounds for partition column filtered by watermark cutoff"""
    bounds_query = (
        f'(SELECT MIN("{partition_column}") AS min_v, '
        f'MAX("{partition_column}") AS max_v '
        f"FROM {schema_name}.{table_name} "
        f"WHERE {watermark_column} >= TIMESTAMP '{cutoff_ts}') AS bounds"
    )

    df_bounds = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", bounds_query)
        .option("driver", "org.postgresql.Driver")
        .option("queryTimeout", "0")
        .load()
    )

    row = df_bounds.first()
    if row is None or row["min_v"] is None or row["max_v"] is None:
        return None

    min_v, max_v = row["min_v"], row["max_v"]
    if min_v == max_v:
        return None

    return int(min_v), int(max_v)


def resolve_target_catalog(schema_name: str) -> str:
    """Resolve target catalog based on source schema"""
    if schema_name == "public":
        return CATALOG_PUBLIC
    if schema_name in ("financial", "financial_manager"):
        return CATALOG_FINANCIAL
    return f"{schema_name}_vcn_{ENVIRONMENT}"


def calculate_num_partitions(lower_bound: int, upper_bound: int) -> int:
    """Calculate optimal number of partitions based on ID range and cluster parallelism"""
    range_size = max(1, upper_bound - lower_bound)
    base_parallelism = max(4, spark.sparkContext.defaultParallelism // 2)
    # 1M IDs per partition max
    return min(base_parallelism, max(1, range_size // 1_000_000))


# COMMAND ----------
def ingest_heavy_table(table_conf: dict) -> dict:
    """
    Ingest a single heavy table using watermark-based incremental loading
    Returns dict with status and metrics
    """
    schema_name = table_conf["schema"]
    table_name = table_conf["name"]
    # Use pk field (consistent with DLT pipeline)
    partition_column = table_conf.get("pk")
    watermark_column = table_conf.get("watermark")
    # watermark_days can be set per table in YAML, defaults to 180
    watermark_days = int(table_conf.get("watermark_days", WATERMARK_DAYS_DEFAULT))

    target_catalog = resolve_target_catalog(schema_name)
    target_table = f"{target_catalog}.bronze.{table_name}"

    print(f"\n{'='*80}")
    print(f"Starting ingestion: {table_name}")
    print(f"Target: {target_table}")
    print(f"Watermark column: {watermark_column}")
    print(f"Partition column: {partition_column}")
    print(f"{'='*80}\n")

    # Get JDBC URL
    jdbc_url = get_jdbc_url(SOURCE_CATALOG)
    timed_url = with_jdbc_timeouts(jdbc_url)

    # Determine watermark cutoff
    last_wm = get_last_loaded_watermark(target_table, watermark_column)

    if last_wm:
        cutoff_ts = format_timestamp(last_wm)
        watermark_filter = f"{watermark_column} > TIMESTAMP '{cutoff_ts}'"
        print(f"Incremental load: watermark > {cutoff_ts}")
    else:
        cutoff_date = datetime.utcnow() - timedelta(days=watermark_days)
        cutoff_ts = cutoff_date.strftime("%Y-%m-%d %H:%M:%S")
        watermark_filter = f"{watermark_column} >= TIMESTAMP '{cutoff_ts}'"
        print(f"Initial load: watermark >= {cutoff_ts} (last {watermark_days} days)")

    # Get partition bounds for parallelism
    print(f"Calculating partition bounds...")
    bounds = get_filtered_partition_bounds(
        timed_url,
        schema_name,
        table_name,
        partition_column,
        watermark_column,
        cutoff_ts,
    )

    if not bounds:
        print(f"No new data for {table_name}, skipping")
        return {
            "table": table_name,
            "status": "skipped",
            "reason": "no_new_data",
            "rows": 0,
        }

    lower_bound, upper_bound = bounds
    num_partitions = calculate_num_partitions(lower_bound, upper_bound)

    print(f"Partition bounds: [{lower_bound}, {upper_bound}]")
    print(f"Range size: {upper_bound - lower_bound:,}")
    print(f"Number of partitions: {num_partitions}")

    # Build JDBC read query
    query = (
        f"(SELECT * FROM {schema_name}.{table_name} "
        f"WHERE {watermark_filter}) AS src"
    )

    # Read from PostgreSQL with parallelism
    print(f"Reading from PostgreSQL...")
    start_time = datetime.now()

    df = (
        spark.read.format("jdbc")
        .option("url", timed_url)
        .option("dbtable", query)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .option("queryTimeout", "0")
        .option("sessionInitStatement", "SET statement_timeout = 0")
        .option("partitionColumn", partition_column)
        .option("lowerBound", str(lower_bound))
        .option("upperBound", str(upper_bound))
        .option("numPartitions", str(num_partitions))
        .load()
    )

    # Add ingestion metadata
    df = df.withColumn("_ingestion_ts", current_timestamp())
    df = df.withColumn(
        "_ingestion_batch", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))
    )

    # Write to Delta Bronze layer (append mode for incremental)
    print(f"Writing to Delta Bronze: {target_table}")
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        target_table
    )

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    row_count = df.count()

    print(f"\n✅ Successfully ingested {table_name}")
    print(f"   Rows: {row_count:,}")
    print(f"   Duration: {duration:.2f}s")
    print(f"   Throughput: {row_count/duration:.0f} rows/s\n")

    return {
        "table": table_name,
        "status": "success",
        "rows": row_count,
        "duration_seconds": duration,
        "watermark_cutoff": cutoff_ts,
    }


# COMMAND ----------
# Main execution
if __name__ == "__main__":
    print(f"\n{'#'*80}")
    print(f"# Heavy Tables Ingestion Job")
    print(f"# Environment: {ENVIRONMENT}")
    print(f"# Source Catalog: {SOURCE_CATALOG}")
    print(f"# Config: tables_vcn_public.yaml (filtering heavy: true)")
    print(f"# Tables to ingest: {len(HEAVY_TABLES)}")
    print(f"{'#'*80}\n")

    results = []

    for table_conf in HEAVY_TABLES:
        try:
            result = ingest_heavy_table(table_conf)
            results.append(result)
        except Exception as e:
            print(f"\n❌ ERROR ingesting {table_conf['name']}: {str(e)}\n")
            results.append(
                {"table": table_conf["name"], "status": "failed", "error": str(e)}
            )
            # Continue with other tables
            continue

    # Summary
    print(f"\n{'='*80}")
    print("INGESTION SUMMARY")
    print(f"{'='*80}")

    success_count = sum(1 for r in results if r["status"] == "success")
    failed_count = sum(1 for r in results if r["status"] == "failed")
    skipped_count = sum(1 for r in results if r["status"] == "skipped")
    total_rows = sum(r.get("rows", 0) for r in results if r["status"] == "success")

    print(f"Total tables: {len(results)}")
    print(f"  ✅ Success: {success_count}")
    print(f"  ❌ Failed: {failed_count}")
    print(f"  ⏭️  Skipped: {skipped_count}")
    print(f"Total rows ingested: {total_rows:,}")
    print(f"{'='*80}\n")

    # Store results for monitoring
    from pyspark.sql import Row

    results_df = spark.createDataFrame([Row(**r) for r in results])
    results_df.show(truncate=False)

    if failed_count > 0:
        raise RuntimeError(f"{failed_count} table(s) failed to ingest")
