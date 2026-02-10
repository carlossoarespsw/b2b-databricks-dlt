# Databricks notebook source
import yaml
from datetime import datetime, timedelta
from pyspark.sql import functions as F

# =============================
# CONFIG
# =============================

ENVIRONMENT = spark.conf.get("pipeline.env", "dev").lower()
CATALOG_PUBLIC = spark.conf.get("catalog_public", f"{ENVIRONMENT}_vcn_public")
SOURCE_CATALOG = "vcn-federated"  # federated catalog name
RAW_SCHEMA = "raw"

JDBC_FETCHSIZE = 10000
DEFAULT_PARALLELISM = max(4, spark.sparkContext.defaultParallelism)

# =============================
# LOAD YAML
# =============================

config_path = (
    f"/Workspace/Repos/sp_b2b_ops_bot/b2b-databricks-dlt-{ENVIRONMENT}/config/tables_vcn_public.yaml"
)

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

TABLES = [t for t in config.get("tables", []) if t.get("heavy", False)]

print(f"Heavy tables to ingest: {[t['name'] for t in TABLES]}")

# =============================
# JDBC URL FROM FEDERATION
# =============================

jdbc_url = spark.conf.get(
    f"spark.databricks.sql.federation.catalog.{SOURCE_CATALOG}.url"
)

if "?" in jdbc_url:
    jdbc_url = f"{jdbc_url}&socketTimeout=0&connectTimeout=10"
else:
    jdbc_url = f"{jdbc_url}?socketTimeout=0&connectTimeout=10"

base_options = {
    "url": jdbc_url,
    "driver": "org.postgresql.Driver",
    "fetchsize": str(JDBC_FETCHSIZE),
    "queryTimeout": "0",
    "sessionInitStatement": "SET statement_timeout = 0",
}

# =============================
# FUNCTIONS
# =============================


def get_last_watermark(raw_table, watermark_col):
    if not spark.catalog.tableExists(raw_table):
        return None
    return (
        spark.read.table(raw_table)
        .agg(F.max(watermark_col).alias("wm"))
        .collect()[0]["wm"]
    )


def get_bounds(schema, table, column, where_clause=None):
    query = (
        f"(SELECT MIN({column}) AS min_v, MAX({column}) AS max_v FROM {schema}.{table}"
    )
    if where_clause:
        query += f" WHERE {where_clause}"
    query += ") AS bounds"

    row = (
        spark.read.format("jdbc")
        .options(**base_options)
        .option("dbtable", query)
        .load()
        .first()
    )

    if not row or not row["min_v"] or not row["max_v"]:
        return None

    return int(row["min_v"]), int(row["max_v"])


# =============================
# MAIN LOOP
# =============================

for table in TABLES:
    name = table["name"]
    schema = table["schema"]
    pk = table.get("partition_column") or table.get("pk")
    watermark = table.get("watermark")
    watermark_days = int(table.get("watermark_days", 180))

    source_table = f"{schema}.{name}"
    raw_table = f"{CATALOG_PUBLIC}.{RAW_SCHEMA}.{name}"

    print(f"\nIngesting HEAVY table: {source_table}")

    last_wm = get_last_watermark(raw_table, watermark)

    if last_wm:
        cutoff = last_wm.strftime("%Y-%m-%d %H:%M:%S")
        where_filter = f"{watermark} > TIMESTAMP '{cutoff}'"
        print(f"Incremental load from watermark: {cutoff}")
    else:
        cutoff = (datetime.utcnow() - timedelta(days=watermark_days)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        where_filter = f"{watermark} >= TIMESTAMP '{cutoff}'"
        print(f"Initial load from last {watermark_days} days")

    bounds = get_bounds(schema, name, pk, where_filter)

    if not bounds:
        print("No new data found.")
        continue

    lower, upper = bounds
    num_parts = min(DEFAULT_PARALLELISM, max(4, (upper - lower) // 1_000_000))

    query = f"(SELECT * FROM {schema}.{name} WHERE {where_filter}) AS src"

    df = (
        spark.read.format("jdbc")
        .options(**base_options)
        .option("dbtable", query)
        .option("partitionColumn", pk)
        .option("lowerBound", str(lower))
        .option("upperBound", str(upper))
        .option("numPartitions", str(num_parts))
        .load()
    )

    print(f"Writing {df.count()} rows to RAW -> {raw_table}")

    (
        df.withColumn("_ingestion_ts", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .saveAsTable(raw_table)
    )

print("\nHeavy ingestion finished.")
