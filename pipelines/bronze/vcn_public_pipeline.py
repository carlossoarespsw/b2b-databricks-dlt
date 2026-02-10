# Databricks notebook source
import dlt
import yaml
from datetime import date, datetime, timedelta
from pyspark.sql.functions import col, current_timestamp, max as spark_max, lit
from pyspark.sql.types import StringType
import os


# COMMAND ----------
DEV_SAMPLE_LIMIT = 100_000
ENVIRONMENT = spark.conf.get("pipeline.env", "dev").lower()
PIPELINE_LAYER = spark.conf.get("pipeline.layer", "bronze").lower()
ENV_LABEL = "gold" if ENVIRONMENT == "prod" else ENVIRONMENT
SOURCE_SYSTEM = "VCN_PUBLIC"
DEFAULT_NUM_PARTITIONS = 8

CATALOG_PUBLIC = spark.conf.get("catalog_public", f"{ENVIRONMENT}_vcn_public")
CATALOG_FINANCIAL = spark.conf.get("catalog_financial", f"{ENVIRONMENT}_vcn_financial")


# COMMAND ----------
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
TABLES = config.get("tables", [])
DESCRIPTIONS = config.get("descriptions", {})


# COMMAND ----------
def get_jdbc_url(catalog_name):
    try:
        return spark.conf.get(
            f"spark.databricks.sql.federation.catalog.{catalog_name}.url"
        )
    except:
        return None


def with_jdbc_timeouts(jdbc_url):
    if "?" in jdbc_url:
        return f"{jdbc_url}&socketTimeout=0&connectTimeout=10"
    return f"{jdbc_url}?socketTimeout=0&connectTimeout=10"


def generate_time_windows(start_date, end_date, step_days=30):
    windows = []
    current = start_date
    while current < end_date:
        next_date = current + timedelta(days=step_days)
        windows.append((current, min(next_date, end_date)))
        current = next_date
    return windows


def get_partition_bounds(jdbc_url, schema_name, table_name, partition_column):
    bounds_query = (
        f'(SELECT MIN("{partition_column}") AS min_v, '
        f'MAX("{partition_column}") AS max_v FROM {schema_name}.{table_name}) AS bounds'
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
    if row is None:
        return None

    min_v = row["min_v"]
    max_v = row["max_v"]
    if min_v is None or max_v is None:
        return None
    if min_v == max_v:
        return None

    if isinstance(min_v, (int, float)) and isinstance(max_v, (int, float)):
        return int(min_v), int(max_v)

    if isinstance(min_v, (datetime, date)) and isinstance(max_v, (datetime, date)):
        return min_v, max_v

    return None


def get_filtered_partition_bounds(
    jdbc_url,
    schema_name,
    table_name,
    partition_column,
    watermark_column,
    cutoff_ts,
):
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
    if row is None:
        return None

    min_v = row["min_v"]
    max_v = row["max_v"]
    if min_v is None or max_v is None:
        return None
    if min_v == max_v:
        return None

    if isinstance(min_v, (int, float)) and isinstance(max_v, (int, float)):
        return int(min_v), int(max_v)

    if isinstance(min_v, (datetime, date)) and isinstance(max_v, (datetime, date)):
        return min_v, max_v

    return None


def format_timestamp(value):
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def get_last_loaded_watermark(target_table, watermark_column):
    try:
        df = spark.read.table(target_table)
        row = df.select(spark_max(watermark_column).alias("wm")).collect()[0]
        return row["wm"]
    except Exception:
        return None


def resolve_target_catalog(schema_name: str) -> str:
    if schema_name == "public":
        return CATALOG_PUBLIC
    if schema_name in ("financial", "financial_manager"):
        return CATALOG_FINANCIAL
    return f"{schema_name}_vcn_{ENVIRONMENT}"


def apply_column_comments(df, columns_desc):
    if not columns_desc:
        return df
    return df.select(
        [
            (
                col(c).alias(c, metadata={"comment": columns_desc[c]})
                if c in columns_desc
                else col(c)
            )
            for c in df.columns
        ]
    )


def ingest_heavy_table(table_conf: dict) -> None:
    schema_name = table_conf["schema"]
    table_name = table_conf["name"]
    partition_column = table_conf.get("partition_column") or table_conf.get("pk")
    watermark_column = table_conf.get("watermark")
    watermark_days = int(table_conf.get("watermark_days", 180))

    target_catalog = resolve_target_catalog(schema_name)
    target_table = f"{target_catalog}.raw.{table_name}"

    print(f"\n{'='*80}")
    print(f"Starting heavy ingestion: {table_name}")
    print(f"Target RAW: {target_table}")
    print(f"Watermark column: {watermark_column}")
    print(f"Partition column: {partition_column}")
    print(f"{'='*80}\n")

    jdbc_url = get_jdbc_url(SOURCE_CATALOG)
    if not jdbc_url:
        raise RuntimeError(f"JDBC URL not found for source catalog {SOURCE_CATALOG}")

    timed_url = with_jdbc_timeouts(jdbc_url)
    last_wm = get_last_loaded_watermark(target_table, watermark_column)

    if last_wm:
        cutoff_ts = format_timestamp(last_wm)
        watermark_filter = f"{watermark_column} > TIMESTAMP '{cutoff_ts}'"
        print(f"Incremental load: watermark > {cutoff_ts}")
    else:
        cutoff_ts = (datetime.utcnow() - timedelta(days=watermark_days)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        watermark_filter = f"{watermark_column} >= TIMESTAMP '{cutoff_ts}'"
        print(f"Initial load: watermark >= {cutoff_ts} (last {watermark_days} days)")

    print("Calculating partition bounds...")
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
        return

    lower_bound, upper_bound = bounds
    range_size = max(1, int(upper_bound) - int(lower_bound))
    base_parts = max(4, int(spark.sparkContext.defaultParallelism // 2))
    num_parts = min(base_parts, max(1, range_size // 1_000_000))

    print(f"Partition bounds: [{lower_bound}, {upper_bound}]")
    print(f"Range size: {upper_bound - lower_bound:,}")
    print(f"Number of partitions: {num_parts}")

    query = (
        f"(SELECT * FROM {schema_name}.{table_name} "
        f"WHERE {watermark_filter}) AS src"
    )

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
        .option("numPartitions", str(num_parts))
        .load()
    )

    df = df.withColumn("_ingestion_ts", current_timestamp())
    df = df.withColumn(
        "_ingestion_batch", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))
    )

    print(f"Writing to Delta RAW: {target_table}")
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        target_table
    )



def create_table(table_conf):
    schema_name = table_conf["schema"]
    table_name = table_conf["name"]
    is_heavy = bool(table_conf.get("heavy", False))

    if is_heavy:
        source_fqn = f"{CATALOG_PUBLIC}.raw.{table_name}"
        print(f"HEAVY table {table_name} will be read from RAW: {source_fqn}")
    else:
        source_fqn = f"`{SOURCE_CATALOG}`.`{schema_name}`.`{table_name}`"
        print(f"Normal table {table_name} will be read from Federated: {source_fqn}")
    target_catalog = resolve_target_catalog(schema_name)
    target_schema = PIPELINE_LAYER
    target_table = f"{target_catalog}.{target_schema}.{table_name}"
    table_meta = DESCRIPTIONS.get(table_name, {})
    table_desc = table_meta.get(
        "description",
        f"Full load from {source_fqn} into {target_catalog}.{target_schema}",
    )
    columns_desc = table_meta.get("columns", {})

    @dlt.table(
        name=target_table,
        comment=table_desc,
        table_properties={
            "quality": PIPELINE_LAYER,
            "source_system": SOURCE_SYSTEM,
            "environment": ENVIRONMENT,
        },
    )
    def load_table(source_fqn=source_fqn):
        try:
            df = spark.read.table(source_fqn)
            print(f"Reading source for {table_name} from {source_fqn}")
        except Exception as e:
            raise Exception(f"Failed reading source {source_fqn} -> {str(e)}")

        safe_columns = []
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                safe_columns.append(col(field.name).cast("string").alias(field.name))
            else:
                safe_columns.append(col(field.name))

        df_safe = df.select(*safe_columns)
        df_safe = apply_column_comments(df_safe, columns_desc)

        if ENVIRONMENT == "dev":
            return df_safe.limit(DEV_SAMPLE_LIMIT).withColumn(
                "_ingestion_ts", current_timestamp()
            )

        return df_safe.withColumn("_ingestion_ts", current_timestamp())


# COMMAND ----------
for table_conf in TABLES:
    create_table(table_conf)
