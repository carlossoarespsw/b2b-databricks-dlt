# Databricks notebook source
import dlt
import yaml
from datetime import date, datetime, timedelta
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType


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


# COMMAND ----------


def create_table(table_conf):
    schema_name = table_conf["schema"]
    table_name = table_conf["name"]
    partition_column = table_conf.get("partition_column") or table_conf.get("pk")
    watermark_column = table_conf.get("watermark")
    is_heavy = bool(table_conf.get("heavy", False))
    source_fqn = f"`{SOURCE_CATALOG}`.`{schema_name}`.`{table_name}`"
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
        except Exception as e:
            print(f"Falha no federado para {table_name}. Erro: {str(e)}")
            jdbc_url = get_jdbc_url(SOURCE_CATALOG)

            if jdbc_url:
                try:
                    timed_url = with_jdbc_timeouts(jdbc_url)
                    base_options = {
                        "url": timed_url,
                        "driver": "org.postgresql.Driver",
                        "fetchsize": "10000",
                        "queryTimeout": "0",
                        "sessionInitStatement": "SET statement_timeout = 0",
                    }

                    if is_heavy and watermark_column:
                        print(f"Lendo tabela HEAVY {table_name} por janelas de tempo")
                        start_date = datetime(2000, 1, 1)
                        end_date = datetime.today()
                        max_parallel = 12
                        total_days = max(1, (end_date - start_date).days)
                        step_days = max(30, total_days // max_parallel)
                        windows = generate_time_windows(start_date, end_date, step_days)
                        predicates = [
                            f"{watermark_column} >= '{start}' AND {watermark_column} < '{end}'"
                            for start, end in windows
                        ]

                        df = (
                            spark.read.format("jdbc")
                            .options(**base_options)
                            .option("dbtable", f"{schema_name}.{table_name}")
                            .option("numPartitions", len(predicates))
                            .option("partitionColumn", watermark_column)
                            .option("lowerBound", "0")
                            .option("upperBound", "1")
                            .option("predicates", predicates)
                            .load()
                        )
                    else:
                        options = base_options.copy()
                        options["dbtable"] = f"{schema_name}.{table_name}"
                        if partition_column:
                            options.update(
                                {
                                    "partitionColumn": partition_column,
                                    "lowerBound": "0",
                                    "upperBound": "1000000000",
                                    "numPartitions": str(DEFAULT_NUM_PARTITIONS),
                                }
                            )

                        print(f"Usando JDBC para {table_name}")
                        df = spark.read.format("jdbc").options(**options).load()
                except Exception as jdbc_error:
                    print(f"Falha no JDBC para {table_name}. Erro: {str(jdbc_error)}")
                    raise
            else:
                raise

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
