# Databricks notebook source
import dlt
import yaml
from datetime import date, datetime
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
                    dbtable = f"{schema_name}.{table_name}"

                    options = {
                        "url": timed_url,
                        "dbtable": dbtable,
                        "driver": "org.postgresql.Driver",
                        "fetchsize": "10000",
                        "queryTimeout": "0",
                        "sessionInitStatement": "SET statement_timeout = 0",
                    }

                    bounds = None
                    if partition_column:
                        bounds = get_partition_bounds(
                            timed_url, schema_name, table_name, partition_column
                        )

                    if bounds:
                        lower_bound, upper_bound = bounds
                        num_partitions = min(DEFAULT_NUM_PARTITIONS, 4)
                        if isinstance(lower_bound, (int, float)) and isinstance(
                            upper_bound, (int, float)
                        ):
                            range_size = max(1, int(upper_bound) - int(lower_bound))
                            num_partitions = min(
                                DEFAULT_NUM_PARTITIONS, max(1, range_size // 5_000_000)
                            )

                        options.update(
                            {
                                "partitionColumn": partition_column,
                                "lowerBound": str(lower_bound),
                                "upperBound": str(upper_bound),
                                "numPartitions": str(num_partitions),
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
