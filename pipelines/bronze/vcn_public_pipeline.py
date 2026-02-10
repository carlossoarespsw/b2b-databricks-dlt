# Databricks notebook source
import dlt
import yaml
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType


# COMMAND ----------
# --- CONFIGURAÇÃO ---
# Mantemos 1k em dev para ser instantâneo
DEV_SAMPLE_LIMIT = 100_000
ENVIRONMENT = spark.conf.get("pipeline.env", "dev").lower()
PIPELINE_LAYER = spark.conf.get("pipeline.layer", "bronze").lower()
ENV_LABEL = "gold" if ENVIRONMENT == "prod" else ENVIRONMENT
SOURCE_SYSTEM = "VCN_PUBLIC"
DEFAULT_NUM_PARTITIONS = 8

CATALOG_PUBLIC = spark.conf.get("catalog_public", f"public_vcn_{ENVIRONMENT}")
CATALOG_FINANCIAL = spark.conf.get("catalog_financial", f"financial_vcn_{ENVIRONMENT}")


# COMMAND ----------
# Carrega YAML (código padrão que já tínhamos)
try:
    import os
    config_path = f"{os.getcwd()}/config/tables_vcn_public.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
except:
    # Caminho de fallback se necessário
    config_path = f"/Workspace/Repos/sp_b2b_ops_bot/b2b-databricks-dlt-{ENVIRONMENT}/config/tables_vcn_public.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

SOURCE_CATALOG = config["source_catalog"]
TABLES = config.get("tables", [])
DESCRIPTIONS = config.get("descriptions", {})


# COMMAND ----------
# Função auxiliar para pegar URL do Catalog (O truque do Databricks)
def get_jdbc_url(catalog_name):
    try:
        return spark.conf.get(f"spark.databricks.sql.federation.catalog.{catalog_name}.url")
    except:
        return None

def with_jdbc_timeouts(jdbc_url):
    if "?" in jdbc_url:
        return f"{jdbc_url}&socketTimeout=0&connectTimeout=10"
    return f"{jdbc_url}?socketTimeout=0&connectTimeout=10"

def get_partition_bounds(jdbc_url, schema_name, table_name, pk_column):
    bounds_query = (
        f"(SELECT MIN(\"{pk_column}\") AS min_id, "
        f"MAX(\"{pk_column}\") AS max_id FROM {schema_name}.{table_name}) AS bounds"
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

    min_id = row["min_id"]
    max_id = row["max_id"]
    if not isinstance(min_id, (int, float)) or not isinstance(max_id, (int, float)):
        return None
    if min_id == max_id:
        return None

    return int(min_id), int(max_id)

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
            col(c).alias(c, metadata={"comment": columns_desc[c]}) if c in columns_desc else col(c)
            for c in df.columns
        ]
    )


def create_table(table_conf):
    schema_name = table_conf["schema"]
    table_name = table_conf["name"]
    pk_column = table_conf.get("pk")
    source_fqn = f"`{SOURCE_CATALOG}`.`{schema_name}`.`{table_name}`"
    target_catalog = resolve_target_catalog(schema_name)
    target_schema = PIPELINE_LAYER
    target_table = f"{target_catalog}.{target_schema}.{table_name}"
    table_meta = DESCRIPTIONS.get(table_name, {})
    table_desc = table_meta.get("description", f"Full load from {source_fqn} into {target_catalog}.{target_schema}")
    columns_desc = table_meta.get("columns", {})

    @dlt.table(
        name=target_table,
        comment=table_desc,
        table_properties={
            "quality": PIPELINE_LAYER,
            "source_system": SOURCE_SYSTEM,
            "environment": ENVIRONMENT
        }
    )
    def load_table(source_fqn=source_fqn):
        # 1. TENTATIVA INTELIGENTE (Híbrida)
        # Tenta ler via JDBC direto para injetar opções de Timeout
        # Isso evita o erro "canceling statement due to statement timeout"
        
        jdbc_url = get_jdbc_url(SOURCE_CATALOG)

        if jdbc_url:
            try:
                timed_url = with_jdbc_timeouts(jdbc_url)
                options = {
                    "url": timed_url,
                    "dbtable": f"{schema_name}.{table_name}",
                    "driver": "org.postgresql.Driver",
                    "fetchsize": "10000",
                    "queryTimeout": "0",
                }

                bounds = None
                if pk_column:
                    bounds = get_partition_bounds(timed_url, schema_name, table_name, pk_column)

                if bounds:
                    lower_bound, upper_bound = bounds
                    options.update(
                        {
                            "partitionColumn": pk_column,
                            "lowerBound": str(lower_bound),
                            "upperBound": str(upper_bound),
                            "numPartitions": str(DEFAULT_NUM_PARTITIONS),
                        }
                    )

                print(f"Usando JDBC para {table_name}")
                df = spark.read.format("jdbc").options(**options).load()
            except Exception as e:
                print(f"Falha no JDBC para {table_name}. Erro: {str(e)}")
                df = spark.read.table(source_fqn)
        else:
            df = spark.read.table(source_fqn)

        # 2. BLINDAGEM DE STRING (Resolve o erro "value too long")
        safe_columns = []
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                safe_columns.append(col(field.name).cast("string").alias(field.name))
            else:
                safe_columns.append(col(field.name))
        
        df_safe = df.select(*safe_columns)
        df_safe = apply_column_comments(df_safe, columns_desc)

        # 3. LIMIT PARA DEV
        if ENVIRONMENT == "dev":
             return df_safe.limit(DEV_SAMPLE_LIMIT).withColumn("_ingestion_ts", current_timestamp())

        return df_safe.withColumn("_ingestion_ts", current_timestamp())
    


# COMMAND ----------
# Inicializa as tabelas
for table_conf in TABLES:
    create_table(table_conf)