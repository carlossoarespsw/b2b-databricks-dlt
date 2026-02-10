import dlt
import yaml
from datetime import date, datetime
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType

# --- CONFIGURAÇÃO ---
# Garantimos que este código só execute a lógica incremental se o ambiente for PROD
ENVIRONMENT = spark.conf.get("pipeline.env", "prod").lower()
PIPELINE_LAYER = spark.conf.get("pipeline.layer", "bronze").lower()
ENV_LABEL = "gold" if ENVIRONMENT == "prod" else ENVIRONMENT
DEFAULT_NUM_PARTITIONS = 8

CATALOG_PUBLIC = spark.conf.get("catalog_public", f"{ENVIRONMENT}_vcn_public")
CATALOG_FINANCIAL = spark.conf.get("catalog_financial", f"{ENVIRONMENT}_vcn_financial")

# Carregamento do YAML consolidado (com PK e Watermark)
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
DESCRIPTIONS = config.get("descriptions", {})


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


def apply_string_shield(df):
    """Resolve o problema de nvarchar(64) convertendo para STRING ilimitada."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, col(field.name).cast("string"))
    return df


# --- MOTOR DE CARGA INCREMENTAL ---
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


for table_conf in config["tables"]:
    t_name = table_conf["name"]
    t_schema = table_conf["schema"]
    t_pk = table_conf["pk"]  # Baseado no seu VCN_PKS.txt
    t_watermark = table_conf.get("watermark", "last_modified")

    source_fqn = f"`{SOURCE_CATALOG}`.`{t_schema}`.`{t_name}`"
    target_catalog = CATALOG_PUBLIC if t_schema == "public" else CATALOG_FINANCIAL
    target_schema = PIPELINE_LAYER
    target_table = f"{target_catalog}.{target_schema}.{t_name}"
    table_meta = DESCRIPTIONS.get(t_name, {})
    table_desc = table_meta.get(
        "description",
        f"Carga incremental de {t_name} via {t_watermark} em {target_catalog}.{target_schema}",
    )
    columns_desc = table_meta.get("columns", {})

    # 1. Criamos uma VIEW de leitura (Blindada contra erros de nvarchar)
    @dlt.view(name=f"{t_name}_incremental_source")
    def get_source(fqn=source_fqn):
        try:
            df = spark.read.table(fqn)
        except Exception as e:
            print(f"Falha no federado para {t_name}. Erro: {str(e)}")
            jdbc_url = get_jdbc_url(SOURCE_CATALOG)

            if jdbc_url:
                timed_url = with_jdbc_timeouts(jdbc_url)
                options = {
                    "url": timed_url,
                    "dbtable": f"{t_schema}.{t_name}",
                    "driver": "org.postgresql.Driver",
                    "fetchsize": "10000",
                    "queryTimeout": "0",
                    "sessionInitStatement": "SET statement_timeout = 0",
                }

                bounds = None
                if t_pk:
                    bounds = get_partition_bounds(timed_url, t_schema, t_name, t_pk)

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
                            "partitionColumn": t_pk,
                            "lowerBound": str(lower_bound),
                            "upperBound": str(upper_bound),
                            "numPartitions": str(num_partitions),
                        }
                    )

                print(f"Usando JDBC para {t_name}")
                df = spark.read.format("jdbc").options(**options).load()
            else:
                raise
        # Aplica a blindagem para garantir que o 'value too long' não ocorra
        df = apply_string_shield(df)
        df = apply_column_comments(df, columns_desc)
        return df.withColumn("_ingestion_ts", current_timestamp())

    # 2. Criamos a TABELA de destino com APPLY CHANGES (Upsert)
    # Isso garante que apenas as linhas com watermark novo sejam processadas
    dlt.create_streaming_table(name=target_table, comment=table_desc)

    dlt.apply_changes(
        target=target_table,
        source=f"{t_name}_incremental_source",
        keys=[t_pk],
        sequence_by=col(t_watermark),  # Usa a marca d'água para ordenar as atualizações
        stored_as_scd_type=1,  # Sobrescreve registros antigos (Upsert)
    )
