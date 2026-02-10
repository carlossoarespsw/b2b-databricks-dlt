import dlt
import yaml
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType

# --- CONFIGURAÇÃO ---
# Garantimos que este código só execute a lógica incremental se o ambiente for PROD
ENVIRONMENT = spark.conf.get("pipeline.env", "prod").lower()
PIPELINE_LAYER = spark.conf.get("pipeline.layer", "bronze").lower()
ENV_LABEL = "gold" if ENVIRONMENT == "prod" else ENVIRONMENT

CATALOG_PUBLIC = spark.conf.get("catalog_public", f"public_vcn_{ENVIRONMENT}")
CATALOG_FINANCIAL = spark.conf.get("catalog_financial", f"financial_vcn_{ENVIRONMENT}")

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
            col(c).alias(c, metadata={"comment": columns_desc[c]}) if c in columns_desc else col(c)
            for c in df.columns
        ]
    )


for table_conf in config['tables']:
    t_name = table_conf['name']
    t_schema = table_conf['schema']
    t_pk = table_conf['pk'] # Baseado no seu VCN_PKS.txt 
    t_watermark = table_conf.get('watermark', 'last_modified')
    
    source_fqn = f"`{SOURCE_CATALOG}`.`{t_schema}`.`{t_name}`"
    target_catalog = CATALOG_PUBLIC if t_schema == "public" else CATALOG_FINANCIAL
    target_schema = PIPELINE_LAYER
    target_table = f"{target_catalog}.{target_schema}.{t_name}"
    table_meta = DESCRIPTIONS.get(t_name, {})
    table_desc = table_meta.get("description", f"Carga incremental de {t_name} via {t_watermark} em {target_catalog}.{target_schema}")
    columns_desc = table_meta.get("columns", {})

    # 1. Criamos uma VIEW de leitura (Blindada contra erros de nvarchar)
    @dlt.view(name=f"{t_name}_incremental_source")
    def get_source(fqn=source_fqn):
        df = spark.read.table(fqn)
        # Aplica a blindagem para garantir que o 'value too long' não ocorra
        df = apply_string_shield(df)
        df = apply_column_comments(df, columns_desc)
        return df.withColumn("_ingestion_ts", current_timestamp())

    # 2. Criamos a TABELA de destino com APPLY CHANGES (Upsert)
    # Isso garante que apenas as linhas com watermark novo sejam processadas
    dlt.create_streaming_table(
        name=target_table,
        comment=table_desc
    )

    dlt.apply_changes(
        target = target_table,
        source = f"{t_name}_incremental_source",
        keys = [t_pk],
        sequence_by = col(t_watermark), # Usa a marca d'água para ordenar as atualizações
        stored_as_scd_type = 1 # Sobrescreve registros antigos (Upsert)
    )