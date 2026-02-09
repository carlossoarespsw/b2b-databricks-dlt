import dlt
import yaml
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType

# --- CONFIGURAÇÃO ---
# Garantimos que este código só execute a lógica incremental se o ambiente for PROD
ENVIRONMENT = spark.conf.get("pipeline.env", "prod").lower()

# Carregamento do YAML consolidado (com PK e Watermark)
config_path = f"/Workspace/Repos/sp_b2b_ops_bot/b2b-databricks-dlt-prod/config/tables_vcn_b2b.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

SOURCE_CATALOG = config["source_catalog"]

def apply_string_shield(df):
    """Resolve o problema de nvarchar(64) convertendo para STRING ilimitada."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, col(field.name).cast("string"))
    return df

# --- MOTOR DE CARGA INCREMENTAL ---
for table_conf in config['tables']:
    t_name = table_conf['name']
    t_schema = table_conf['schema']
    t_pk = table_conf['pk'] # Baseado no seu VCN_PKS.txt 
    t_watermark = table_conf.get('watermark', 'last_modified')
    
    source_fqn = f"`{SOURCE_CATALOG}`.`{t_schema}`.`{t_name}`"

    # 1. Criamos uma VIEW de leitura (Blindada contra erros de nvarchar)
    @dlt.view(name=f"{t_name}_incremental_source")
    def get_source(fqn=source_fqn):
        df = spark.read.table(fqn)
        # Aplica a blindagem para garantir que o 'value too long' não ocorra
        df = apply_string_shield(df)
        return df.withColumn("_ingestion_ts", current_timestamp())

    # 2. Criamos a TABELA de destino com APPLY CHANGES (Upsert)
    # Isso garante que apenas as linhas com watermark novo sejam processadas
    dlt.create_streaming_table(
        name=t_name,
        comment=f"Carga incremental de {t_name} via {t_watermark}"
    )

    dlt.apply_changes(
        target = t_name,
        source = f"{t_name}_incremental_source",
        keys = [t_pk],
        sequence_by = col(t_watermark), # Usa a marca d'água para ordenar as atualizações
        stored_as_scd_type = 1 # Sobrescreve registros antigos (Upsert)
    )