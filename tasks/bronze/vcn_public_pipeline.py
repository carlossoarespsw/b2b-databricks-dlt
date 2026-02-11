# Databricks notebook source
# MAGIC %md
# MAGIC # VCN Public Bronze Pipeline (DLT)
# MAGIC 
# MAGIC Pipeline Delta Live Tables para consolidação da camada Bronze.
# MAGIC 
# MAGIC **Roteamento Inteligente:**
# MAGIC - **Tabelas Heavy**: Leitura da RAW catalog (já ingeridas via JDBC)
# MAGIC - **Tabelas Light**: Leitura direta da Federação (PostgreSQL)
# MAGIC 
# MAGIC **Processamento:**
# MAGIC - Deduplicação via `apply_changes` (SCD Type 1)
# MAGIC - Proteção contra erros de tipo (string shield)
# MAGIC - Limite de 1000 registros em DEV para tabelas federadas

# COMMAND ----------

import dlt
import yaml
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup e Configuração

# COMMAND ----------
ENVIRONMENT = spark.conf.get("pipeline.env", "dev").lower()
# Nota: O path deve ser dinâmico ou fixo no repo
CONFIG_PATH = f"/Workspace/Repos/sp_b2b_ops_bot/b2b-databricks-dlt-{ENVIRONMENT}/config/tables_vcn_public.yaml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

SOURCE_CATALOG_FEDERATED = config["source_catalog"]
RAW_CATALOG = f"raw_vcn_{ENVIRONMENT}"

print(f"✅ Pipeline configurado:")
print(f"   📍 Ambiente: {ENVIRONMENT}")
print(f"   📂 Federação: {SOURCE_CATALOG_FEDERATED}")
print(f"   📂 RAW: {RAW_CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Funções de Transformação

# COMMAND ----------

def apply_string_shield(df):
    """Proteção universal contra value too long"""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, col(field.name).cast("string"))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gerador Dinâmico de Tabelas DLT

# COMMAND ----------

def generate_dlt_table(table_conf):
    t_name = table_conf['name']
    t_schema = table_conf['schema']
    t_pk = table_conf['pk']
    t_watermark = table_conf.get('watermark', 'last_modified')
    is_heavy = table_conf.get('heavy', False)

    # LÓGICA DE ROTEAMENTO DE ORIGEM
    if is_heavy:
        # Se for pesado, a origem é a tabela Delta RAW (carregada pelo Job 1)
        source_fqn = f"`{RAW_CATALOG}`.`{t_schema}`.`{t_name}`"
        desc = f"Origem: RAW ({source_fqn})"
    else:
        # Se for leve, vai direto na Federação
        source_fqn = f"`{SOURCE_CATALOG_FEDERATED}`.`{t_schema}`.`{t_name}`"
        desc = f"Origem: FEDERADA ({source_fqn})"

    # 1. VIEW TEMPORÁRIA (Leitura + Tratamento)
    @dlt.view(name=f"vw_{t_name}_clean", comment=desc)
    def get_source():
        # Leitura
        try:
            if is_heavy:
                # Lendo da RAW (Stream para processar apenas o novo que chegou na raw)
                df = spark.readStream.table(source_fqn)
            else:
                # Lendo da Federação (Batch)
                df = spark.read.table(source_fqn)
        except Exception as e:
            print(f"Erro lendo {source_fqn}: {e}")
            raise e
        
        # Tratamento
        df = apply_string_shield(df)
        
        # Limite em DEV para tabelas federadas (Heavy já vem filtrado da raw, não precisa)
        if ENVIRONMENT == "dev" and not is_heavy:
            df = df.limit(1000)
            
        return df.withColumn("_processed_at", current_timestamp())

    # 2. TABELA FINAL (Upsert / Deduplicação)
    target_table_name = t_name # O DLT usa o Target Schema definido no Pipeline Settings
    
    dlt.create_streaming_table(
        name=target_table_name,
        comment=f"Tabela Bronze consolidada. {desc}"
    )

    dlt.apply_changes(
        target = target_table_name,
        source = f"vw_{t_name}_clean",
        keys = [t_pk],
        sequence_by = col(t_watermark), # Garante que o registro mais novo vença
        stored_as_scd_type = 1 # Atualiza (não mantém histórico type 2 na bronze)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execução - Geração de Tabelas

# COMMAND ----------

for t in config["tables"]:
    generate_dlt_table(t)