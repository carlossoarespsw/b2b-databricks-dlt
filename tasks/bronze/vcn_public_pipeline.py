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
import os
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType
from pyspark.sql.functions import year

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup e Configuração

# COMMAND ----------
ENVIRONMENT = spark.conf.get("pipeline.env", "dev").lower()
CONFIG_PATH = f"/Workspace/Repos/sp_b2b_ops_bot/b2b-databricks-dlt-{ENVIRONMENT}/config/tables_vcn_public.yaml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

SOURCE_CATALOG_FEDERATED = config["source_catalog"]
RAW_CATALOG = "landingzone"
RAW_SCHEMA = "raw"

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
    if is_heavy:
        source_fqn = f"`{RAW_CATALOG}`.`{RAW_SCHEMA}`.`{t_name}`"
        desc = f"Origem: RAW ({source_fqn})"
    else:
        source_fqn = f"`{SOURCE_CATALOG_FEDERATED}`.`{t_schema}`.`{t_name}`"
        desc = f"Origem: JDBC Catalog Federado ({source_fqn})"

    @dlt.view(name=f"vw_{t_name}_clean", comment=desc)
    def get_source():
        try:
            if ENVIRONMENT == "dev":
                df = spark.read.table(source_fqn)
                df = apply_string_shield(df)
                df = df.limit(100000)
                return df.withColumn("_processed_at", current_timestamp())
            else:
                from pyspark.sql.functions import month, year
                if is_heavy:
                    months = [row[0] for row in spark.read.table(source_fqn).select(month(col(t_watermark))).distinct().collect()]
                    df_all = None
                    for m in months:
                        df_month = spark.read.table(source_fqn).filter(month(col(t_watermark)) == m)
                        df_month = apply_string_shield(df_month)
                        df_month = df_month.withColumn("_processed_at", current_timestamp())
                        if df_all is None:
                            df_all = df_month
                        else:
                            df_all = df_all.unionByName(df_month)
                    return df_all
                else:
                    years = [row[0] for row in spark.read.table(source_fqn).select(year(col(t_watermark))).distinct().collect()]
                    df_all = None
                    for y in years:
                        df_year = spark.read.table(source_fqn).filter(year(col(t_watermark)) == y)
                        df_year = apply_string_shield(df_year)
                        df_year = df_year.withColumn("_processed_at", current_timestamp())
                        if df_all is None:
                            df_all = df_year
                        else:
                            df_all = df_all.unionByName(df_year)
                    return df_all
        except Exception as e:
            print(f"Erro final lendo {source_fqn}: {e}")
            raise e

    dlt.apply_changes(
        target = t_name,
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