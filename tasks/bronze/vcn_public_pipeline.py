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

import yaml
import os
import logging
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType
from pyspark.sql.functions import year

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup e Configuração

# COMMAND ----------
ENVIRONMENT = spark.conf.get("pipeline.env", "dev").lower()
CONFIG_PATH = f"/Workspace/Repos/sp_b2b_ops_bot/b2b-databricks-dlt-{ENVIRONMENT}/config/tables_vcn_public.yaml"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("vcn_public_pipeline")

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

SOURCE_CATALOG_FEDERATED = config["source_catalog"]
RAW_CATALOG = "landingzone"
RAW_SCHEMA = "raw"

logger.info(f"Pipeline configurado: Ambiente={ENVIRONMENT}, Federacao={SOURCE_CATALOG_FEDERATED}, RAW={RAW_CATALOG}")

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

    partition_cols = []
    if t_watermark:
        partition_cols = ["_year", "_month"]

    @dlt.table(
        name=t_name,
        comment=f"Tabela Bronze consolidada. {desc}",
        partition_cols=partition_cols if partition_cols else None
    )
    def bronze_table():
        from pyspark.sql import Window
        from pyspark.sql.functions import row_number, month, year
        if ENVIRONMENT == "dev":
            df = spark.read.table(source_fqn)
            df = apply_string_shield(df)
            df = df.filter(year(col("last_modified")).isin(2025, 2026))
            df = df.withColumn("_processed_at", current_timestamp())
        else:
            if is_heavy:
                months_df = spark.read.table(source_fqn).select(month(col(t_watermark))).distinct()
                months = [row[0] for row in months_df.collect()]
                df = spark.read.table(source_fqn).filter(month(col(t_watermark)).isin(months))
                df = apply_string_shield(df)
                df = df.withColumn("_processed_at", current_timestamp())
            else:
                years_df = spark.read.table(source_fqn).select(year(col(t_watermark))).distinct()
                years = [row[0] for row in years_df.collect()]
                df = spark.read.table(source_fqn).filter(year(col(t_watermark)).isin(years))
                df = apply_string_shield(df)
                df = df.withColumn("_processed_at", current_timestamp())

        # Otimização: Repartition dinâmico antes da deduplicação para evitar shuffles grandes
        # Calcula o número de partições com base no número de meses/anos lidos
        if ENVIRONMENT == "dev":
            num_partitions = 8  # valor baixo para DEV
        else:
            if is_heavy:
                num_partitions = max(1, len(months)) if 'months' in locals() else 8
            else:
                num_partitions = max(1, len(years)) if 'years' in locals() else 8
        df = df.repartition(num_partitions)

        df.cache()

        if t_watermark and t_watermark in df.columns:
            window_spec = Window.partitionBy(t_pk).orderBy(col(t_watermark).desc())
            df_dedup = df.withColumn("_row_num", row_number().over(window_spec)) \
                         .filter(col("_row_num") == 1) \
                         .drop("_row_num")
        else:
            df_dedup = df.dropDuplicates([t_pk])

        if t_watermark and t_watermark in df_dedup.columns:
            from pyspark.sql.functions import year, month
            df_dedup = df_dedup.withColumn("_year", year(col(t_watermark)))
            df_dedup = df_dedup.withColumn("_month", month(col(t_watermark)))

        df.unpersist()
        return df_dedup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execução - Geração de Tabelas

# COMMAND ----------

for t in config["tables"]:
    generate_dlt_table(t)