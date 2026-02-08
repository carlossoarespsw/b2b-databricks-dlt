# Databricks notebook source
import dlt
import yaml
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType

# COMMAND ----------
# Caminho correto no DLT (Repo)
config_path = "/Workspace/Repos/sp_b2b_ops_bot/b2b-databricks-dlt-dev/config/tables_vcn_b2b.yaml"

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

SOURCE_CATALOG = config["source_catalog"]
TABLES = config.get("tables", [])

ENVIRONMENT = spark.conf.get("pipeline.env", "dev")
SOURCE_SYSTEM = "VCN_B2B"

# COMMAND ----------
def create_table(table_conf):
    schema_name = table_conf["schema"]
    table_name = table_conf["name"]

    source_fqn = f"`{SOURCE_CATALOG}`.`{schema_name}`.`{table_name}`"

    @dlt.table(
        name=table_name,
        comment=f"Full load from {source_fqn}",
        table_properties={
            "quality": "bronze",
            "source_system": str(SOURCE_SYSTEM) if SOURCE_SYSTEM is not None else "",
            "environment": str(ENVIRONMENT) if ENVIRONMENT is not None else "",
        },
    )
    def load_table(source_fqn=source_fqn):
        df = spark.read.table(source_fqn)
        
        # Varre todas as colunas. Se for texto (Varchar/Char/String), força cast para STRING ilimitado.
        # Se for número, data, etc., mantém como está.
        safe_columns = []
        
        for field in df.schema.fields:
            # Verifica se o tipo de dado é baseado em texto
            if isinstance(field.dataType, StringType):
                # Força o cast para ignorar limites (ex: varchar(64) -> string)
                safe_columns.append(col(field.name).cast("string").alias(field.name))
            else:
                # Mantém a coluna original
                safe_columns.append(col(field.name))
        
        # Aplica a seleção segura
        df_safe = df.select(*safe_columns)

        # 3. Adiciona metadados de ingestão
        return df_safe.withColumn("_ingestion_ts", current_timestamp())

# COMMAND ----------

for t in TABLES:
    create_table(t)