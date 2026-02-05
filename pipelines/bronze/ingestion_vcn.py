# Databricks notebook source

import dlt
from pyspark.sql.functions import current_timestamp, lit

ENVIRONMENT = spark.conf.get("environment", "dev")
SOURCE_CATALOG = "vcn-federated"
SOURCE_SCHEMA = "public"
DEST_CATALOG = "development"
DEST_SCHEMA = "vcn_bronze"
RECORD_LIMIT = 1000000
SOURCE_SYSTEM = "VCN"

# COMMAND ----------

if ENVIRONMENT == "dev":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DEST_CATALOG}.{DEST_SCHEMA}")
    print(f"[INFO] Schema {DEST_CATALOG}.{DEST_SCHEMA} pronto")

# COMMAND ----------

try:
    tables_query = f"""
    SELECT DISTINCT table_name 
    FROM {SOURCE_CATALOG}.information_schema.tables 
    WHERE table_schema = '{SOURCE_SCHEMA}'
    AND table_type = 'BASE TABLE'
    ORDER BY table_name
    """
    tables_result = spark.sql(tables_query).collect()
    table_names = [row["table_name"] for row in tables_result]
    print(f"[INFO] Ingestão VCN: {len(table_names)} tabelas descobertas de {SOURCE_CATALOG}")
except Exception as e:
    print(f"[AVISO] VCN indisponível: {e}")
    table_names = []

# COMMAND ----------

for table_name in table_names:
    @dlt.table(
        name=f"vcn_{table_name}",
        catalog=DEST_CATALOG,
        schema=DEST_SCHEMA,
        comment=f"VCN: {SOURCE_CATALOG}.{SOURCE_SCHEMA}.{table_name}",
        table_properties={
            "quality": "bronze",
            "source_system": SOURCE_SYSTEM,
            "pipelines.autoOptimize.managed": "true",
            "source_catalog": SOURCE_CATALOG,
            "source_schema": SOURCE_SCHEMA,
            "source_table": table_name,
            "environment": ENVIRONMENT
        }
    )
    def create_bronze_vcn_table(src_catalog=SOURCE_CATALOG, src_schema=SOURCE_SCHEMA, tbl_name=table_name, source_system=SOURCE_SYSTEM):
        source_table = f"{src_catalog}.{src_schema}.{tbl_name}"
        df = spark.read.table(source_table).limit(RECORD_LIMIT)
        
        df = df.withColumn("_ingestion_timestamp", current_timestamp())
        df = df.withColumn("_source_system", lit(source_system))
        df = df.withColumn("_source_catalog", lit(src_catalog))
        df = df.withColumn("_source_schema", lit(src_schema))
        df = df.withColumn("_source_table", lit(tbl_name))
        df = df.withColumn("_environment", lit(ENVIRONMENT))
        
        return df
