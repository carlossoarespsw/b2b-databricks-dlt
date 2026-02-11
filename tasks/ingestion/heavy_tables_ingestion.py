# Databricks notebook source
# MAGIC %md
# MAGIC # Heavy Tables Ingestion
# MAGIC 
# MAGIC Notebook para ingestão de tabelas pesadas do PostgreSQL para o RAW Catalog usando JDBC otimizado.
# MAGIC 
# MAGIC **Fluxo:**
# MAGIC 1. Leitura paralela via JDBC com particionamento
# MAGIC 2. Carga incremental baseada em watermark
# MAGIC 3. Escrita em formato Delta no RAW catalog

# COMMAND ----------

import yaml
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup e Configuração

# COMMAND ----------

# Recebe o ambiente via widget ou argumento
dbutils.widgets.text("env", "dev")
ENVIRONMENT = dbutils.widgets.get("env").lower()
CONFIG_PATH = f"/Workspace/Repos/sp_b2b_ops_bot/b2b-databricks-dlt-{ENVIRONMENT}/config/tables_vcn_b2b.yaml"

# Carregar Config
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

SOURCE_CATALOG = config["settings"]["source_catalog"]
RAW_CATALOG = config["settings"]["raw_catalog_pattern"].format(env=ENVIRONMENT)

print(f"✅ Configuração carregada:")
print(f"   📍 Ambiente: {ENVIRONMENT}")
print(f"   📂 Source: {SOURCE_CATALOG}")
print(f"   📂 Target: {RAW_CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Funções Auxiliares

# COMMAND ----------

def get_jdbc_params(source_catalog):
    """Recupera URL e credenciais da conexão federada do Unity Catalog"""
    # Truque: Usamos as configurações do Spark para pegar a URL oculta da federação
    try:
        url = spark.conf.get(f"spark.databricks.sql.federation.catalog.{source_catalog}.url")
        # Adiciona timeouts agressivos para evitar lock no banco
        return f"{url}&socketTimeout=0&connectTimeout=30"
    except Exception as e:
        print(f"Erro ao obter URL JDBC: {e}")
        raise e

# COMMAND ----------

def get_max_watermark(target_table, watermark_col):
    """Descobre até onde já carregamos na RAW para fazer carga incremental"""
    try:
        return spark.read.table(target_table).agg(F.max(watermark_col)).collect()[0][0]
    except (AnalysisException, Exception):
        return None # Tabela não existe ou está vazia

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Função Principal de Ingestão

# COMMAND ----------

def ingest_table(table_conf):
    table_name = table_conf["name"]
    schema = table_conf["schema"]
    watermark_col = table_conf.get("watermark")
    pk_col = table_conf.get("jdbc_partition_column", table_conf.get("pk"))
    
    # Destino: catalogo_raw.schema.tabela
    target_table = f"{RAW_CATALOG}.{schema}.{table_name}"
    
    print(f"🚀 Iniciando ingestão HEAVY: {table_name}")
    
    # 1. Definição do Escopo (Incremental ou Full?)
    last_wm = get_max_watermark(target_table, watermark_col)
    
    # Query Base (Pushdown Predicate)
    # Importante: Filtramos na FONTE para não trafegar dados inúteis
    if last_wm:
        print(f"   🔄 Incremental: Buscando dados > {last_wm}")
        dbtable_query = f"(SELECT * FROM {schema}.{table_name} WHERE {watermark_col} > '{last_wm}') as subq"
    else:
        print(f"   🆕 Carga Inicial (Full Load)")
        dbtable_query = f"{schema}.{table_name}"

    # 2. Configuração JDBC Otimizada
    jdbc_url = get_jdbc_params(SOURCE_CATALOG)
    
    # Precisamos dos limites para o particionamento funcionar
    # Isso faz o Spark abrir N conexões paralelas baseadas no ID
    bounds = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"(SELECT min({pk_col}) as min_id, max({pk_col}) as max_id FROM {schema}.{table_name}) as b") \
        .option("driver", "org.postgresql.Driver") \
        .load().collect()[0]
    
    lower_bound, upper_bound = bounds.min_id, bounds.max_id

    if lower_bound is None:
        print("   ⚠️ Tabela vazia na origem. Pulando.")
        return

    # 3. Leitura Paralela
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", dbtable_query) \
        .option("driver", "org.postgresql.Driver") \
        .option("partitionColumn", pk_col) \
        .option("lowerBound", lower_bound) \
        .option("upperBound", upper_bound) \
        .option("numPartitions", table_conf.get("jdbc_num_partitions", 10)) \
        .option("fetchsize", table_conf.get("jdbc_fetch_size", 10000)) \
        .load()

    # 4. Tratamento de Tipos (Blindagem)
    # Converte tudo que é string/varchar para StringType do Spark (sem limite)
    for field in df.schema.fields:
        if "StringType" in str(field.dataType):
            df = df.withColumn(field.name, F.col(field.name).cast(StringType()))

    # Metadados de Ingestão
    df = df.withColumn("_ingestion_ts", F.current_timestamp())

    # 5. Escrita na RAW (Append Only)
    # A Raw é um log histórico. O DLT que lute para deduplicar depois.
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)
    
    print(f"   ✅ Sucesso: {table_name} salvo em {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execução - Processamento de Tabelas Heavy

# COMMAND ----------

# Filtra apenas tabelas marcadas como heavy
heavy_tables = [t for t in config["tables"] if t.get("heavy") is True]

if not heavy_tables:
    print("Nenhuma tabela HEAVY encontrada no YAML.")
else:
    for t in heavy_tables:
        ingest_table(t)