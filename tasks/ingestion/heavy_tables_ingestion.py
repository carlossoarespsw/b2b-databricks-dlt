# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Ingestão de Tabelas Pesadas (Heavy Ingestion)
# MAGIC **Objetivo:** Migração incremental e histórica de tabelas massivas via Unity Catalog com visualização em tempo real.

# COMMAND ----------
# I. IMPORTS E SETUP VISUAL

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.targetFileSize", "134217728")  # 128MB

import yaml
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from IPython.display import display, HTML
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException


class TableProgressBar:
    """Componente visual para monitoramento do progresso por data."""
    def __init__(self, table_name, start_date, end_date):
        self.table_name = table_name
        self.start_date = start_date
        self.end_date = end_date
        self.current_date = start_date
        self.total_records = 0
        self.display_handle = display(HTML(self._generate_html()), display_id=True)
    
    def _generate_html(self):
        total_days = (self.end_date - self.start_date).days
        processed_days = (self.current_date - self.start_date).days
        percent = int((processed_days / total_days) * 100) if total_days > 0 else 0
        
        return f"""
            <div style="width: 100%; border: 1px solid #ddd; border-radius: 8px; padding: 15px; margin: 10px 0; background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%); box-shadow: 0 4px 6px rgba(0,0,0,0.1); font-family: sans-serif;">
                <h3 style="margin: 0 0 10px 0; color: #2c3e50;">📂 {self.table_name}</h3>
                <div style="background-color: #ecf0f1; border-radius: 10px; height: 25px; width: 100%; position: relative; overflow: hidden;">
                    <div style="background: linear-gradient(90deg, #3498db 0%, #2ecc71 100%); width: {percent}%; height: 25px; border-radius: 10px; transition: width 0.5s ease-in-out;"></div>
                    <div style="position: absolute; top: 0; left: 0; right: 0; bottom: 0; display: flex; align-items: center; justify-content: center; color: #2c3e50; font-weight: bold; font-size: 13px;">{percent}%</div>
                </div>
                <div style="margin-top: 10px; font-size: 13px; color: #34495e;">
                    <div style="display: flex; justify-content: space-between;">
                        <span>📅 Janela: {self.start_date.date()} ➔ {self.end_date.date()}</span>
                        <span>📊 Registros: {self.total_records:,}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; margin-top: 5px;">
                        <span>✅ Processado até: <b>{self.current_date.strftime('%Y-%m-%d')}</b></span>
                        <span>⏳ Faltam: {max(0, (self.end_date - self.current_date).days)} dias</span>
                    </div>
                </div>
            </div>
        """
    
    def update(self, current_date, total_records):
        self.current_date = current_date
        self.total_records = total_records
        self.display_handle.update(HTML(self._generate_html()))

# COMMAND ----------
# II. CONFIGURAÇÃO DE AMBIENTE E PARÂMETROS

ENVIRONMENT = dbutils.widgets.get("environment").lower()
RAW_CATALOG = "landingzone"
RAW_SCHEMA = "raw"

CONFIG_PATH = f"/Workspace/Repos/sp_b2b_ops_bot/b2b-databricks-dlt-{ENVIRONMENT}/config/tables_vcn_public.yaml"

try:
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)
    SOURCE_CATALOG = config["source_catalog"]
    print(f"✅ YAML carregado com sucesso para ambiente: {ENVIRONMENT}")
except Exception as e:
    raise RuntimeError(f"Falha ao carregar configuração em {CONFIG_PATH}: {e}")

# COMMAND ----------
# III. ENGINE DE INGESTÃO COM RESILIÊNCIA
def get_last_ingested_date(target_table, watermark_col):
    """Verifica na RAW o checkpoint para evitar reprocessamento."""
    try:
        max_date = spark.read.table(target_table).agg(F.max(watermark_col)).collect()[0][0]
        return max_date
    except:
        return None

def ingest_batch(table_conf, start_date, end_date):
    """Executa a carga de um intervalo específico (Batch)."""
    t_name = table_conf["name"]
    t_schema = table_conf["schema"]
    t_wm = table_conf["watermark"]
    
    target_table = f"{RAW_CATALOG}.{RAW_SCHEMA}.{t_name}"
    source_fqn = f"`{SOURCE_CATALOG}`.`{t_schema}`.`{t_name}`"
    
    import os
    import math
    from pyspark.sql import DataFrame
    from delta.tables import DeltaTable
    import time as pytime

    # Log: início leitura
    t0 = pytime.time()
    df = spark.read.table(source_fqn).filter(
        (F.col(t_wm) >= start_date) & (F.col(t_wm) < end_date)
    )
    t1 = pytime.time()
    read_time = t1 - t0

    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.col(field.name).cast("string"))

    estimated_rows = df.count()
    if estimated_rows == 0:
        print(f"      ⚠️ Batch vazio. Tempo leitura: {read_time:.2f}s")
        return 0

    num_partitions = min(max(8, estimated_rows // 1_000_000), 128)
    df = df.repartition(num_partitions)

    # PartitionBy: se watermark for data, particione por ano/mes para evitar excesso de partições
    if "date" in t_wm or "created" in t_wm or "last_modified" in t_wm:
        df = df.withColumn("ano", F.year(F.col(t_wm)))
        df = df.withColumn("mes", F.month(F.col(t_wm)))
        partition_cols = ["ano", "mes"]
    else:
        partition_cols = [t_wm]

    df = df.withColumn("_ingestion_ts", F.current_timestamp())

    # Log: início escrita
    t2 = pytime.time()
    df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy(*partition_cols) \
        .saveAsTable(target_table)
    t3 = pytime.time()
    write_time = t3 - t2


    # Número de workers atuais
    try:
        sc = spark.sparkContext
        num_workers = sc._jsc.sc().getExecutorMemoryStatus().size() - 1  # -1 para driver
    except Exception as e:
        num_workers = -1

    print(f"      ⏱️ Tempo leitura: {read_time:.2f}s | escrita: {write_time:.2f}s | partições: {num_partitions} | workers: {num_workers} | linhas: {estimated_rows}")

    return estimated_rows

# COMMAND ----------
# IV. EXECUÇÃO DO LOOP DE MIGRAÇÃO COM CHECKPOINT INTELIGENTE
from datetime import timedelta

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {RAW_CATALOG}.{RAW_SCHEMA}")
    print(f"✅ Schema {RAW_CATALOG}.{RAW_SCHEMA} verificado/criado\n")
except Exception as e:
    print(f"⚠️ Aviso ao criar schema: {e}\n")

heavy_tables = [t for t in config['tables'] if t.get('heavy', False)]

if not heavy_tables:
    print("Nenhuma tabela com flag 'heavy: true' encontrada.")
else:
    print(f"🚀 Iniciando migração de {len(heavy_tables)} tabelas com checkpoint automático\n")
    
    for table_conf in heavy_tables:
        t_name = table_conf["name"]
        t_wm = table_conf["watermark"]
        target_table = f"{RAW_CATALOG}.{RAW_SCHEMA}.{t_name}"
        source_fqn = f"`{SOURCE_CATALOG}`.`{table_conf['schema']}`.`{t_name}`"
        
        print(f"\n{'='*80}")
        print(f"📂 {t_name}")
        print(f"{'='*80}")
        
        last_date = get_last_ingested_date(target_table, t_wm)
        
        if last_date:
            if isinstance(last_date, str):
                start_date = datetime.strptime(last_date[:10], "%Y-%m-%d") + timedelta(days=1)
            else:
                start_date = last_date + timedelta(days=1)
            print(f"🔄 Checkpoint encontrado: {last_date}")
            print(f"   Continuando de: {start_date.date()}")
        else:
            min_date_val = spark.read.table(source_fqn).agg(F.min(t_wm)).collect()[0][0]
            if not min_date_val:
                print(f"⚠️ Tabela vazia na origem. Pulando.")
                continue
            start_date = min_date_val
            print(f"🆕 Iniciando carga histórica completa")
            print(f"   Data inicial na origem: {start_date}")
        
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date[:10], "%Y-%m-%d")
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        current_month_start = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        p_bar = TableProgressBar(t_name, start_date, end_date)
        current_date = start_date
        total_rows = 0
        batch_count = 0
        
        # 2. CARGA MÊS A MÊS até o mês atual
        print(f"\n📆 Fase 1: Carga MENSAL (até {current_month_start.date()})")
        while current_date < current_month_start:
            batch_count += 1
            next_date = (current_date + relativedelta(months=1)).replace(day=1)
            
            if next_date > current_month_start:
                next_date = current_month_start
            
            try:
                print(f"   Batch {batch_count}: {current_date.strftime('%Y-%m')} ({current_date.date()} até {next_date.date()})")
                rows = ingest_batch(table_conf, current_date, next_date)
                total_rows += rows
                p_bar.update(next_date, total_rows)
                print(f"      ✅ {rows:,} registros")
            except Exception as e:
                print(f"      ❌ Erro: {e}")
                print(f"      📌 Checkpoint salvo em: {current_date.date()}")
                break
            
            current_date = next_date
        
        # 3. CARGA DIA A DIA para o mês atual
        if current_date >= current_month_start and current_date < end_date:
            print(f"\n📅 Fase 2: Carga DIÁRIA (mês atual: {current_month_start.strftime('%Y-%m')})")
            
            while current_date < end_date:
                batch_count += 1
                next_date = current_date + timedelta(days=1)
                
                if next_date > end_date:
                    next_date = end_date
                
                try:
                    print(f"   Batch {batch_count}: {current_date.strftime('%Y-%m-%d')}")
                    rows = ingest_batch(table_conf, current_date, next_date)
                    total_rows += rows
                    p_bar.update(next_date, total_rows)
                    print(f"      ✅ {rows:,} registros")
                except Exception as e:
                    print(f"      ❌ Erro: {e}")
                    print(f"      📌 Checkpoint salvo em: {current_date.date()}")
                    break
                
                current_date = next_date
        
        print(f"\n📊 Resumo {t_name}:")
        print(f"   Total de registros: {total_rows:,}")
        print(f"   Total de batches: {batch_count}")
        print(f"   Último checkpoint: {current_date.date()}")
