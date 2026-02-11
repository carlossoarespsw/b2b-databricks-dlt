# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Ingestão de Tabelas Pesadas (Heavy Ingestion)
# MAGIC **Objetivo:** Migração incremental e histórica de tabelas massivas via Unity Catalog com visualização em tempo real.

# COMMAND ----------
# I. IMPORTS E SETUP VISUAL
import yaml
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from IPython.display import display, HTML
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException

# Silencia logs técnicos do Spark para manter a interface limpa
spark.sparkContext.setLogLevel("ERROR")

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
dbutils.widgets.text("environment", "dev", "Ambiente (dev/staging/prod)")
ENVIRONMENT = dbutils.widgets.get("environment").lower()

# Definição dos catálogos de Destino
RAW_CATALOG = "landingzone"
RAW_SCHEMA = "raw"

# Carregamento do YAML de configuração
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
    
    # Leitura com Predicate Pushdown
    df = spark.read.table(source_fqn).filter(
        (F.col(t_wm) >= start_date) & (F.col(t_wm) < end_date)
    )
    
    # Shield contra nvarchar(64) - Blindagem de strings
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.col(field.name).cast("string"))
            
    count = df.count()
    if count > 0:
        df.withColumn("_ingestion_ts", F.current_timestamp()) \
          .write.format("delta") \
          .mode("append") \
          .option("mergeSchema", "true") \
          .saveAsTable(target_table)
    
    return count

# COMMAND ----------
# IV. EXECUÇÃO DO LOOP DE MIGRAÇÃO
heavy_tables = [t for t in config['tables'] if t.get('heavy', False)]

if not heavy_tables:
    print("Nenhuma tabela com flag 'heavy: true' encontrada.")
else:
    for table_conf in heavy_tables:
        t_name = table_conf["name"]
        t_wm = table_conf["watermark"]
        target_table = f"{RAW_CATALOG}.{RAW_SCHEMA}.{t_name}"
        source_fqn = f"`{SOURCE_CATALOG}`.`{table_conf['schema']}`.`{t_name}`"
        
        # Lógica de Checkpoint (Retomada Automática)
        last_date = get_last_ingested_date(target_table, t_wm)
        
        if last_date:
            start_date = last_date
            print(f"🔄 Checkpoint encontrado para {t_name}. Retomando de {start_date}")
        else:
            min_date_val = spark.read.table(source_fqn).agg(F.min(t_wm)).collect()[0][0]
            if not min_date_val:
                print(f"⚠️ {t_name} está vazia na origem. Pulando.")
                continue
            start_date = min_date_val
            print(f"🆕 Iniciando carga histórica total para {t_name}")

        # Normalização de datas
        if isinstance(start_date, str): 
            start_date = datetime.strptime(start_date[:10], "%Y-%m-%d")
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = datetime.now()
        
        # Inicializa UI de progresso
        p_bar = TableProgressBar(t_name, start_date, end_date)
        current_date = start_date
        total_rows = 0
        
        # Loop Mensal
        while current_date < end_date:
            next_date = current_date + relativedelta(months=1)
            if next_date > end_date: next_date = end_date
            
            try:
                rows = ingest_batch(table_conf, current_date, next_date)
                total_rows += rows
                p_bar.update(next_date, total_rows)
            except Exception as e:
                print(f"❌ Erro crítico no batch {current_date.date()} da tabela {t_name}: {e}")
                break
                
            current_date = next_date

print(f"\n✅ PROCESSO FINALIZADO PARA {len(heavy_tables)} TABELAS.")