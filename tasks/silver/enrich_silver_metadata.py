# Databricks notebook source
# MAGIC %md
# MAGIC # Enriquecimento de Metadados nas Tabelas Silver
# MAGIC
# MAGIC Este notebook lê o arquivo YAML de configuração e aplica descrições nas tabelas
# MAGIC e colunas do catálogo silver no Databricks. As descrições são extraídas do
# MAGIC bloco `descriptions` do YAML.
# MAGIC
# MAGIC - Atualiza comentários de tabela
# MAGIC - Atualiza comentários de coluna
# MAGIC - Só aplica se a tabela existir no catálogo silver
# COMMAND ----------

import yaml
# COMMAND ----------
# MAGIC %md
# MAGIC ## Caminho do arquivo YAML de configuração
# COMMAND ----------
yaml_path = "../config/tables_vcn_public.yaml"

# COMMAND ----------
silver_catalog = "silver"

# COMMAND ----------
# MAGIC %md
# Carrega o YAML e extrai descrições
# COMMAND ----------
with open(yaml_path, "r") as f:
    config = yaml.safe_load(f)

descricoes = config.get("descriptions", {})

# COMMAND ----------
# MAGIC %md
# Aplica descrições nas tabelas e colunas do catálogo silver
# COMMAND ----------
for nome_tabela, meta in descricoes.items():
    desc_tabela = meta.get("description", "")
    desc_colunas = meta.get("columns", {})
    tabela_completa = f"{silver_catalog}.{nome_tabela}"
    if spark.catalog.tableExists(tabela_completa):
        if desc_tabela:
            spark.sql(f"ALTER TABLE {tabela_completa} SET COMMENT '{desc_tabela.replace("'", "\'")}'")
        for coluna, desc_col in desc_colunas.items():
            spark.sql(f"ALTER TABLE {tabela_completa} ALTER COLUMN {coluna} COMMENT '{desc_col.replace("'", "\'")}'")
    else:
        print(f"Tabela {tabela_completa} não encontrada no catálogo silver.")
