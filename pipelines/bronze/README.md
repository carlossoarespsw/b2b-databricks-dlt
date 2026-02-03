# Bronze Layer - Ingestão de Dados Brutos

## 📥 Objetivo

A camada **Bronze** é responsável pela ingestão de dados brutos com mínima transformação. Os dados são armazenados em seu formato original, preservando o histórico completo.

## 🔧 Pipeline: ingestion_pipeline.py

### Tabelas Criadas

#### 1. bronze_vendas
- **Fonte**: JSON files (S3/ADLS)
- **Formato**: Streaming (cloudFiles)
- **Validações**:
  - `id` não pode ser nulo
  - `data_venda` não pode ser nulo
- **Metadados Adicionados**:
  - `_ingestion_timestamp`: Timestamp de ingestão
  - `_source_file`: Arquivo de origem

#### 2. bronze_clientes
- **Fonte**: Parquet files
- **Formato**: Streaming (cloudFiles)
- **Validações**:
  - `cliente_id` não pode ser nulo
  - `email` deve conter '@'

#### 3. bronze_produtos
- **Fonte**: Delta table
- **Formato**: Batch

### Características

- ✅ **Auto Loader**: Processamento incremental automático
- ✅ **Schema Evolution**: Evolução automática de schema
- ✅ **Change Data Feed**: Rastreamento de mudanças
- ✅ **Quality Checks**: Validações de dados com expectations

## 🎯 Regras de Negócio

### Ambiente de Desenvolvimento
- Filtra apenas dados de 2025 (configurável)
- Debug mode habilitado
- Sample de dados para testes

### Ambiente de Produção
- Processa todos os dados históricos
- Sem filtros
- Otimizações ativadas

## 📊 Monitoramento

A tabela `bronze_metrics` registra:
- Quantidade de registros por tabela
- Timestamp de execução
- Permite rastreamento de volume de dados

## 🚨 Expectativas de Qualidade

```python
@dlt.expect_all_or_drop  # Descarta registros inválidos
@dlt.expect_or_fail      # Falha a pipeline se violado
@dlt.expect              # Apenas registra a violação
```

## 🔄 Fluxo de Dados

```
Source (S3/ADLS)
    │
    │ Auto Loader (cloudFiles)
    ▼
Bronze Tables
    │
    │ Metadados + Validações
    ▼
Unity Catalog (bronze schema)
```

## ⚙️ Configurações

Configurações passadas pelo Terraform:
- `environment`: dev/staging/prod
- `catalog`: Nome do catálogo
- `data_filter_year`: Filtro de ano (dev only)
- `debug_mode`: Modo de debug
