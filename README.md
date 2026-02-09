# B2B Databricks - DLT Jobs

Este repositório contém os **jobs Delta Live Tables (DLT)** para a arquitetura medalhão do projeto B2B.

## 📁 Estrutura

```
b2b-databricks-dlt/
├── pipelines/                    # Pipelines DLT organizadas por camada
│   ├── bronze/                   # Camada Bronze (dados brutos)
│   │   ├── ingestion_pipeline.py
│   │   └── README.md
│   ├── silver/                   # Camada Silver (dados limpos)
│   │   ├── transformation_pipeline.py
│   │   └── README.md
│   └── gold/                     # Camada Gold (dados agregados)
│       ├── aggregation_pipeline.py
│       └── README.md
├── shared/                       # Código compartilhado
│   ├── utils.py                  # Utilitários gerais
│   ├── schemas.py                # Schemas de dados
│   └── validators.py             # Validadores de qualidade
├── config/                       # Configurações por ambiente
│   ├── dev.yaml
│   ├── staging.yaml
│   └── prod.yaml
├── tests/                        # Testes unitários
│   └── test_pipelines.py
├── .env.example                  # Exemplo de variáveis de ambiente
├── requirements.txt              # Dependências Python
└── README.md                     # Este arquivo
```

## 🚀 Como Funciona

1. **Git Push**: Você faz push do código para este repositório
2. **Databricks Repo**: O Terraform sincroniza automaticamente (via `databricks_repo`)
3. **DLT Pipeline**: A pipeline DLT executa o código sincronizado
4. **Job**: O job orquestra a execução da pipeline

## 🧭 Resumo Operacional por Ambiente (DLT VCN)

### 🟢 DEV - Laboratorio
- **Comportamento**: Amostragem rapida
- **Volume**: N registros (ex: 100.000 linhas) de qualquer epoca
- **Objetivo**: Validar estrutura, colunas e logica sem custo alto
- **Disparo**: Manual

### 🟡 STAGING - Ensaio Geral
- **Comportamento**: Espelho de producao
- **Volume**: Volume real
- **Logica**: SCD Type 1 nas gigantes e Full Load nas pequenas
- **Objetivo**: Testar performance e CDC antes de release
- **Disparo**: Manual ou via CI/CD

### 🔴 PROD - Vida Real
- **Comportamento**: Automatico e incremental
- **Volume**: Deltas diarios apos a carga inicial
- **Logica**: DLT gerencia estado com base em last_modified
- **Objetivo**: Manter Lakehouse atualizado
- **Disparo**: Agendado (ex: 01:00)

## 🏗️ Arquitetura Medalhão

### Bronze (Raw Data)
- Ingestão de dados brutos
- Mínima transformação
- Histórico completo

### Silver (Cleaned Data)
- Dados limpos e validados
- Deduplicação
- Padronização de tipos

### Gold (Aggregated Data)
- Dados prontos para consumo
- Agregações de negócio
- Métricas e KPIs

## ⚙️ Configuração

### 1. Copie o arquivo de exemplo
```bash
cp .env.example .env
```

### 2. Configure as variáveis
Edite o `.env` com suas credenciais e configurações.

### 3. Instale as dependências
```bash
pip install -r requirements.txt
```

## 🔧 Desenvolvimento Local

Para testar localmente (sem DLT):
```bash
python -m pytest tests/
```

## 📊 Monitoramento

- **Qualidade de Dados**: Expectations do DLT
- **Performance**: Métricas do Photon
- **Logs**: Disponíveis no Databricks UI

## 🔐 Segurança

- Credenciais nunca em código
- Use variáveis de ambiente
- Service principals para CI/CD

## 📝 Contribuindo

1. Crie uma branch feature
2. Faça suas alterações
3. Teste localmente
4. Crie um Pull Request para `develop`
