# ========================================
# Quick Start Guide
# ========================================

## 🎯 Objetivo

Este guia rápido vai te ajudar a configurar e executar as pipelines DLT em 5 minutos!

## 📋 Pré-requisitos

- Python 3.8+
- Git configurado
- Acesso ao Databricks Workspace
- Service Principal criado

## 🚀 Passo a Passo

### 1. Clone o repositório

```bash
git clone https://github.com/seu-usuario/b2b-databricks-dlt.git
cd b2b-databricks-dlt
git checkout develop
```

### 2. Configure as variáveis de ambiente

```bash
cp .env.example .env
# Edite o .env com suas credenciais
```

### 3. Instale as dependências

```bash
pip install -r requirements.txt
```

### 4. Execute os testes

```bash
pytest tests/ -v
```

### 5. Deploy da infraestrutura (no repo de infra)

```bash
cd ../b2b-databricks-infra/environments/dev
terraform init
terraform plan
terraform apply
```

### 6. Monitore a execução

Acesse o Databricks UI:
- Pipelines: `/pipelines`
- Jobs: `/jobs`

## 🔧 Desenvolvimento

### Criar nova pipeline

1. Crie um arquivo em `pipelines/{layer}/minha_pipeline.py`
2. Implemente as funções DLT
3. Adicione testes em `tests/`
4. Commit e push

```bash
git add .
git commit -m "feat: nova pipeline de ingestão"
git push origin develop
```

### Testar localmente

```bash
python -m pytest tests/test_pipelines.py::TestBronzeLayer -v
```

### Ver logs

```bash
# No Databricks UI
# Pipelines > Sua Pipeline > Execution History
```

## 📊 Estrutura de Dados

### Bronze (Raw)
```
s3://b2b-databricks-dev/bronze/
├── vendas/
├── clientes/
└── produtos/
```

### Silver (Cleaned)
```
catalog: b2b_dev
schema: silver
tables:
  - silver_vendas
  - silver_clientes
  - silver_produtos
```

### Gold (Aggregated)
```
catalog: b2b_dev
schema: gold
tables:
  - gold_vendas_mensais
  - gold_top_produtos
  - gold_perfil_clientes
```

## ⚠️ Troubleshooting

### Erro: "Cannot connect to Databricks"
- Verifique `DATABRICKS_HOST` e `DATABRICKS_TOKEN` no `.env`
- Confirme que o token não expirou

### Erro: "Permission denied"
- Verifique permissões do Service Principal
- Confirme acesso ao catálogo Unity Catalog

### Pipeline não executa
- Verifique se o Job está pausado (normal em dev)
- Execute manualmente pelo UI

## 📚 Próximos Passos

1. ✅ Configurar notificações
2. ✅ Adicionar mais testes
3. ✅ Configurar CI/CD
4. ✅ Monitorar métricas de qualidade

## 🆘 Ajuda

- Documentação: [README.md](README.md)
- Issues: GitHub Issues
- Slack: #data-engineering
