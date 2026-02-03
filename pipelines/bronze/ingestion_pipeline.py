# ========================================
# Bronze Layer - Ingestion Pipeline (TEST VERSION)
# ========================================
# Pipeline de teste com dados mockados para validação

import dlt
from pyspark.sql.functions import col, current_timestamp, lit
from datetime import datetime, date

# ========================================
# Configurações
# ========================================
ENVIRONMENT = spark.conf.get("environment", "dev")
CATALOG = spark.conf.get("catalog", "b2b_federated")

# ========================================
# Bronze Table: Test Vendas
# ========================================

@dlt.table(
    name="bronze_vendas",
    comment="Dados de teste - Camada Bronze",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all_or_drop({
    "valid_id": "id IS NOT NULL",
    "valid_valor": "valor_venda > 0"
})
def bronze_vendas():
    """
    Teste: Cria dados mockados de vendas.
    """
    data = [
        (1, 100, 200, date(2025, 1, 15), 150.50, 2, "CONFIRMADO"),
        (2, 101, 201, date(2025, 1, 20), 200.00, 3, "CONFIRMADO"),
        (3, 102, 202, date(2025, 2, 10), 175.25, 1, "PENDENTE"),
        (4, 103, 203, date(2025, 2, 15), 300.00, 5, "CONFIRMADO"),
        (5, 104, 204, date(2025, 3, 05), 125.75, 2, "CONFIRMADO"),
    ]
    
    df = spark.createDataFrame(
        data,
        ["id", "cliente_id", "produto_id", "data_venda", "valor_venda", "quantidade", "status"]
    )
    
    df = df.withColumn("_ingestion_timestamp", current_timestamp())
    df = df.withColumn("_source_file", lit("test_data"))
    
    return df


@dlt.table(
    name="bronze_clientes",
    comment="Dados de teste de clientes - Camada Bronze",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all_or_drop({
    "valid_cliente_id": "cliente_id IS NOT NULL",
    "valid_email": "email IS NOT NULL"
})
def bronze_clientes():
    """
    Teste: Cria dados mockados de clientes.
    """
    data = [
        (100, "Cliente A", "clientea@email.com", "12345678901", "11987654321", "São Paulo", "SP"),
        (101, "Cliente B", "clienteb@email.com", "98765432100", "11987654322", "Rio de Janeiro", "RJ"),
        (102, "Cliente C", "clientec@email.com", "11144455566", "11987654323", "Belo Horizonte", "MG"),
        (103, "Cliente D", "cliented@email.com", "22233344455", "11987654324", "São Paulo", "SP"),
        (104, "Cliente E", "cliente.e@email.com", "33344455566", "11987654325", "Brasília", "DF"),
    ]
    
    df = spark.createDataFrame(
        data,
        ["cliente_id", "nome", "email", "cpf_cnpj", "telefone", "cidade", "estado"]
    )
    
    df = df.withColumn("_ingestion_timestamp", current_timestamp())
    df = df.withColumn("_source_file", lit("test_data"))
    
    return df
de teste de produtos - Camada Bronze"
)
def bronze_produtos():
    """
    Teste: Cria dados mockados de produtos.
    """
    data = [
        (200, "Notebook Dell", "Eletrônicos", 3500.00, "Notebook Dell Inspiron 15"),
        (201, "Mouse Logitech", "Periféricos", 150.00, "Mouse sem fio Logitech M705"),
        (202, "Teclado Mecânico", "Periféricos", 450.00, "Teclado mecânico RGB"),
        (203, "Monitor LG 24\"", "Eletrônicos", 1200.00, "Monitor LG 24 polegadas Full HD"),
        (204, "Webcam HD", "Periféricos", 300.00, "Webcam Full HD com microfone"),
    ]
    
    df = spark.createDataFrame(
        data,
        ["produto_id", "nome_produto", "categoria", "preco_unitario", "descricao"]
    )
    
    df = df.withColumn("_ingestion_timestamp", current_timestamp())
    
    return df


# ========================================
# Métricas de Teste
# ========================================

@dlt.table(
    name="bronze_metrics",
    comment="Métricas de ingestão - TESTE"
)
def bronze_metrics():
    """
    Tabela de métricas para validação.
    """
    vendas = dlt.read("bronze_vendas")
    clientes = dlt.read("bronze_clientes")
    produtos = dlt.read("bronze_produtos")
    
    metrics = spark.createDataFrame([
        ("bronze_vendas", vendas.count()),
        ("bronze_clientes", clientes.count()),
        ("bronze_produtos", produtos.count())
    ], ["table_name", "record_count"])
    
    return metrics.withColumn("timestamp", current_timestamp())ity_checks():
    """
    Checks de qualidade para garantir dados válidos.
    """
    pass
