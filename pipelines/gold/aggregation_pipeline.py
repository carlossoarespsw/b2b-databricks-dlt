# ========================================
# Gold Layer - Aggregation Pipeline (TEST VERSION)
# ========================================
# Pipeline de teste com agregações simplificadas

import dlt
from pyspark.sql.functions import col, sum, count, avg, current_timestamp, desc

# ========================================
# Configurações
# ========================================
ENVIRONMENT = spark.conf.get("environment", "dev")
CATALOG = spark.conf.get("catalog", "b2b_federated")

# ========================================
# Gold Table: Vendas Resumidas (TESTE)
# ========================================

@dlt.table(
    name="gold_vendas_resumo",
    comment="Resumo de vendas - TESTE",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_vendas_resumo():
    """
    Teste: Resumo de vendas por cliente.
    """
    vendas = dlt.read("silver_vendas")
    
    df = vendas.groupBy("cliente_id").agg(
        sum("valor_venda").alias("total_vendas"),
        avg("valor_venda").alias("ticket_medio"),
        count("id").alias("qtd_transacoes"),
        sum("quantidade").alias("qtd_produtos")
    )
    
    df = df.withColumn("_processing_timestamp", current_timestamp())
    
    return df.orderBy(desc("total_vendas"))


# ========================================
# Gold Table: Top Produtos (TESTE)
# ========================================

@dlt.table(
    name="gold_top_produtos",
    comment="Produtos mais vendidos - TESTE"
)
def gold_top_produtos():
    """
    Teste: Ranking de produtos.
    """
    vendas = dlt.read("silver_vendas")
    produtos = dlt.read("silver_produtos")
    
    df = vendas.join(produtos, "produto_id")
    
    df = df.groupBy(
        col("produto_id"),
        col("nome_produto"),
        col("categoria")
    ).agg(
        sum("valor_venda").alias("receita_total"),
        sum("quantidade").alias("qtd_vendida"),
        count("id").alias("qtd_transacoes")
    )
    
    df = df.withColumn("_processing_timestamp", current_timestamp())
    
    return df.orderBy(desc("receita_total"))


# ========================================
# Gold Table: Perfil de Clientes (TESTE)
# ========================================

@dlt.table(
    name="gold_perfil_clientes",
    comment="Perfil de clientes - TESTE"
)
def gold_perfil_clientes():
    """
    Teste: Análise de clientes.
    """
    vendas = dlt.read("silver_vendas")
    clientes = dlt.read("silver_clientes")
    
    df = vendas.join(clientes, "cliente_id")
    
    df = df.groupBy(
        col("cliente_id"),
        col("nome"),
        col("tipo_cliente"),
        col("cidade")
    ).agg(
        sum("valor_venda").alias("ltv"),
        avg("valor_venda").alias("ticket_medio"),
        count("id").alias("qtd_compras")
    )
    
    # Classificação
    from pyspark.sql.functions import when
    df = df.withColumn(
        "segmento",
        when(col("ltv") >= 500, "VIP")
        .when(col("ltv") >= 250, "PREMIUM")
        .otherwise("REGULAR")
    )
    
    df = df.withColumn("_processing_timestamp", current_timestamp())
    
    return df.orderBy(desc("ltv"))


# ========================================
# Gold Table: KPIs (TESTE)
# ========================================

@dlt.table(
    name="gold_kpis",
    comment="KPIs principais - TESTE"
)
def gold_kpis():
    """
    Teste: Dashboard de KPIs.
    """
    vendas = dlt.read("silver_vendas")
    clientes = dlt.read("silver_clientes")
    produtos = dlt.read("silver_produtos")
    
    total_receita = vendas.agg(sum("valor_venda")).collect()[0][0]
    total_transacoes = vendas.count()
    total_clientes = clientes.count()
    total_produtos = produtos.count()
    
    kpis = spark.createDataFrame([{
        "metrica": "Receita Total",
        "valor": float(total_receita or 0),
        "tipo": "currency"
    }, {
        "metrica": "Total de Transações",
        "valor": float(total_transacoes),
        "tipo": "integer"
    }, {
        "metrica": "Total de Clientes",
        "valor": float(total_clientes),
        "tipo": "integer"
    }, {
        "metrica": "Total de Produtos",
        "valor": float(total_produtos),
        "tipo": "integer"
    }])
    
    return kpis.withColumn("_processing_timestamp", current_timestamp())
