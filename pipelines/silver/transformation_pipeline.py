# ========================================
# Silver Layer - Transformation Pipeline (TEST VERSION)
# ========================================
# Pipeline de teste com transformações simplificadas

import dlt
from pyspark.sql.functions import col, when, trim, upper, current_timestamp, row_number, desc
from pyspark.sql.window import Window

# ========================================
# Configurações
# ========================================
ENVIRONMENT = spark.conf.get("environment", "dev")
CATALOG = spark.conf.get("catalog", "b2b_federated")

# ========================================
# Silver Table: Vendas Limpas (TESTE)
# ========================================

@dlt.table(
    name="silver_vendas",
    comment="Vendas limpas - TESTE",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all({
    "valid_valor": "valor_venda > 0",
    "valid_date": "data_venda IS NOT NULL"
})
def silver_vendas():
    """
    Teste: Transforma dados brutos de vendas.
    """
    df = dlt.read_stream("bronze_vendas")
    
    # Transformações básicas
    df = df.select(
        col("id").cast("bigint"),
        col("cliente_id").cast("bigint"),
        col("produto_id").cast("bigint"),
        col("data_venda"),
        col("valor_venda").cast("decimal(10,2)"),
        col("quantidade").cast("int"),
        trim(upper(col("status"))).alias("status"),
        col("_ingestion_timestamp")
    )
    
    # Deduplicação
    window_spec = Window.partitionBy("id").orderBy(desc("_ingestion_timestamp"))
    df = df.withColumn("rn", row_number().over(window_spec))
    df = df.filter(col("rn") == 1).drop("rn")
    
    df = df.withColumn("_processing_timestamp", current_timestamp())
    
    return df


# ========================================
# Silver Table: Clientes Limpos (TESTE)
# ========================================

@dlt.table(
    name="silver_clientes",
    comment="Clientes limpos - TESTE"
)
@dlt.expect_all({
    "valid_email": "email IS NOT NULL"
})
def silver_clientes():
    """
    Teste: Transforma dados brutos de clientes.
    """
    df = dlt.read_stream("bronze_clientes")
    
    # Transformações básicas
    df = df.select(
        col("cliente_id").cast("bigint"),
        trim(col("nome")).alias("nome"),
        trim(upper(col("email"))).alias("email"),
        col("cpf_cnpj"),
        trim(col("telefone")).alias("telefone"),
        trim(col("cidade")).alias("cidade"),
        trim(upper(col("estado"))).alias("estado"),
        col("_ingestion_timestamp")
    )
    
    # Classificação
    df = df.withColumn(
        "tipo_cliente",
        when(col("cpf_cnpj").isNotNull() & (col("cpf_cnpj").cast("string").length == 11), "PF")
        .otherwise("PJ")
    )
    
    # Deduplicação
    window_spec = Window.partitionBy("cliente_id").orderBy(desc("_ingestion_timestamp"))
    df = df.withColumn("rn", row_number().over(window_spec))
    df = df.filter(col("rn") == 1).drop("rn")
    
    df = df.withColumn("_processing_timestamp", current_timestamp())
    
    return df


# ========================================
# Silver Table: Produtos Limpos (TESTE)
# ========================================

@dlt.table(
    name="silver_produtos",
    comment="Produtos limpos - TESTE"
)
def silver_produtos():
    """
    Teste: Transforma dados brutos de produtos.
    """
    df = dlt.read("bronze_produtos")
    
    df = df.select(
        col("produto_id").cast("bigint"),
        trim(col("nome_produto")).alias("nome_produto"),
        trim(upper(col("categoria"))).alias("categoria"),
        col("preco_unitario").cast("decimal(10,2)"),
        trim(col("descricao")).alias("descricao")
    )
    
    # Validação
    df = df.filter(col("preco_unitario") > 0)
    
    df = df.withColumn("_processing_timestamp", current_timestamp())
    
    return df
