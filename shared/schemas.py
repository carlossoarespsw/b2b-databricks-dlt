# ========================================
# Data Schemas
# ========================================
# Schemas de dados para as tabelas DLT

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    DecimalType, DateType, TimestampType,
    BooleanType
)


# ========================================
# Bronze Schemas
# ========================================

VENDAS_SCHEMA = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("cliente_id", LongType(), nullable=False),
    StructField("produto_id", LongType(), nullable=False),
    StructField("data_venda", DateType(), nullable=False),
    StructField("valor_venda", DecimalType(10, 2), nullable=False),
    StructField("quantidade", IntegerType(), nullable=False),
    StructField("status", StringType(), nullable=True),
    StructField("ano_venda", IntegerType(), nullable=True),
    StructField("mes_venda", IntegerType(), nullable=True),
])

CLIENTES_SCHEMA = StructType([
    StructField("cliente_id", LongType(), nullable=False),
    StructField("nome", StringType(), nullable=False),
    StructField("email", StringType(), nullable=False),
    StructField("cpf_cnpj", StringType(), nullable=True),
    StructField("telefone", StringType(), nullable=True),
    StructField("endereco", StringType(), nullable=True),
    StructField("cidade", StringType(), nullable=True),
    StructField("estado", StringType(), nullable=True),
    StructField("cep", StringType(), nullable=True),
])

PRODUTOS_SCHEMA = StructType([
    StructField("produto_id", LongType(), nullable=False),
    StructField("nome_produto", StringType(), nullable=False),
    StructField("categoria", StringType(), nullable=True),
    StructField("subcategoria", StringType(), nullable=True),
    StructField("preco_unitario", DecimalType(10, 2), nullable=False),
    StructField("descricao", StringType(), nullable=True),
    StructField("estoque", IntegerType(), nullable=True),
])


# ========================================
# Silver Schemas (com auditoria)
# ========================================

def add_audit_fields(schema: StructType) -> StructType:
    """Adiciona campos de auditoria ao schema."""
    audit_fields = [
        StructField("_ingestion_timestamp", TimestampType(), nullable=True),
        StructField("_processing_timestamp", TimestampType(), nullable=True),
        StructField("_source_file", StringType(), nullable=True),
    ]
    return StructType(schema.fields + audit_fields)


SILVER_VENDAS_SCHEMA = add_audit_fields(VENDAS_SCHEMA)
SILVER_CLIENTES_SCHEMA = add_audit_fields(CLIENTES_SCHEMA)
SILVER_PRODUTOS_SCHEMA = add_audit_fields(PRODUTOS_SCHEMA)


# ========================================
# Gold Schemas
# ========================================

VENDAS_MENSAIS_SCHEMA = StructType([
    StructField("ano_venda", IntegerType(), nullable=False),
    StructField("mes_venda", IntegerType(), nullable=False),
    StructField("total_vendas", DecimalType(18, 2), nullable=True),
    StructField("ticket_medio", DecimalType(10, 2), nullable=True),
    StructField("qtd_transacoes", LongType(), nullable=True),
    StructField("qtd_clientes_unicos", LongType(), nullable=True),
    StructField("qtd_produtos_vendidos", LongType(), nullable=True),
    StructField("_processing_timestamp", TimestampType(), nullable=True),
])

TOP_PRODUTOS_SCHEMA = StructType([
    StructField("produto_id", LongType(), nullable=False),
    StructField("nome_produto", StringType(), nullable=False),
    StructField("categoria", StringType(), nullable=True),
    StructField("receita_total", DecimalType(18, 2), nullable=True),
    StructField("qtd_vendida", LongType(), nullable=True),
    StructField("qtd_transacoes", LongType(), nullable=True),
    StructField("_processing_timestamp", TimestampType(), nullable=True),
])

PERFIL_CLIENTES_SCHEMA = StructType([
    StructField("cliente_id", LongType(), nullable=False),
    StructField("nome", StringType(), nullable=False),
    StructField("tipo_cliente", StringType(), nullable=True),
    StructField("cidade", StringType(), nullable=True),
    StructField("estado", StringType(), nullable=True),
    StructField("ltv", DecimalType(18, 2), nullable=True),
    StructField("ticket_medio", DecimalType(10, 2), nullable=True),
    StructField("qtd_compras", LongType(), nullable=True),
    StructField("ultima_compra", DateType(), nullable=True),
    StructField("segmento", StringType(), nullable=True),
    StructField("_processing_timestamp", TimestampType(), nullable=True),
])
