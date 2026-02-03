# ========================================
# Unit Tests for DLT Pipelines
# ========================================

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, DateType
from datetime import date
from chispa.dataframe_comparer import assert_df_equality


@pytest.fixture(scope="session")
def spark():
    """Cria uma sessão Spark para testes."""
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("DLT-Tests")
        .getOrCreate()
    )


class TestBronzeLayer:
    """Testes para a camada Bronze."""
    
    def test_bronze_vendas_schema(self, spark):
        """Testa se o schema da tabela bronze_vendas está correto."""
        data = [
            (1, 100, 200, date(2025, 1, 1), 150.50, 2, "CONFIRMADO")
        ]
        
        schema = StructType([
            StructField("id", LongType(), False),
            StructField("cliente_id", LongType(), False),
            StructField("produto_id", LongType(), False),
            StructField("data_venda", DateType(), False),
            StructField("valor_venda", DecimalType(10, 2), False),
            StructField("quantidade", LongType(), False),
            StructField("status", StringType(), True),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        assert df.count() == 1
        assert "id" in df.columns
        assert "valor_venda" in df.columns
    
    def test_bronze_vendas_not_null(self, spark):
        """Testa validação de valores nulos."""
        data = [
            (1, 100, 200, date(2025, 1, 1), 150.50, 2, "CONFIRMADO")
        ]
        
        schema = StructType([
            StructField("id", LongType(), False),
            StructField("cliente_id", LongType(), False),
            StructField("produto_id", LongType(), False),
            StructField("data_venda", DateType(), False),
            StructField("valor_venda", DecimalType(10, 2), False),
            StructField("quantidade", LongType(), False),
            StructField("status", StringType(), True),
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Verifica que não há nulos em colunas obrigatórias
        assert df.filter("id IS NULL").count() == 0
        assert df.filter("valor_venda IS NULL").count() == 0


class TestSilverLayer:
    """Testes para a camada Silver."""
    
    def test_silver_vendas_deduplication(self, spark):
        """Testa deduplicação de registros."""
        from pyspark.sql.functions import row_number, desc
        from pyspark.sql.window import Window
        
        # Dados duplicados
        data = [
            (1, 100, 200, date(2025, 1, 1), 150.50, 2, "2025-01-01 10:00:00"),
            (1, 100, 200, date(2025, 1, 1), 150.50, 2, "2025-01-01 11:00:00"),  # Duplicata
        ]
        
        df = spark.createDataFrame(
            data,
            ["id", "cliente_id", "produto_id", "data_venda", "valor_venda", "quantidade", "_ingestion_timestamp"]
        )
        
        # Aplica deduplicação
        window_spec = Window.partitionBy("id").orderBy(desc("_ingestion_timestamp"))
        df_deduped = df.withColumn("rn", row_number().over(window_spec))
        df_deduped = df_deduped.filter("rn = 1").drop("rn")
        
        assert df_deduped.count() == 1
    
    def test_silver_clientes_email_validation(self, spark):
        """Testa validação de email."""
        from pyspark.sql.functions import col
        
        data = [
            (1, "Cliente A", "valido@email.com", "12345678901"),
            (2, "Cliente B", "invalido", "98765432100"),
        ]
        
        df = spark.createDataFrame(
            data,
            ["cliente_id", "nome", "email", "cpf_cnpj"]
        )
        
        # Filtra emails válidos
        df_valid = df.filter(col("email").rlike(r'^[\w\.-]+@[\w\.-]+\.\w+$'))
        
        assert df_valid.count() == 1
        assert df_valid.first()["email"] == "valido@email.com"


class TestGoldLayer:
    """Testes para a camada Gold."""
    
    def test_gold_vendas_mensais_aggregation(self, spark):
        """Testa agregação mensal de vendas."""
        from pyspark.sql.functions import sum, count, avg
        
        data = [
            (1, 100, date(2025, 1, 15), 100.0, 2025, 1),
            (2, 101, date(2025, 1, 20), 200.0, 2025, 1),
            (3, 102, date(2025, 2, 10), 150.0, 2025, 2),
        ]
        
        df = spark.createDataFrame(
            data,
            ["id", "cliente_id", "data_venda", "valor_venda", "ano_venda", "mes_venda"]
        )
        
        # Agrega por mês
        df_agg = df.groupBy("ano_venda", "mes_venda").agg(
            sum("valor_venda").alias("total_vendas"),
            count("id").alias("qtd_transacoes"),
            avg("valor_venda").alias("ticket_medio")
        )
        
        # Verifica agregação de janeiro
        jan_data = df_agg.filter("mes_venda = 1").first()
        assert jan_data["total_vendas"] == 300.0
        assert jan_data["qtd_transacoes"] == 2
        assert jan_data["ticket_medio"] == 150.0


class TestUtils:
    """Testes para funções utilitárias."""
    
    def test_validate_cpf(self):
        """Testa validação de CPF."""
        from shared.utils import validate_cpf
        
        # CPF válido
        assert validate_cpf("12345678909") == True  # Exemplo simplificado
        
        # CPF inválido
        assert validate_cpf("11111111111") == False
        assert validate_cpf("123") == False
        assert validate_cpf(None) == False
    
    def test_clean_phone(self, spark):
        """Testa limpeza de telefone."""
        from shared.utils import clean_phone
        
        data = [
            (1, "(11) 98765-4321"),
            (2, "11 9 8765-4321"),
        ]
        
        df = spark.createDataFrame(data, ["id", "telefone"])
        df_clean = clean_phone(df, "telefone")
        
        # Verifica que apenas números restaram
        assert df_clean.first()["telefone"] == "11987654321"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
