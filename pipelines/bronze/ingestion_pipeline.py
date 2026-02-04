import dlt
from pyspark.sql.functions import current_timestamp, lit

ENVIRONMENT = spark.conf.get("environment", "dev")
CATALOG_DEST = spark.conf.get("catalog", "b2b-federated")
SOURCE_CATALOG = "vcn-federated"
SOURCE_SCHEMA = "public"
RECORD_LIMIT = 1000000
SOURCE_SYSTEM = "VCN"

try:
    tables_query = f"""
    SELECT DISTINCT table_name 
    FROM {SOURCE_CATALOG}.information_schema.tables 
    WHERE table_schema = '{SOURCE_SCHEMA}'
    AND table_type = 'BASE TABLE'
    ORDER BY table_name
    """
    tables_result = spark.sql(tables_query).collect()
    table_names = [row["table_name"] for row in tables_result]
    print(f"[INFO] Ingestão VCN: {len(table_names)} tabelas descobertas de {SOURCE_CATALOG}")
except Exception as e:
    print(f"[AVISO] VCN indisponível: {e}")
    table_names = []

for table_name in table_names:
    @dlt.table(
        name=f"bronze_vcn_{table_name}",
        comment=f"VCN: {SOURCE_CATALOG}.{SOURCE_SCHEMA}.{table_name}",
        table_properties={
            "quality": "bronze",
            "source_system": SOURCE_SYSTEM,
            "pipelines.autoOptimize.managed": "true",
            "source_catalog": SOURCE_CATALOG,
            "source_schema": SOURCE_SCHEMA,
            "source_table": table_name
        }
    )
    def create_bronze_vcn_table(src_catalog=SOURCE_CATALOG, src_schema=SOURCE_SCHEMA, tbl_name=table_name, source_system=SOURCE_SYSTEM):
        source_table = f"{src_catalog}.{src_schema}.{tbl_name}"
        df = spark.read.table(source_table).limit(RECORD_LIMIT)
        
        df = df.withColumn("_ingestion_timestamp", current_timestamp())
        df = df.withColumn("_source_system", lit(source_system))
        df = df.withColumn("_source_catalog", lit(src_catalog))
        df = df.withColumn("_source_schema", lit(src_schema))
        df = df.withColumn("_source_table", lit(tbl_name))
        df = df.withColumn("_environment", lit(ENVIRONMENT))
        
        return df


if not table_names:
    print("[AVISO] Nenhuma tabela descoberta - usando dados de teste")
    
    @dlt.table(name="bronze_vendas", comment="Dados de teste")
    def bronze_test_vendas():
        from datetime import date
        data = [
            (1, 100, 200, date(2025, 1, 15), 150.50, 2, "CONFIRMADO"),
            (2, 101, 201, date(2025, 1, 20), 200.00, 3, "CONFIRMADO"),
            (3, 102, 202, date(2025, 2, 10), 175.25, 1, "PENDENTE"),
        ]
        df = spark.createDataFrame(
            data,
            ["id", "cliente_id", "produto_id", "data_venda", "valor_venda", "quantidade", "status"]
        )
        return df.withColumn("_ingestion_timestamp", current_timestamp()).withColumn("_environment", lit(ENVIRONMENT))
    
    @dlt.table(name="bronze_clientes", comment="Dados de teste")
    def bronze_test_clientes():
        from datetime import date
        data = [
            (100, "Cliente A", "cliente.a@email.com", date(2024, 1, 1)),
            (101, "Cliente B", "cliente.b@email.com", date(2024, 2, 15)),
            (102, "Cliente C", "cliente.c@email.com", date(2024, 3, 20)),
        ]
        df = spark.createDataFrame(data, ["id", "nome", "email", "data_cadastro"])
        return df.withColumn("_ingestion_timestamp", current_timestamp()).withColumn("_environment", lit(ENVIRONMENT))
    
    @dlt.table(name="bronze_produtos", comment="Dados de teste")
    def bronze_test_produtos():
        data = [
            (200, "Produto X", "Eletrônicos", 1500.00),
            (201, "Produto Y", "Eletrônicos", 2000.00),
            (202, "Produto Z", "Livros", 45.50),
        ]
        df = spark.createDataFrame(data, ["id", "nome", "categoria", "preco"])
        return df.withColumn("_ingestion_timestamp", current_timestamp()).withColumn("_environment", lit(ENVIRONMENT))