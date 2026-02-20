# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Silver: Flatten json_data Column
# MAGIC
# MAGIC This notebook flattens the nested JSON stored in `json_data` from bronze
# MAGIC tables into columnar silver tables using DLT and PySpark.
# MAGIC
# MAGIC - Source: Bronze tables
# MAGIC - Target: Silver tables
# MAGIC - Approach: Auto-detect and flatten `json_data` column if present
# COMMAND ----------

import pyspark.sql.functions as F
import yaml
from pyspark.sql.types import (
    StructType,
    ArrayType,
)
import json

ENVIRONMENT = spark.conf.get("pipeline.env", "dev").lower()
CONFIG_PATH = f"/Workspace/Repos/sp_b2b_ops_bot/b2b-databricks-dlt-{ENVIRONMENT}/config/tables_vcn_public.yaml"

BRONZE_DB = f"vcn_public_{ENVIRONMENT}.bronze"
SILVER_DB = f"vcn_public_{ENVIRONMENT}.silver"
# COMMAND ----------


from pyspark.sql.types import ArrayType, StructType
from pyspark.sql.functions import expr, col, to_json
import re


def normalize_name(name: str) -> str:
    """
    Converte nomes para camelCase correto.
    - Se já é camelCase (sem underscore), preserva: bankSlipDigit -> bankSlipDigit
    - Se tem underscore, converte: bank_slip_digit -> bankSlipDigit
    - Primeira letra sempre minúscula
    """
    parts = re.split(r'[_\s]+', name)

    if not parts:
        return name

    first = parts[0]
    result = first[0].lower() + first[1:] if first else ""

    for part in parts[1:]:
        if part:
            result += part[0].upper() + part[1:]

    return result


def flatten_schema(schema, prefix="", parent_col="", apply_camel_case=True, skip_names=None):
    """
    Recursively flatten a StructType schema into column expressions.

    Args:
        schema: StructType to flatten
        prefix: Prefix for column names
        parent_col: Parent column path (for accessing nested fields)
        apply_camel_case: Se True, aplica camelCase nos nomes
        skip_names: Set de nomes a serem ignorados (já existem nos metadados)

    Returns:
        List of Column expressions
    """
    if skip_names is None:
        skip_names = set()

    flat_cols = []

    for field in schema.fields:
        field_name = field.name
        field_type = field.dataType

        # Build the full column path
        if parent_col:
            col_path = f"{parent_col}.`{field_name}`"
        else:
            col_path = f"`{field_name}`"

        # Build the alias name
        if apply_camel_case:
            full_name = normalize_name(f"{prefix}_{field_name}" if prefix else field_name)
        else:
            full_name = f"{prefix}_{field_name}" if prefix else field_name

        # PULAR se o nome já existe nos metadados
        if full_name in skip_names:
            print(f"[DLT INFO] Skipping column '{full_name}' from JSON - already exists in metadata")
            continue

        if isinstance(field_type, StructType):
            # Recursively flatten nested structs
            flat_cols.extend(
                flatten_schema(
                    field_type,
                    prefix=full_name,
                    parent_col=col_path,
                    apply_camel_case=apply_camel_case,
                    skip_names=skip_names
                )
            )
        elif isinstance(field_type, ArrayType):
            flat_cols.append(col(col_path).alias(full_name))
        else:
            flat_cols.append(col(col_path).alias(full_name))

    return flat_cols
# COMMAND ----------

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

def get_table_config(table_name):
    tables = config.get('tables') or []
    for t in tables:
        if t.get('name') == table_name:
            return t
    return {}

# COMMAND ----------

from pyspark.sql.functions import expr

def get_bronze_tables():
    catalog = f"vcn_public_{ENVIRONMENT}"

    rows = spark.sql(f"""
        SELECT table_name
        FROM system.information_schema.tables
        WHERE table_catalog = '{catalog}'
          AND table_schema = 'bronze'
          AND table_type = 'MATERIALIZED_VIEW'
          AND table_name NOT LIKE '__materialization%'
    """).collect()

    # Filtrar apenas tabelas que possuem coluna json_data
    tables_with_json = []
    for r in rows:
        table_name = r.table_name
        try:
            columns = spark.read.table(f"{catalog}.bronze.{table_name}").columns
            if "json_data" in columns:
                tables_with_json.append(table_name)
        except Exception as e:
            print(f"[DLT WARNING] Could not read table {table_name}: {e}")

    return tables_with_json

from pyspark.sql.functions import schema_of_json, first, col, lit

_schema_cache = {}

def _merge_struct_types(s1, s2):
    """Merge two StructTypes: union of fields, preferring wider types."""
    fields_map = {f.name: f for f in s1.fields}
    for f in s2.fields:
        if f.name not in fields_map:
            fields_map[f.name] = f
        else:
            existing = fields_map[f.name]
            # Se ambos são StructType, merge recursivo
            if isinstance(existing.dataType, StructType) and isinstance(f.dataType, StructType):
                merged_inner = _merge_struct_types(existing.dataType, f.dataType)
                from pyspark.sql.types import StructField
                fields_map[f.name] = StructField(f.name, merged_inner, nullable=True)
            # Caso contrário, mantém o existente (primeiro vence)
    return StructType(list(fields_map.values()))


def infer_super_schema(spark, table_name, bronze_db=None, sample_size=100):
    """
    Infere um SUPER-SCHEMA mergindo schemas de múltiplas amostras de json_data.
    Compatível com DLT (sem RDD). Usa schema_of_json + merge manual.
    Linhas que não possuem um campo ganham NULL nele.
    """
    if table_name in _schema_cache:
        return _schema_cache[table_name]

    db = bronze_db or BRONZE_DB

    try:
        df = spark.read.table(f"{db}.{table_name}")
        # Coletar amostras diversas de json_data
        samples = (
            df.select("json_data")
            .where(F.col("json_data").isNotNull())
            .limit(sample_size)
            .collect()
        )

        if not samples:
            print(f"[DLT WARNING] No json_data rows for {table_name}")
            return None

        # Inferir schema de cada sample e mergir
        merged_schema = None
        for row in samples:
            try:
                ddl = spark.range(1).select(
                    schema_of_json(lit(row.json_data))
                ).first()[0]
                row_schema = StructType.fromDDL(ddl)

                if not isinstance(row_schema, StructType):
                    continue

                if merged_schema is None:
                    merged_schema = row_schema
                else:
                    merged_schema = _merge_struct_types(merged_schema, row_schema)
            except Exception:
                # JSON inválido ou primitivo — pular
                continue

        if merged_schema is None or len(merged_schema.fields) == 0:
            print(f"[DLT WARNING] Empty super-schema for {table_name}")
            return None

        # Proteção contra schema explosivo
        MAX_SCHEMA_FIELDS = 5000
        if len(merged_schema.fields) > MAX_SCHEMA_FIELDS:
            print(f"[DLT WARNING] Schema too large for {table_name}: {len(merged_schema.fields)} fields (max {MAX_SCHEMA_FIELDS})")
            return None

        print(f"[DLT INFO] Super-schema for {table_name}: {len(merged_schema.fields)} top-level fields (merged from {len(samples)} samples)")
        _schema_cache[table_name] = merged_schema
        return merged_schema

    except Exception as e:
        print(f"[DLT WARNING] Failed to infer super-schema for {table_name}: {e}")
        return None


def create_silver_dlt(table_name, bronze_db=None, silver_db=None):
    bdb = bronze_db if bronze_db else BRONZE_DB
    sdb = silver_db if silver_db else SILVER_DB
    table_cfg = get_table_config(table_name)
    pk = table_cfg.get('pk')
    view_name = f"{table_name}_prepared"

    json_schema = infer_super_schema(spark, table_name, bdb)

    if json_schema is None:
        print(f"[DLT INFO] Skipping {table_name} — could not infer schema")
        return

    if not isinstance(json_schema, StructType):
        print(f"[DLT ERROR] Schema is not StructType for {table_name}: {type(json_schema)}")
        return

    @dlt.view(name=view_name)
    def prepared_view():
        try:
            df = spark.read.table(f"{bdb}.{table_name}")
        except Exception as e:
            print(f"[DLT WARNING] Bronze table missing for {table_name}: {e}")
            return spark.createDataFrame([], StructType([]))

        try:
            df = df.withColumn(
                "data",
                F.from_json(F.col("json_data"), json_schema)
            )
        except Exception as e:
            print(f"[DLT ERROR] Failed to parse JSON for {table_name}: {e}")
            return spark.createDataFrame([], StructType([]))

        df = df.filter(F.col("data").isNotNull())

        meta_cols = [c for c in df.columns if c not in ["json_data", "data"]]

        try:
            flatten_cols = flatten_schema(
                json_schema,
                prefix="",
                parent_col="data",
                apply_camel_case=True,
                skip_names=set(meta_cols)  # Passar nomes a serem ignorados
            )
        except Exception as e:
            print(f"[DLT ERROR] Failed to flatten schema for {table_name}: {e}")
            return spark.createDataFrame([], StructType([]))

        if not flatten_cols:
            print(f"[DLT WARNING] No columns found in json_data for {table_name}")
            return df.select(*[col(c) for c in meta_cols])

        df = df.select(*[col(c) for c in meta_cols], *flatten_cols)

        if pk and pk in df.columns:
            df = df.dropDuplicates([pk])

        return df

    @dlt.table(name=table_name, comment=f"Silver table for {table_name} (deduplicated by PK)")
    def silver_table():
        return dlt.read(view_name)


bronze_tables = get_bronze_tables()

for t in bronze_tables:
    create_silver_dlt(t)