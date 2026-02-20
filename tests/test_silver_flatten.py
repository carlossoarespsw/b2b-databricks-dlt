# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Test: Silver Flatten JSON Logic
# MAGIC 
# MAGIC Notebook de teste para validar a lógica de:
# MAGIC 1. Descoberta de tabelas bronze com `json_data`
# MAGIC 2. Inferência de super-schema (merge de múltiplas amostras)
# MAGIC 3. Flatten recursivo com camelCase
# MAGIC 4. Display dos resultados

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, ArrayType, StructField
from pyspark.sql.functions import schema_of_json, col, lit
import re
import json

ENVIRONMENT = "dev"
BRONZE_DB = f"vcn_public_{ENVIRONMENT}.bronze"
CATALOG = f"vcn_public_{ENVIRONMENT}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Funções (copiadas do pipeline)

# COMMAND ----------

def normalize_name(name: str) -> str:
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
    if skip_names is None:
        skip_names = set()
    flat_cols = []
    for field in schema.fields:
        field_name = field.name
        field_type = field.dataType
        if parent_col:
            col_path = f"{parent_col}.`{field_name}`"
        else:
            col_path = f"`{field_name}`"
        if apply_camel_case:
            full_name = normalize_name(f"{prefix}_{field_name}" if prefix else field_name)
        else:
            full_name = f"{prefix}_{field_name}" if prefix else field_name
        if full_name in skip_names:
            continue
        if isinstance(field_type, StructType):
            flat_cols.extend(flatten_schema(field_type, prefix=full_name, parent_col=col_path, apply_camel_case=apply_camel_case, skip_names=skip_names))
        else:
            flat_cols.append(col(col_path).alias(full_name))
    return flat_cols


def _merge_struct_types(s1, s2):
    fields_map = {f.name: f for f in s1.fields}
    for f in s2.fields:
        if f.name not in fields_map:
            fields_map[f.name] = f
        else:
            existing = fields_map[f.name]
            if isinstance(existing.dataType, StructType) and isinstance(f.dataType, StructType):
                merged_inner = _merge_struct_types(existing.dataType, f.dataType)
                fields_map[f.name] = StructField(f.name, merged_inner, nullable=True)
    return StructType(list(fields_map.values()))


def infer_super_schema(spark, table_name, sample_size=100):
    df = spark.read.table(f"{BRONZE_DB}.{table_name}")
    samples = (
        df.select("json_data")
        .where(F.col("json_data").isNotNull())
        .limit(sample_size)
        .collect()
    )
    if not samples:
        print(f"⚠️ No json_data for {table_name}")
        return None

    merged_schema = None
    errors = 0
    for row in samples:
        try:
            ddl = spark.range(1).select(schema_of_json(lit(row.json_data))).first()[0]
            row_schema = StructType.fromDDL(ddl)
            if not isinstance(row_schema, StructType):
                continue
            if merged_schema is None:
                merged_schema = row_schema
            else:
                merged_schema = _merge_struct_types(merged_schema, row_schema)
        except Exception:
            errors += 1
            continue

    if merged_schema:
        print(f"✅ {table_name}: {len(merged_schema.fields)} fields merged from {len(samples)} samples ({errors} errors)")
    return merged_schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Descobrir tabelas bronze com json_data

# COMMAND ----------

rows = spark.sql(f"""
    SELECT table_name
    FROM system.information_schema.tables
    WHERE table_catalog = '{CATALOG}'
      AND table_schema = 'bronze'
      AND table_type = 'MATERIALIZED_VIEW'
      AND table_name NOT LIKE '__materialization%'
    ORDER BY table_name
""").collect()

bronze_tables = []
for r in rows:
    try:
        columns = spark.read.table(f"{CATALOG}.bronze.{r.table_name}").columns
        if "json_data" in columns:
            bronze_tables.append(r.table_name)
    except Exception as e:
        print(f"⚠️ Skip {r.table_name}: {e}")

print(f"\n🔍 Found {len(bronze_tables)} tables with json_data:")
for t in bronze_tables:
    print(f"  • {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Testar flatten para cada tabela (primeiras 5)

# COMMAND ----------

MAX_TABLES_TO_TEST = 5  # ajuste para testar mais/menos tabelas
MAX_ROWS_DISPLAY = 20   # linhas por display

results_summary = []

for table_name in bronze_tables[:MAX_TABLES_TO_TEST]:
    print(f"\n{'='*80}")
    print(f"📋 TABLE: {table_name}")
    print(f"{'='*80}")

    # 1. Ler bronze
    df = spark.read.table(f"{BRONZE_DB}.{table_name}")
    total_rows = df.count()
    print(f"   Total rows: {total_rows}")

    # 2. Inferir super-schema
    schema = infer_super_schema(spark, table_name, sample_size=100)
    if schema is None:
        print(f"   ❌ Could not infer schema — SKIPPING")
        results_summary.append((table_name, total_rows, 0, "NO SCHEMA"))
        continue

    # 3. Parse JSON com super-schema
    meta_cols = [c for c in df.columns if c != "json_data"]
    df_parsed = df.withColumn("data", F.from_json(F.col("json_data"), schema))

    # 4. Contar NULLs após parse (indica JSON que não casou com schema)
    null_count = df_parsed.filter(F.col("data").isNull()).count()
    valid_count = total_rows - null_count
    print(f"   ✅ Parsed OK: {valid_count} | ⚠️ NULL (parse failed): {null_count}")

    df_parsed = df_parsed.filter(F.col("data").isNotNull())

    # 5. Flatten
    flatten_cols = flatten_schema(
        schema,
        prefix="",
        parent_col="data",
        apply_camel_case=True,
        skip_names=set(meta_cols)
    )

    if not flatten_cols:
        print(f"   ⚠️ No flatten columns generated")
        results_summary.append((table_name, total_rows, 0, "NO COLUMNS"))
        continue

    df_flat = df_parsed.select(*[F.col(c) for c in meta_cols], *flatten_cols)

    print(f"   📊 Output columns: {len(df_flat.columns)}")
    print(f"   Columns: {df_flat.columns[:20]}{'...' if len(df_flat.columns) > 20 else ''}")

    results_summary.append((table_name, total_rows, len(df_flat.columns), "OK"))

    # 6. Display
    display(df_flat.limit(MAX_ROWS_DISPLAY))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resumo

# COMMAND ----------

print(f"\n{'='*80}")
print(f"📊 SUMMARY")
print(f"{'='*80}")
print(f"{'Table':<40} {'Rows':>8} {'Cols':>6} {'Status':>12}")
print(f"{'-'*40} {'-'*8} {'-'*6} {'-'*12}")
for name, rows, cols, status in results_summary:
    emoji = "✅" if status == "OK" else "❌"
    print(f"{name:<40} {rows:>8} {cols:>6} {emoji} {status:>10}")

print(f"\nTotal tables tested: {len(results_summary)}")
print(f"Total tables available: {len(bronze_tables)}")
