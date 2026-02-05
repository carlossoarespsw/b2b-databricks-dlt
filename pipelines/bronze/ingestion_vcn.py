# Databricks notebook source

import re
from pathlib import Path

import dlt
import yaml
from pyspark.sql.functions import col, current_timestamp, lit

ENVIRONMENT = spark.conf.get("pipeline.env", "dev")
DEV_SAMPLE_LIMIT = int(spark.conf.get("pipeline.dev_sample_limit", "100000"))
SOURCE_SYSTEM = "VCN"

# COMMAND ----------

def _resolve_config_path() -> Path:
    candidates = []
    if "__file__" in globals():
        base_dir = Path(__file__).resolve().parent
        candidates.extend([
            base_dir.parent.parent / "config" / "tables_vcn.yaml",
            base_dir.parent / "config" / "tables_vcn.yaml",
        ])
    candidates.extend([
        Path("config") / "tables_vcn.yaml",
        Path("../config/tables_vcn.yaml"),
        Path("../../config/tables_vcn.yaml"),
    ])

    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError("tables_vcn.yaml not found. Check pipeline source path.")


def _is_clean_table_name(name: str) -> bool:
    if name.lower().endswith("_bkp"):
        return False
    if re.search(r"\d{8}", name):
        return False
    return True


config_path = _resolve_config_path()
with open(config_path, "r", encoding="utf-8") as config_file:
    config = yaml.safe_load(config_file)

SOURCE_CATALOG = config.get("source_catalog", "vcn-federated")
DEST_CATALOG = config.get("target_catalog", "vcn-catalog")
DEST_SCHEMA = config.get("target_schema", "bronze")
TABLES = [t for t in config.get("tables", []) if _is_clean_table_name(t.get("name", ""))]

# COMMAND ----------

# Dummy table to ensure pipeline is valid (at least one table required)
@dlt.table(
    name="vcn_metadata",
    catalog=DEST_CATALOG,
    schema=DEST_SCHEMA,
    comment="VCN Pipeline Metadata",
    table_properties={"quality": "bronze", "type": "metadata"}
)
def vcn_metadata_table():
    from datetime import datetime
    return spark.createDataFrame([
        (ENVIRONMENT, SOURCE_CATALOG, datetime.now().isoformat(), len(TABLES))
    ], ["environment", "source_catalog", "execution_time", "tables_discovered"])

# COMMAND ----------

for table_info in TABLES:
    table_name = table_info["name"]
    schema_name = table_info.get("schema", "public")
    strategy = table_info.get("strategy", "full_limit")
    target_table_name = f"{schema_name}_{table_name}"
    source_table = f"{SOURCE_CATALOG}.{schema_name}.{table_name}"

    if strategy == "scd_type_1":
        view_name = f"view_{target_table_name}"

        @dlt.view(name=view_name)
        def read_source(src_table=source_table):
            df = spark.read.table(src_table)
            if ENVIRONMENT == "dev":
                return df.limit(DEV_SAMPLE_LIMIT)
            return df

        dlt.create_streaming_table(
            name=target_table_name,
            catalog=DEST_CATALOG,
            schema=DEST_SCHEMA,
            comment=f"Bronze merge: {source_table}",
            table_properties={
                "quality": "bronze",
                "source_system": SOURCE_SYSTEM,
                "source_catalog": SOURCE_CATALOG,
                "source_schema": schema_name,
                "source_table": table_name,
                "environment": ENVIRONMENT,
            },
        )

        dlt.apply_changes(
            target=f"{DEST_CATALOG}.{DEST_SCHEMA}.{target_table_name}",
            source=view_name,
            keys=table_info["primary_keys"],
            sequence_by=col(table_info["watermark_column"]),
            stored_as_scd_type=1,
        )
    else:
        @dlt.table(
            name=target_table_name,
            catalog=DEST_CATALOG,
            schema=DEST_SCHEMA,
            comment=f"Bronze snapshot: {source_table}",
            table_properties={
                "quality": "bronze",
                "source_system": SOURCE_SYSTEM,
                "pipelines.autoOptimize.managed": "true",
                "source_catalog": SOURCE_CATALOG,
                "source_schema": schema_name,
                "source_table": table_name,
                "environment": ENVIRONMENT,
            },
        )
        def ingestion_simple(src_table=source_table):
            df = spark.read.table(src_table)
            if ENVIRONMENT == "dev":
                return df.limit(DEV_SAMPLE_LIMIT)
            return df
