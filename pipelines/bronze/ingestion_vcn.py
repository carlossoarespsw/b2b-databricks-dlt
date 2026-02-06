# Databricks notebook source

import re
from pathlib import Path
from datetime import datetime

import dlt
import yaml
from pyspark.sql.functions import col

ENVIRONMENT = spark.conf.get("pipeline.env", "dev")
DEV_SAMPLE_LIMIT = int(spark.conf.get("pipeline.dev_sample_limit", "100000"))
SOURCE_SYSTEM = "VCN"

# COMMAND ----------

def fqtn(catalog: str, schema: str, table: str) -> str:
    """Return a UC-safe fully-qualified table name with backticks."""
    return f"`{catalog}`.`{schema}`.`{table}`"


def is_clean_table_name(name: str) -> bool:
    if name is None:
        return False
    if name.lower().endswith("_bkp"):
        return False
    if re.search(r"\d{8}", name):
        return False
    return True


def resolve_config_path() -> Path:
    candidates = [
        Path("config/tables_vcn.yaml"),
        Path("../config/tables_vcn.yaml"),
        Path("../../config/tables_vcn.yaml"),
    ]

    if "__file__" in globals():
        base = Path(__file__).resolve().parent
        candidates.extend([
            base.parent / "config" / "tables_vcn.yaml",
            base.parent.parent / "config" / "tables_vcn.yaml",
        ])

    for c in candidates:
        if c.exists():
            return c
    raise FileNotFoundError("tables_vcn.yaml not found")


config_path = resolve_config_path()
with open(config_path, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

SOURCE_CATALOG = config.get("source_catalog")
TABLES = [t for t in config.get("tables", []) if is_clean_table_name(t.get("name", ""))]

# COMMAND ----------

@dlt.table(
    name="vcn_metadata",
    comment="VCN pipeline execution metadata",
    table_properties={
        "quality": "bronze",
        "type": "metadata",
        "source_system": SOURCE_SYSTEM,
        "environment": ENVIRONMENT,
    },
)
def vcn_metadata():
    return spark.createDataFrame(
        [
            (
                ENVIRONMENT,
                SOURCE_CATALOG,
                datetime.utcnow().isoformat(),
                len(TABLES),
            )
        ],
        ["environment", "source_catalog", "execution_time", "tables_discovered"],
    )

# COMMAND ----------

for table in TABLES:
    table_name = table["name"]
    schema_name = table.get("schema", "public")
    strategy = table.get("strategy", "snapshot")

    target_table_name = f"{schema_name}_{table_name}"
    source_table_fqtn = fqtn(SOURCE_CATALOG, schema_name, table_name)

    if strategy == "scd_type_1":

        view_name = f"view_{target_table_name}"

        @dlt.view(name=view_name)
        def source_view(src=source_table_fqtn):
            df = spark.read.table(src)
            return df.limit(DEV_SAMPLE_LIMIT) if ENVIRONMENT == "dev" else df

        dlt.create_streaming_table(
            name=target_table_name,
            comment=f"SCD Type 1 merge from {source_table_fqtn}",
            table_properties={
                "quality": "bronze",
                "source_system": SOURCE_SYSTEM,
                "environment": ENVIRONMENT,
            },
        )

        dlt.apply_changes(
            target=target_table_name,
            source=view_name,
            keys=table["primary_keys"],
            sequence_by=col(table["watermark_column"]),
            stored_as_scd_type=1,
        )

    else:

        @dlt.table(
            name=target_table_name,
            comment=f"Snapshot from {source_table_fqtn}",
            table_properties={
                "quality": "bronze",
                "source_system": SOURCE_SYSTEM,
                "environment": ENVIRONMENT,
                "pipelines.autoOptimize.managed": "true",
            },
        )
        def snapshot_table(src=source_table_fqtn):
            df = spark.read.table(src)
            return df.limit(DEV_SAMPLE_LIMIT) if ENVIRONMENT == "dev" else df
