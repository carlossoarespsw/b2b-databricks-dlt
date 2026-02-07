# Databricks notebook source
from __future__ import annotations

# COMMAND ----------

from pathlib import Path
import dlt
import yaml
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType

ENVIRONMENT = spark.conf.get("pipeline.env", "dev")
DEV_SAMPLE_LIMIT = 100_000
SOURCE_SYSTEM = "VCN"

# COMMAND ----------


def fqtn(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def _resolve_config_path() -> Path:
    try:
        nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        repo_root = (Path("/Workspace") / nb_path.lstrip("/")).parents[2]
        repo_config = repo_root / "config" / "tables_vcn_b2b.yaml"
        if repo_config.exists():
            return repo_config
    except Exception:
        pass

    filestore_config = Path("/dbfs/FileStore/config/tables_vcn_b2b.yaml")
    if filestore_config.exists():
        return filestore_config

    cwd = Path.cwd().resolve()
    for base in [cwd, *cwd.parents]:
        candidate = base / "config" / "tables_vcn_b2b.yaml"
        if candidate.exists():
            return candidate

    raise FileNotFoundError("config/tables_vcn_b2b.yaml not found in repo or FileStore")


config_path = _resolve_config_path()
with config_path.open("r", encoding="utf-8") as handle:
    config = yaml.safe_load(handle)

SOURCE_CATALOG = config["source_catalog"]
DEFAULTS = config.get("defaults", {})
TABLES = config.get("tables", [])

# COMMAND ----------


def _read_source(catalog: str, schema: str, table: str):
    source_name = fqtn(catalog, schema, table)

    try:
        df = spark.read.table(source_name)
        df = df.withColumn("_ingestion_ts", current_timestamp())

        if ENVIRONMENT == "dev":
            return df.limit(DEV_SAMPLE_LIMIT)

        return df

    except Exception as e:
        print(f"ERRO AO LER {source_name}")
        raise e


def make_source_view(catalog, schema_name, table_name, view_name):
    @dlt.view(name=view_name)
    def source_view():
        return _read_source(catalog, schema_name, table_name)
    return source_view


def make_snapshot_table(catalog, schema_name, table_name, target_table_name, source_fq):
    @dlt.table(
        name=target_table_name,
        comment=f"Snapshot from {source_fq}",
        table_properties={
            "quality": "bronze",
            "environment": ENVIRONMENT,
            "source_system": SOURCE_SYSTEM,
        },
    )
    def snapshot_table():
        return _read_source(catalog, schema_name, table_name)
    return snapshot_table

# COMMAND ----------


for table in TABLES:
    schema_name = table["schema"]
    table_name = table["name"]

    strategy = table.get("strategy", DEFAULTS.get("strategy", "snapshot"))
    watermark = table.get("watermark_column", DEFAULTS.get("watermark_column"))

    target_table_name = table_name
    source_fq = fqtn(SOURCE_CATALOG, schema_name, table_name)

    use_incremental = strategy == "incremental" and watermark
    if ENVIRONMENT == "staging":
        use_incremental = False

    if use_incremental:
        view_name = f"vw_{target_table_name}"
        make_source_view(SOURCE_CATALOG, schema_name, table_name, view_name)

        dlt.create_streaming_table(
            name=target_table_name,
            comment=f"Incremental from {source_fq}",
            table_properties={
                "quality": "bronze",
                "environment": ENVIRONMENT,
                "source_system": SOURCE_SYSTEM,
            },
        )

        keys = table.get("keys")
        if not keys:
            raise ValueError(
                f"Tabela {schema_name}.{table_name} está como incremental mas não tem 'keys' definidas no YAML."
            )

        dlt.apply_changes(
            target=target_table_name,
            source=view_name,
            keys=keys,
            sequence_by=col(watermark),
            stored_as_scd_type=1,
        )

    else:
        make_snapshot_table(SOURCE_CATALOG, schema_name, table_name, target_table_name, source_fq)


