from __future__ import annotations

from pathlib import Path
import dlt
import yaml
from pyspark.sql.functions import col, current_timestamp

ENVIRONMENT = spark.conf.get("pipeline.env", "dev")
DEV_SAMPLE_LIMIT = 100_000
SOURCE_SYSTEM = "VCN"


def fqtn(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "tables_vcn.yaml"
with CONFIG_PATH.open("r", encoding="utf-8") as handle:
    config = yaml.safe_load(handle)

SOURCE_CATALOG = config["source_catalog"]
DEFAULTS = config.get("defaults", {})
TABLES = config.get("tables", [])


def _read_source(catalog: str, schema: str, table: str):
    df = spark.read.table(fqtn(catalog, schema, table))
    df = df.withColumn("_ingestion_ts", current_timestamp())

    if ENVIRONMENT == "dev":
        return df.limit(DEV_SAMPLE_LIMIT)
    return df


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


for table in TABLES:
    schema_name = table["schema"]
    table_name = table["name"]

    strategy = table.get("strategy", DEFAULTS.get("strategy", "snapshot"))
    watermark = table.get("watermark_column", DEFAULTS.get("watermark_column"))

    target_table_name = f"{schema_name}__{table_name}"
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
            apply_as_deletes=False,
        )

    else:
        make_snapshot_table(SOURCE_CATALOG, schema_name, table_name, target_table_name, source_fq)
