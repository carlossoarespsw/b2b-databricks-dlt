import dlt
import yaml
from pyspark.sql.functions import col, current_timestamp

# ================================
# CONFIG
# ================================
config_path = "/dbfs/FileStore/config/tables_vcn_b2b.yaml"

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

SOURCE_CATALOG = config["source_catalog"]
DEFAULTS = config.get("defaults", {})
TABLES = config.get("tables", [])

ENVIRONMENT = spark.conf.get("pipeline.env", "dev")
SOURCE_SYSTEM = "VCN_B2B"

# ================================
# FUNÇÃO PARA GERAR TABELAS
# ================================
def create_table(table_conf):
    schema_name = table_conf["schema"]
    table_name = table_conf["name"]
    keys = table_conf.get("keys")
    strategy = table_conf.get("strategy", DEFAULTS.get("strategy", "snapshot"))
    watermark = table_conf.get("watermark_column", DEFAULTS.get("watermark_column"))

    source_fqn = f"{SOURCE_CATALOG}.{schema_name}.{table_name}"

    # ---------------- SNAPSHOT ----------------
    if strategy != "incremental":

        @dlt.table(
            name=table_name,
            comment=f"Snapshot from {source_fqn}",
            table_properties={
                "quality": "bronze",
                "source_system": SOURCE_SYSTEM,
                "environment": ENVIRONMENT,
            },
        )
        def snapshot():
            return (
                spark.read.table(source_fqn)
                .withColumn("_ingestion_ts", current_timestamp())
            )

    # ---------------- INCREMENTAL (CDC) ----------------
    else:
        view_name = f"vw_{table_name}"

        @dlt.view(name=view_name)
        def source_view():
            return spark.read.table(source_fqn)

        dlt.create_streaming_table(
            name=table_name,
            comment=f"Incremental from {source_fqn}",
            table_properties={
                "quality": "bronze",
                "source_system": SOURCE_SYSTEM,
                "environment": ENVIRONMENT,
            },
        )

        dlt.apply_changes(
            target=table_name,
            source=view_name,
            keys=keys,
            sequence_by=col(watermark),
            stored_as_scd_type=1,
        )


# ================================
# GERAR TODAS AS TABELAS
# ================================
for t in TABLES:
    create_table(t)
