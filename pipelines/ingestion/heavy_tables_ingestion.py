# Databricks notebook source
# COMMAND ----------
# CELL 1: Imports and Initial Setup
# =============================

print("=" * 80)
print("HEAVY TABLES INGESTION - START")
print("=" * 80)

import yaml
from datetime import datetime, timedelta
from pyspark.sql import functions as F

print("✓ Imports loaded successfully")

# COMMAND ----------
# CELL 2: Configuration and Environment Detection
# =============================

print("\n" + "=" * 80)
print("CONFIGURATION")
print("=" * 80)

# Setup widgets for job parameters
try:
    dbutils.widgets.text("environment", "dev", "Environment")
    dbutils.widgets.text("catalog_public", "", "Public Catalog Name")
    
    ENVIRONMENT = dbutils.widgets.get("environment").lower()
    CATALOG_PUBLIC = dbutils.widgets.get("catalog_public")
    
    # If catalog not provided via widget, construct default
    if not CATALOG_PUBLIC:
        CATALOG_PUBLIC = f"public_vcn_{ENVIRONMENT}"
    
    print(f"✓ Running as JOB")
    print(f"  - Environment: {ENVIRONMENT}")
    print(f"  - Catalog: {CATALOG_PUBLIC}")
    
except Exception as e:
    print(f"✓ Running as PIPELINE (DLT mode)")
    print(f"  - Exception: {str(e)[:100]}")
    # Fallback to pipeline config if widgets not available (DLT pipeline context)
    ENVIRONMENT = spark.conf.get("pipeline.env", "dev").lower()
    CATALOG_PUBLIC = spark.conf.get("catalog_public", f"public_vcn_{ENVIRONMENT}")
    print(f"  - Environment: {ENVIRONMENT}")
    print(f"  - Catalog: {CATALOG_PUBLIC}")

SOURCE_CATALOG = "vcn-federated"  # federated catalog name
RAW_SCHEMA = "raw"

JDBC_FETCHSIZE = 10000
# Use fixed parallelism to avoid sparkContext access (not supported in serverless)
DEFAULT_PARALLELISM = 8

print(f"\n✓ Configuration loaded:")
print(f"  - Source Catalog: {SOURCE_CATALOG}")
print(f"  - RAW Schema: {RAW_SCHEMA}")
print(f"  - JDBC Fetch Size: {JDBC_FETCHSIZE}")
print(f"  - Default Parallelism: {DEFAULT_PARALLELISM}")

# COMMAND ----------
# CELL 3: Load YAML Configuration
# =============================

print("\n" + "=" * 80)
print("LOADING YAML CONFIGURATION")
print("=" * 80)

config_path = (
    f"/Workspace/Repos/sp_b2b_ops_bot/b2b-databricks-dlt-{ENVIRONMENT}/config/tables_vcn_public.yaml"
)

print(f"✓ Config path: {config_path}")

try:
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    all_tables = config.get("tables", [])
    TABLES = [t for t in all_tables if t.get("heavy", False)]
    
    print(f"✓ YAML loaded successfully")
    print(f"  - Total tables in config: {len(all_tables)}")
    print(f"  - Heavy tables found: {len(TABLES)}")
    print(f"\n✓ Heavy tables to ingest:")
    for i, t in enumerate(TABLES, 1):
        print(f"  {i}. {t['schema']}.{t['name']}")
        print(f"     - PK: {t.get('partition_column') or t.get('pk')}")
        print(f"     - Watermark: {t.get('watermark')}")
        print(f"     - Watermark days: {t.get('watermark_days', 180)}")
        
except Exception as e:
    print(f"✗ ERROR loading YAML: {str(e)}")
    raise

# COMMAND ----------
# CELL 4: JDBC Configuration
# =============================

print("\n" + "=" * 80)
print("JDBC CONNECTION SETUP")
print("=" * 80)

try:
    jdbc_url = spark.conf.get(
        f"spark.databricks.sql.federation.catalog.{SOURCE_CATALOG}.url"
    )
    
    print(f"✓ Retrieved JDBC URL from federated catalog")
    print(f"  - Catalog: {SOURCE_CATALOG}")
    print(f"  - URL (masked): {jdbc_url[:30]}...{jdbc_url[-20:]}")
    
    if "?" in jdbc_url:
        jdbc_url = f"{jdbc_url}&socketTimeout=0&connectTimeout=10"
    else:
        jdbc_url = f"{jdbc_url}?socketTimeout=0&connectTimeout=10"
    
    base_options = {
        "url": jdbc_url,
        "driver": "org.postgresql.Driver",
        "fetchsize": str(JDBC_FETCHSIZE),
        "queryTimeout": "0",
        "sessionInitStatement": "SET statement_timeout = 0",
    }
    
    print(f"✓ JDBC options configured:")
    print(f"  - Driver: {base_options['driver']}")
    print(f"  - Fetch size: {base_options['fetchsize']}")
    print(f"  - Query timeout: {base_options['queryTimeout']} (unlimited)")
    print(f"  - Session init: {base_options['sessionInitStatement']}")
    
except Exception as e:
    print(f"✗ ERROR configuring JDBC: {str(e)}")
    raise

# COMMAND ----------
# CELL 5: Helper Functions
# =============================

print("\n" + "=" * 80)
print("DEFINING HELPER FUNCTIONS")
print("=" * 80)

def get_last_watermark(raw_table, watermark_col):
    """Get the maximum watermark value from existing RAW table"""
    if not spark.catalog.tableExists(raw_table):
        print(f"  ⓘ Table does not exist yet: {raw_table}")
        return None
    
    result = (
        spark.read.table(raw_table)
        .agg(F.max(watermark_col).alias("wm"))
        .collect()[0]["wm"]
    )
    
    if result:
        print(f"  ✓ Last watermark found: {result}")
    else:
        print(f"  ⓘ Table exists but no watermark data found")
    
    return result


def get_bounds(schema, table, column, where_clause=None):
    """Get MIN/MAX bounds for partition column from source table"""
    query = (
        f"(SELECT MIN({column}) AS min_v, MAX({column}) AS max_v FROM {schema}.{table}"
    )
    if where_clause:
        query += f" WHERE {where_clause}"
    query += ") AS bounds"
    
    print(f"  ⓘ Fetching bounds from source...")
    print(f"    Query: SELECT MIN/MAX({column}) WHERE {where_clause[:50] if where_clause else 'NO FILTER'}...")
    
    try:
        row = (
            spark.read.format("jdbc")
            .options(**base_options)
            .option("dbtable", query)
            .load()
            .first()
        )
        
        if not row or not row["min_v"] or not row["max_v"]:
            print(f"  ⓘ No data found matching filter criteria")
            return None
        
        bounds = (int(row["min_v"]), int(row["max_v"]))
        print(f"  ✓ Bounds: MIN={bounds[0]:,} | MAX={bounds[1]:,} | Range={bounds[1]-bounds[0]:,}")
        return bounds
        
    except Exception as e:
        print(f"  ✗ ERROR fetching bounds: {str(e)}")
        raise

print("✓ Helper functions defined successfully")

# COMMAND ----------
# CELL 6: Main Ingestion Loop
# =============================

print("\n" + "=" * 80)
print("STARTING HEAVY TABLE INGESTION")
print("=" * 80)

ingestion_results = []
start_time = datetime.utcnow()

for idx, table in enumerate(TABLES, 1):
    table_start = datetime.utcnow()
    
    print("\n" + "-" * 80)
    print(f"TABLE {idx}/{len(TABLES)}: Processing heavy table")
    print("-" * 80)
    
    name = table["name"]
    schema = table["schema"]
    pk = table.get("partition_column") or table.get("pk")
    watermark = table.get("watermark")
    watermark_days = int(table.get("watermark_days", 180))

    source_table = f"{schema}.{name}"
    raw_table = f"{CATALOG_PUBLIC}.{RAW_SCHEMA}.{name}"

    print(f"✓ Table info:")
    print(f"  - Source: {source_table}")
    print(f"  - Target: {raw_table}")
    print(f"  - Partition Column (PK): {pk}")
    print(f"  - Watermark Column: {watermark}")
    print(f"  - Watermark Days: {watermark_days}")
    
    # Check last watermark
    print(f"\n⏱ Checking last ingestion watermark...")
    try:
        last_wm = get_last_watermark(raw_table, watermark)
    except Exception as e:
        print(f"  ✗ ERROR checking watermark: {str(e)}")
        ingestion_results.append({
            "table": source_table,
            "status": "ERROR",
            "error": f"Failed to check watermark: {str(e)[:100]}"
        })
        continue

    # Determine filter
    if last_wm:
        cutoff = last_wm.strftime("%Y-%m-%d %H:%M:%S")
        where_filter = f"{watermark} > TIMESTAMP '{cutoff}'"
        load_type = "INCREMENTAL"
        print(f"  ✓ Incremental load")
        print(f"    Cutoff: {cutoff}")
    else:
        cutoff = (datetime.utcnow() - timedelta(days=watermark_days)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        where_filter = f"{watermark} >= TIMESTAMP '{cutoff}'"
        load_type = "INITIAL"
        print(f"  ✓ Initial load (last {watermark_days} days)")
        print(f"    Cutoff: {cutoff}")
    
    print(f"    Filter: {where_filter}")

    # Get partition bounds
    print(f"\n📊 Calculating partition bounds...")
    try:
        bounds = get_bounds(schema, name, pk, where_filter)
    except Exception as e:
        print(f"  ✗ ERROR fetching bounds: {str(e)}")
        ingestion_results.append({
            "table": source_table,
            "status": "ERROR",
            "error": f"Failed to fetch bounds: {str(e)[:100]}"
        })
        continue

    if not bounds:
        print(f"  ⓘ No new data found - SKIPPING")
        ingestion_results.append({
            "table": source_table,
            "status": "SKIPPED",
            "reason": "No new data"
        })
        continue

    lower, upper = bounds
    num_parts = min(DEFAULT_PARALLELISM, max(4, (upper - lower) // 1_000_000))
    
    print(f"  ✓ Partitioning strategy:")
    print(f"    Number of partitions: {num_parts}")
    print(f"    Rows per partition (est): {(upper - lower) // num_parts:,}")

    # Read from JDBC
    print(f"\n📥 Reading data from PostgreSQL via JDBC...")
    query = f"(SELECT * FROM {schema}.{name} WHERE {where_filter}) AS src"
    print(f"  Query: {query[:80]}...")
    
    try:
        read_start = datetime.utcnow()
        df = (
            spark.read.format("jdbc")
            .options(**base_options)
            .option("dbtable", query)
            .option("partitionColumn", pk)
            .option("lowerBound", str(lower))
            .option("upperBound", str(upper))
            .option("numPartitions", str(num_parts))
            .load()
        )
        
        print(f"  ✓ DataFrame loaded")
        print(f"    Schema columns: {len(df.columns)}")
        
        # Count rows (triggers execution)
        print(f"\n🔢 Counting rows (executing query)...")
        row_count = df.count()
        read_duration = (datetime.utcnow() - read_start).total_seconds()
        
        print(f"  ✓ Read complete: {row_count:,} rows in {read_duration:.2f}s")
        if read_duration > 0:
            print(f"    Throughput: {row_count / read_duration:,.0f} rows/sec")
        
    except Exception as e:
        print(f"  ✗ ERROR reading data: {str(e)}")
        ingestion_results.append({
            "table": source_table,
            "status": "ERROR",
            "error": f"Failed to read data: {str(e)[:100]}"
        })
        continue

    # Write to Delta
    print(f"\n💾 Writing to Delta Lake...")
    print(f"  Target: {raw_table}")
    print(f"  Mode: APPEND")
    
    try:
        write_start = datetime.utcnow()
        (
            df.withColumn("_ingestion_ts", F.current_timestamp())
            .write.format("delta")
            .mode("append")
            .saveAsTable(raw_table)
        )
        write_duration = (datetime.utcnow() - write_start).total_seconds()
        
        print(f"  ✓ Write complete in {write_duration:.2f}s")
        if write_duration > 0:
            print(f"    Throughput: {row_count / write_duration:,.0f} rows/sec")
        
        table_duration = (datetime.utcnow() - table_start).total_seconds()
        print(f"\n✅ TABLE COMPLETE: {source_table}")
        print(f"    Total time: {table_duration:.2f}s")
        print(f"    Rows ingested: {row_count:,}")
        print(f"    Load type: {load_type}")
        
        ingestion_results.append({
            "table": source_table,
            "status": "SUCCESS",
            "rows": row_count,
            "duration_sec": table_duration,
            "load_type": load_type
        })
        
    except Exception as e:
        print(f"  ✗ ERROR writing data: {str(e)}")
        ingestion_results.append({
            "table": source_table,
            "status": "ERROR",
            "error": f"Failed to write data: {str(e)[:100]}"
        })
        continue

# COMMAND ----------
# CELL 7: Final Summary
# =============================

total_duration = (datetime.utcnow() - start_time).total_seconds()

print("\n" + "=" * 80)
print("HEAVY INGESTION SUMMARY")
print("=" * 80)

success_count = sum(1 for r in ingestion_results if r["status"] == "SUCCESS")
skipped_count = sum(1 for r in ingestion_results if r["status"] == "SKIPPED")
error_count = sum(1 for r in ingestion_results if r["status"] == "ERROR")
total_rows = sum(r.get("rows", 0) for r in ingestion_results if r["status"] == "SUCCESS")

print(f"\n📊 Overall Statistics:")
print(f"  - Total tables processed: {len(TABLES)}")
print(f"  - Successful: {success_count}")
print(f"  - Skipped (no data): {skipped_count}")
print(f"  - Errors: {error_count}")
print(f"  - Total rows ingested: {total_rows:,}")
print(f"  - Total duration: {total_duration:.2f}s ({total_duration/60:.2f} min)")
if total_duration > 0 and total_rows > 0:
    print(f"  - Overall throughput: {total_rows / total_duration:,.0f} rows/sec")

print(f"\n📋 Detailed Results:")
for r in ingestion_results:
    status_icon = "✅" if r["status"] == "SUCCESS" else ("⚠️" if r["status"] == "SKIPPED" else "❌")
    print(f"\n  {status_icon} {r['table']}")
    print(f"     Status: {r['status']}")
    if r["status"] == "SUCCESS":
        print(f"     Rows: {r['rows']:,}")
        print(f"     Duration: {r['duration_sec']:.2f}s")
        print(f"     Load Type: {r['load_type']}")
    elif r["status"] == "SKIPPED":
        print(f"     Reason: {r.get('reason', 'Unknown')}")
    elif r["status"] == "ERROR":
        print(f"     Error: {r.get('error', 'Unknown error')}")

if error_count > 0:
    print(f"\n⚠️  WARNING: {error_count} table(s) failed. Check errors above.")
else:
    print(f"\n🎉 All tables processed successfully!")

print("\n" + "=" * 80)
print("HEAVY INGESTION FINISHED")
print("=" * 80)
