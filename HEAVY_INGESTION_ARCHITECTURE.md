# Heavy Tables Ingestion Architecture

## Overview

Large PostgreSQL tables (15M+ rows, 2017-2026 data) are ingested via **standalone Spark jobs** outside DLT to avoid timeout and materialization overhead.

## Architecture

```
┌──────────────┐     ┌────────────────────────┐     ┌──────────────────┐
│  PostgreSQL  │────▶│ Heavy Ingestion Job    │────▶│  Bronze Layer    │
│              │     │ (Spark Standalone)     │     │  (Delta Tables)  │
│ 15M+ rows    │     │ - Watermark Incremental│     │  heavy tables    │
└──────────────┘     │ - JDBC Partitioning    │     └──────────────────┘
                     └────────────────────────┘

┌──────────────┐     ┌────────────────────────┐     ┌──────────────────┐
│  PostgreSQL  │────▶│  DLT Bronze Pipeline   │────▶│  Bronze Layer    │
│              │     │  (Federated/JDBC)      │     │  (Delta Tables)  │
│ Light tables │     │  - Normal tables only  │     │  light tables    │
└──────────────┘     └────────────────────────┘     └──────────────────┘
                                                               │
                            ┌──────────────────────────────────┘
                            ▼
                     ┌────────────────────────┐
                     │  DLT Silver/Gold       │
                     │  (All Bronze tables)   │
                     └────────────────────────┘
```

## Components

### 1. Heavy Ingestion Job
**File**: `tasks/ingestion/heavy_tables_ingestion.py`

**Purpose**: Ingest large tables from PostgreSQL directly to Delta Bronze layer

**Features**:
- ✅ Watermark-based incremental loading (only changed rows)
- ✅ First load: configurable window (default 180 days)
- ✅ Subsequent loads: only rows since last watermark
- ✅ JDBC partitioning for parallelism (by PK)
- ✅ Dynamic partition count based on data range
- ✅ Index-optimized queries (TIMESTAMP literals)
- ✅ Comprehensive error handling and metrics
- ✅ No DLT overhead - pure Spark job
- ✅ Writes directly to Bronze (no RAW intermediary)

**Target Tables**:
- `{env}_vcn_public.bronze.vcn_request`
- `{env}_vcn_public.bronze.vcn_request_financial_trans`

**Configuration**: `config/tables_vcn_public.yaml` (same as DLT, filters `heavy: true`)

### 2. DLT Bronze Pipeline (Modified)
**File**: `tasks/bronze/vcn_public_pipeline.py`

**Changes**:
- Heavy tables (marked `heavy: true` in YAML) are **skipped**
- Only non-heavy tables are ingested via federated catalog/JDBC
- No timeout issues, no JDBC overhead for heavy tables
- Heavy tables are already in Bronze (from standalone job)

## Configuration

### tables_vcn_public.yaml (Single Source of Truth)

Both DLT pipeline and Heavy Ingestion Job read from the **same YAML file**.

**Heavy tables** are marked with `heavy: true`:
```yaml
source_catalog: "vcn-federated"

tables:
  # ... other tables (processed by DLT) ...
  
  - name: vcn_request
    schema: public
    pk: vcn_request_id
    watermark: last_modified
    heavy: true              # ← Marks as heavy table
    watermark_days: 180      # ← Optional: override default 180 days
    
  - name: vcn_request_financial_trans
    schema: public
    pk: vcn_request_financial_trans_id
    watermark: last_modified
    heavy: true              # ← Marks as heavy table
```

**Behavior**:
- **DLT Pipeline**: Skips tables with `heavy: true` (line 212 in vcn_public_pipeline.py)
- **Heavy Job**: Filters only tables with `heavy: true`
- **Result**: No duplication, single source of configuration

## Orchestration

### Option 1: Databricks Workflows (Recommended)

Create a workflow with 2 independent tasks (can run in parallel):

```yaml
Workflow: VCN Public Ingestion
├─ Task 1: Heavy Ingestion (Independent)
│  ├─ Type: Python (Notebook)
│  ├─ Notebook: tasks/ingestion/heavy_tables_ingestion.py
│  ├─ Cluster: Job Compute (4-8 workers)
│  └─ Schedule: Daily 2 AM
│
└─ Task 2: DLT Pipeline Light Tables (Independent)
   ├─ Type: DLT Pipeline
   ├─ Pipeline: vcn_public_bronze
   ├─ Schedule: Daily 2 AM
   └─ Note: Skips heavy tables automatically
```

**Note**: Tasks can run in parallel since heavy job writes directly to Bronze.

**Create Workflow**:
```bash
# Via Databricks CLI or Workflows UI
databricks workflows create --json-file workflow_config.json
```

### Option 2: Manual Execution

```python
# 1. Run heavy ingestion job
dbutils.notebook.run(
    "/Repos/sp_b2b_ops_bot/b2b-databricks-dlt/tasks/ingestion/heavy_tables_ingestion",
    timeout_seconds=3600,
    arguments={"job.env": "dev"}
)

# 2. Trigger DLT pipeline
# Via UI or API
```

## Incremental Loading Logic

### First Load
```sql
-- Loads last 180 days (configurable)
SELECT * FROM public.vcn_request
WHERE last_modified >= TIMESTAMP '2025-08-13 00:00:00'
```

### Subsequent Loads
```sql
-- Only new/updated rows
SELECT * FROM public.vcn_request
WHERE last_modified > TIMESTAMP '2026-02-09 15:30:45'  -- max from RAW table
```

### Partition Bounds (for parallelism)
```sql
-- Gets MIN/MAX PK filtered by watermark
SELECT MIN(vcn_request_id), MAX(vcn_request_id)
FROM public.vcn_request
WHERE last_modified >= TIMESTAMP '2026-02-09 15:30:45'
```

## Performance Expectations

### Heavy Ingestion Job
- **Initial Load** (180 days): ~2-5M rows, 5-15 minutes
- **Incremental Load** (1 day): ~50-100K rows, 1-3 minutes
- **Throughput**: ~10K-50K rows/second (depends on cluster)
- **Parallelism**: Auto-scaled (4-32 partitions based on range)

### DLT Bronze Pipeline
- **Heavy tables**: <1 minute (Delta read only)
- **Normal tables**: 2-5 minutes (federated/JDBC)
- **Total**: 3-6 minutes (down from 15+ minutes with timeouts)

## Monitoring

### Job Metrics
The heavy ingestion job outputs:
```
✅ Successfully ingested vcn_request
   Rows: 250,000
   Duration: 45.2s
   Throughput: 5,531 rows/s
```

### Check Bronze Layer (Heavy Tables)
```sql
-- Verify incremental loading for heavy tables
SELECT 
  MAX(last_modified) as last_watermark,
  MAX(_ingestion_ts) as last_ingestion,
  MAX(_ingestion_batch) as last_batch,
  COUNT(*) as total_rows
FROM dev_vcn_public.bronze.vcn_request;
```

### Check Bronze Layer (Light Tables)
```sql
-- Verify DLT ingested light tables
SELECT 
  COUNT(*) as bronze_rows,
  MAX(_ingestion_ts) as last_ingestion
FROM dev_vcn_public.bronze.{light_table_name};
```

## Troubleshooting

### Job Fails: "No JDBC URL found"
- Ensure federated catalog is configured
- Check `spark.databricks.sql.federation.catalog.{catalog}.url`

### Job Skips: "No new data"
- Check watermark column has new values
- Query PostgreSQL: `SELECT MAX(last_modified) FROM table`
- Compare with Bronze layer: `SELECT MAX(last_modified) FROM bronze.table`

### DLT Silver/Gold Fails: "Table not found: bronze.vcn_request"
- Heavy ingestion job must run first to create Bronze tables
- Verify job completed successfully
- Check Bronze schema exists: `SHOW TABLES IN {catalog}.bronze`

### Slow Performance
- Check cluster size (4-8 workers recommended)
- Verify index on watermark column: `xu2_vcn_request_financial_trans`
- Monitor partition count (should be 4-32)
- Check PostgreSQL load (concurrent queries)

## Migration Steps

### 1. ~~Create RAW Schema~~ (Not needed - writes to Bronze)

### 2. Ensure Configuration
Verify `tables_vcn_public.yaml` has heavy tables marked:
```yaml
- name: vcn_request
  heavy: true  # Must be set
  
- name: vcn_request_financial_trans
  heavy: true  # Must be set
```

### 3. Run Initial Load
```bash
# Run heavy ingestion job manually first time
# This will create Bronze tables and load initial 180 days
```

### 4. Update DLT Pipeline
- DLT will automatically skip heavy tables (already modified)
- No additional notebooks needed - heavy tables already in Bronze

### 5. Setup Orchestration
- Create Databricks Workflow as described above
- Schedule: Heavy job → DLT pipeline

### 6. Monitor & Validate
- Check job metrics and success rates
- Compare row counts between RAW and Bronze
- Validate data freshness

## Benefits

✅ **No Timeouts**: Separate job controls timing, no DLT 20-min limit
✅ **True Incremental**: Watermark tracking with Delta checkpoint
✅ **Isolation**: Heavy tables don't block light tables
✅ **Scalability**: Independent cluster sizing for heavy ingestion
✅ **Observability**: Detailed metrics and error handling
✅ **Maintainability**: Clear separation of concerns
✅ **Fault Tolerance**: Job retry without full pipeline restart
✅ **No Duplication**: Single copy in Bronze (no RAW intermediary)
✅ **Cost Efficient**: Less storage, less compute for DLT
✅ **Consistency**: All tables (heavy and light) in same Bronze layer
