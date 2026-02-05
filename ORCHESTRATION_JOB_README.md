# VCN Data Ingestion Orchestration Job

## Overview
This orchestration job handles the VCN data ingestion pipeline:

1. **Task 1**: Create VCN internal catalog with medallion schemas (bronze, silver, gold)
2. **Task 2**: Run VCN ingestion notebook

## Setup Instructions

### Step 1: Deploy Orchestration Notebook
1. In Databricks workspace, create a new notebook at: `/Shared/orchestration_vcn_job`
2. Copy the contents of `orchestration_vcn_job.py` into the notebook
3. Language: Python

### Step 2: Create Databricks Job
1. Go to Workflows → Jobs
2. Click "Create Job"
3. Configure:
   - **Name**: `dlt_orchestration_vcn_pipeline`
   - **Task name**: `run_orchestration`
   - **Type**: Notebook
   - **Notebook path**: `/Shared/orchestration_vcn_job`
   - **Cluster**: Use existing cluster or create new
   - **Parameters**:
     - `environment`: `dev` (or staging/prod)

### Step 3: Configure Parameters
- **environment**: Set to your environment (dev/staging/prod)

### Step 4: Run the Job
- Click "Run now" to execute
- Monitor execution in job runs history

## Execution Flow

```
Job Start
  ├─ Task 1: Create VCN Catalog
  │  ├─ CREATE CATALOG `vcn-catalog`
  │  ├─ CREATE SCHEMA `vcn-catalog`.`bronze`
  │  ├─ CREATE SCHEMA `vcn-catalog`.`silver`
  │  └─ CREATE SCHEMA `vcn-catalog`.`gold`
  │
  └─ Task 2: Run VCN Ingestion
     └─ Execute `/Shared/pipelines/vcn_ingestion` notebook
        ├─ Receives parameters: environment, catalog
        └─ Performs data ingestion into bronze layer

Job Complete
```

## Notebooks Referenced
- **Ingestion Notebook**: `/Shared/pipelines/vcn_ingestion`
  - Should accept `environment` and `catalog` parameters
  - Responsible for loading data into vcn-catalog.bronze

## Configuration
- **SQL Warehouse**: Pro (ID: f877840f8d6e4eca)
- **Catalog**: vcn-catalog
- **Schemas**: bronze, silver, gold
- **Environment**: configurable via parameters

## Troubleshooting

### Catalog Creation Fails
- Check SQL warehouse is running
- Verify credentials have CATALOG create permissions
- Check metastore settings

### Ingestion Notebook Not Found
- Ensure `/Shared/pipelines/vcn_ingestion` exists
- Check notebook path in orchestration job
- Verify notebook accepts `environment` and `catalog` parameters

### Job Fails to Run
- Check job cluster has sufficient resources
- Verify Databricks SQL warehouse is active
- Check service principal permissions

## Scheduling
To schedule job execution:
1. In job configuration, click "Schedule"
2. Set cron expression or UI-based schedule
3. Example: Daily at 2 AM UTC: `0 2 * * ?`

## Related Files
- `pipelines/`: Contains actual pipeline definitions
- `config/`: Configuration files for different environments
- `requirements.txt`: Python dependencies
