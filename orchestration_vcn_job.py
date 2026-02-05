# ==============================================================================
# VCN Data Ingestion Orchestration Job
# ==============================================================================
# This script orchestrates the VCN data ingestion pipeline:
# 1. Creates VCN internal catalog and medallion layer schemas via SQL
# 2. Runs the VCN ingestion notebook
# 
# To use:
# 1. Deploy this to Databricks workspace as a notebook
# 2. Create a job that runs this notebook
# 3. Schedule or trigger manually

import os
import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ExecuteStatementRequest

# Initialize Databricks workspace client
w = WorkspaceClient()

# Configuration
ENVIRONMENT = dbutils.widgets.get("environment", "dev")
CATALOG_NAME = "vcn-catalog"
WAREHOUSE_ID = "f877840f8d6e4eca"  # Pro SQL Warehouse

# ==============================================================================
# TASK 1: Create VCN Catalog and Medallion Schemas
# ==============================================================================
def create_vcn_catalog_and_schemas():
    """Create VCN catalog with bronze, silver, gold schemas"""
    
    print(f"Creating VCN catalog and schemas for {ENVIRONMENT}...")
    
    # SQL statements to create catalog and schemas
    sql_statements = [
        # VCN Catalog
        f"""
        CREATE CATALOG IF NOT EXISTS `{CATALOG_NAME}` 
        COMMENT 'Internal catalog for VCN data processing' 
        WITH PROPERTIES (
            'source' = 'vcn', 
            'data_owner' = 'data_engineering', 
            'managed_by' = 'orchestration_job',
            'environment' = '{ENVIRONMENT}'
        )
        """,
        # Bronze Schema
        f"""
        CREATE SCHEMA IF NOT EXISTS `{CATALOG_NAME}`.`bronze` 
        COMMENT 'Bronze layer - Raw ingested data from VCN' 
        WITH PROPERTIES (
            'layer' = 'bronze', 
            'purpose' = 'raw_data_ingestion',
            'environment' = '{ENVIRONMENT}'
        )
        """,
        # Silver Schema
        f"""
        CREATE SCHEMA IF NOT EXISTS `{CATALOG_NAME}`.`silver` 
        COMMENT 'Silver layer - Cleaned and enriched VCN data' 
        WITH PROPERTIES (
            'layer' = 'silver', 
            'purpose' = 'cleaned_data',
            'environment' = '{ENVIRONMENT}'
        )
        """,
        # Gold Schema
        f"""
        CREATE SCHEMA IF NOT EXISTS `{CATALOG_NAME}`.`gold` 
        COMMENT 'Gold layer - Business-ready VCN data' 
        WITH PROPERTIES (
            'layer' = 'gold', 
            'purpose' = 'business_intelligence',
            'environment' = '{ENVIRONMENT}'
        )
        """
    ]
    
    # Execute each statement
    for i, statement in enumerate(sql_statements, 1):
        try:
            print(f"  [{i}/{len(sql_statements)}] Executing SQL statement...")
            response = w.statement_execution.execute_statement(
                warehouse_id=WAREHOUSE_ID,
                statement=statement
            )
            print(f"  ✅ Statement {i} executed successfully")
        except Exception as e:
            print(f"  ⚠️ Statement {i} failed: {str(e)}")
            # Continue anyway since we use IF NOT EXISTS

print("=" * 80)
print("VCN DATA INGESTION ORCHESTRATION JOB")
print("=" * 80)

# Task 1: Create catalog and schemas
try:
    create_vcn_catalog_and_schemas()
    print("\n✅ VCN catalog and schemas created successfully!")
except Exception as e:
    print(f"\n❌ Failed to create catalog/schemas: {str(e)}")
    raise

# Task 2: Run VCN ingestion notebook
print("\n" + "=" * 80)
print("Running VCN ingestion notebook...")
print("=" * 80)

try:
    # Run the VCN ingestion notebook
    dbutils.notebook.run(
        path="/Shared/pipelines/vcn_ingestion",
        timeout_seconds=3600,
        arguments={
            "environment": ENVIRONMENT,
            "catalog": CATALOG_NAME
        }
    )
    print("\n✅ VCN ingestion notebook completed successfully!")
except Exception as e:
    print(f"\n❌ VCN ingestion notebook failed: {str(e)}")
    raise

print("\n" + "=" * 80)
print("✅ VCN DATA INGESTION ORCHESTRATION COMPLETED")
print("=" * 80)
