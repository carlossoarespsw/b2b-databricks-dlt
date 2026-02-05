# Databricks notebook source

# ==============================================================================
# Setup: Create VCN Catalog and Medallion Schemas
# ==============================================================================
# This notebook creates the vcn-catalog and its schemas (bronze, silver, gold)
# Should run BEFORE ingestion_vcn.py
# 
# Parameters passed from pipeline configuration:
# - environment: dev/staging/prod
# - catalog: vcn-catalog (from pipeline config)

import logging
from datetime import datetime

# ============================================================================
# Configuration
# ============================================================================
ENVIRONMENT = spark.conf.get("environment", "dev")
CATALOG_NAME = spark.conf.get("catalog", "vcn-catalog")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("=" * 80)
print("VCN CATALOG SETUP")
print("=" * 80)
print(f"Environment: {ENVIRONMENT}")
print(f"Catalog: {CATALOG_NAME}")
print(f"Timestamp: {datetime.now()}")
print("=" * 80)

# ============================================================================
# TASK 1: Create Catalog
# ============================================================================
print("\n[TASK 1] Creating catalog...")

try:
    create_catalog_sql = f"""
    CREATE CATALOG IF NOT EXISTS `{CATALOG_NAME}`
    COMMENT 'Internal catalog for VCN data processing'
    WITH PROPERTIES (
        'source' = 'vcn',
        'data_owner' = 'data_engineering',
        'managed_by' = 'setup_pipeline',
        'environment' = '{ENVIRONMENT}',
        'created_at' = '{datetime.now()}'
    )
    """
    spark.sql(create_catalog_sql)
    print(f"✅ Catalog `{CATALOG_NAME}` created/verified")
except Exception as e:
    print(f"❌ Error creating catalog: {str(e)}")
    raise

# ============================================================================
# TASK 2: Create Bronze Schema
# ============================================================================
print("\n[TASK 2] Creating BRONZE schema...")

try:
    create_bronze_sql = f"""
    CREATE SCHEMA IF NOT EXISTS `{CATALOG_NAME}`.`bronze`
    COMMENT 'Bronze layer - Raw ingested data from VCN'
    WITH PROPERTIES (
        'layer' = 'bronze',
        'purpose' = 'raw_data_ingestion',
        'environment' = '{ENVIRONMENT}',
        'created_at' = '{datetime.now()}'
    )
    """
    spark.sql(create_bronze_sql)
    print(f"✅ Schema `{CATALOG_NAME}`.`bronze` created/verified")
except Exception as e:
    print(f"❌ Error creating bronze schema: {str(e)}")
    raise

# ============================================================================
# TASK 3: Create Silver Schema
# ============================================================================
print("\n[TASK 3] Creating SILVER schema...")

try:
    create_silver_sql = f"""
    CREATE SCHEMA IF NOT EXISTS `{CATALOG_NAME}`.`silver`
    COMMENT 'Silver layer - Cleaned and enriched VCN data'
    WITH PROPERTIES (
        'layer' = 'silver',
        'purpose' = 'cleaned_data',
        'environment' = '{ENVIRONMENT}',
        'created_at' = '{datetime.now()}'
    )
    """
    spark.sql(create_silver_sql)
    print(f"✅ Schema `{CATALOG_NAME}`.`silver` created/verified")
except Exception as e:
    print(f"❌ Error creating silver schema: {str(e)}")
    raise

# ============================================================================
# TASK 4: Create Gold Schema
# ============================================================================
print("\n[TASK 4] Creating GOLD schema...")

try:
    create_gold_sql = f"""
    CREATE SCHEMA IF NOT EXISTS `{CATALOG_NAME}`.`gold`
    COMMENT 'Gold layer - Business-ready VCN data for analytics'
    WITH PROPERTIES (
        'layer' = 'gold',
        'purpose' = 'business_intelligence',
        'environment' = '{ENVIRONMENT}',
        'created_at' = '{datetime.now()}'
    )
    """
    spark.sql(create_gold_sql)
    print(f"✅ Schema `{CATALOG_NAME}`.`gold` created/verified")
except Exception as e:
    print(f"❌ Error creating gold schema: {str(e)}")
    raise

# ============================================================================
# TASK 5: Verify Setup
# ============================================================================
print("\n[TASK 5] Verifying setup...")

try:
    verify_sql = f"""
    SELECT 
        catalog_name,
        schema_name,
        CASE 
            WHEN schema_name = 'bronze' THEN 'Raw Data Layer'
            WHEN schema_name = 'silver' THEN 'Cleaned Data Layer'
            WHEN schema_name = 'gold' THEN 'Analytics Layer'
            ELSE 'Unknown'
        END as layer_description
    FROM system.information_schema.schemata
    WHERE catalog_name = '{CATALOG_NAME}'
    ORDER BY schema_name
    """
    
    result_df = spark.sql(verify_sql)
    result_count = result_df.count()
    
    print(f"\n✅ Verification Complete - {result_count} schemas found:")
    result_df.show(truncate=False)
    
    if result_count >= 3:
        print("\n✅ All 3 medallion schemas created successfully!")
    else:
        print(f"\n⚠️  Warning: Expected 3 schemas but found {result_count}")
        
except Exception as e:
    print(f"❌ Error verifying setup: {str(e)}")
    raise

# ============================================================================
# Setup Complete
# ============================================================================
print("\n" + "=" * 80)
print("✅ VCN CATALOG SETUP COMPLETED")
print("=" * 80)
print(f"Ready for ingestion_vcn to proceed")
print("=" * 80)
