"""
LOAD Module
---------------------------------------------------------
Loads transformed data into Google BigQuery.

Supports:
  - Schema creation / update
  - Staging table loads (WRITE_TRUNCATE)
  - Dimension table loads with SCD Type 2 merge
  - Fact table incremental loads
  - Data mart loads (WRITE_TRUNCATE)
"""

import pandas as pd
import logging
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from config.settings import (
    GCP_PROJECT_ID,
    BQ_DATASET,
    STG_RETAIL_SALES,
    STG_API_PRODUCTS,
    DIM_CUSTOMER,
    DIM_PRODUCT,
    DIM_DATE,
    DIM_PRODUCT_CATEGORY,
    FACT_SALES,
    MART_SALES_PERFORMANCE,
    MART_CATEGORY_ANALYSIS,
)

logger = logging.getLogger(__name__)


def get_bq_client() -> bigquery.Client:
    """Create and return a BigQuery client."""
    return bigquery.Client(project=GCP_PROJECT_ID)


def ensure_dataset_exists(client: bigquery.Client):
    """Create the BigQuery dataset if it doesn't exist."""
    dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, BQ_DATASET)
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"[DS] Dataset {BQ_DATASET} already exists")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        logger.info(f"[DS] Created dataset {BQ_DATASET}")


# =====================================================================
# SCHEMA DEFINITIONS
# =====================================================================

SCHEMAS = {
    'stg_retail_sales': [
        bigquery.SchemaField("transaction_id", "INTEGER"),
        bigquery.SchemaField("date", "TIMESTAMP"),
        bigquery.SchemaField("customer_id", "STRING"),
        bigquery.SchemaField("gender", "STRING"),
        bigquery.SchemaField("age", "INTEGER"),
        bigquery.SchemaField("product_category", "STRING"),
        bigquery.SchemaField("quantity", "INTEGER"),
        bigquery.SchemaField("price_per_unit", "FLOAT"),
        bigquery.SchemaField("total_amount", "FLOAT"),
        bigquery.SchemaField("row_hash", "STRING"),
        bigquery.SchemaField("_extracted_at", "TIMESTAMP"),
        bigquery.SchemaField("_source", "STRING"),
    ],
    'stg_api_products': [
        bigquery.SchemaField("api_product_id", "INTEGER"),
        bigquery.SchemaField("product_name", "STRING"),
        bigquery.SchemaField("api_price", "FLOAT"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("product_category", "STRING"),
        bigquery.SchemaField("product_image_url", "STRING"),
        bigquery.SchemaField("rating_rate", "FLOAT"),
        bigquery.SchemaField("rating_count", "INTEGER"),
        bigquery.SchemaField("_extracted_at", "TIMESTAMP"),
        bigquery.SchemaField("_source", "STRING"),
    ],
    'dim_date': [
        bigquery.SchemaField("date_key", "INTEGER"),
        bigquery.SchemaField("full_date", "DATE"),
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("quarter", "INTEGER"),
        bigquery.SchemaField("month", "INTEGER"),
        bigquery.SchemaField("month_name", "STRING"),
        bigquery.SchemaField("week_of_year", "INTEGER"),
        bigquery.SchemaField("day_of_month", "INTEGER"),
        bigquery.SchemaField("day_of_week", "INTEGER"),
        bigquery.SchemaField("day_name", "STRING"),
        bigquery.SchemaField("is_weekend", "BOOLEAN"),
        bigquery.SchemaField("fiscal_year", "INTEGER"),
        bigquery.SchemaField("fiscal_quarter", "INTEGER"),
    ],
    'dim_customer': [
        bigquery.SchemaField("customer_key", "INTEGER"),
        bigquery.SchemaField("customer_id", "STRING"),
        bigquery.SchemaField("gender", "STRING"),
        bigquery.SchemaField("age", "INTEGER"),
        bigquery.SchemaField("age_group", "STRING"),
        bigquery.SchemaField("customer_segment", "STRING"),
        bigquery.SchemaField("first_purchase_date", "TIMESTAMP"),
        bigquery.SchemaField("last_purchase_date", "TIMESTAMP"),
        bigquery.SchemaField("total_transactions", "INTEGER"),
        bigquery.SchemaField("effective_start_date", "TIMESTAMP"),
        bigquery.SchemaField("effective_end_date", "TIMESTAMP"),
        bigquery.SchemaField("is_current", "BOOLEAN"),
        bigquery.SchemaField("version", "INTEGER"),
        bigquery.SchemaField("row_hash", "STRING"),
        bigquery.SchemaField("_loaded_at", "TIMESTAMP"),
    ],
    'dim_product': [
        bigquery.SchemaField("product_key", "INTEGER"),
        bigquery.SchemaField("api_product_id", "INTEGER"),
        bigquery.SchemaField("product_name", "STRING"),
        bigquery.SchemaField("api_price", "FLOAT"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("product_category", "STRING"),
        bigquery.SchemaField("product_image_url", "STRING"),
        bigquery.SchemaField("rating_rate", "FLOAT"),
        bigquery.SchemaField("rating_count", "INTEGER"),
        bigquery.SchemaField("effective_start_date", "TIMESTAMP"),
        bigquery.SchemaField("effective_end_date", "TIMESTAMP"),
        bigquery.SchemaField("is_current", "BOOLEAN"),
        bigquery.SchemaField("version", "INTEGER"),
        bigquery.SchemaField("row_hash", "STRING"),
        bigquery.SchemaField("_loaded_at", "TIMESTAMP"),
    ],
    'dim_product_category': [
        bigquery.SchemaField("category_key", "INTEGER"),
        bigquery.SchemaField("category_name", "STRING"),
        bigquery.SchemaField("category_source", "STRING"),
        bigquery.SchemaField("category_group", "STRING"),
        bigquery.SchemaField("_loaded_at", "TIMESTAMP"),
    ],
    'fact_sales': [
        bigquery.SchemaField("sales_key", "INTEGER"),
        bigquery.SchemaField("transaction_id", "INTEGER"),
        bigquery.SchemaField("date_key", "INTEGER"),
        bigquery.SchemaField("customer_key", "INTEGER"),
        bigquery.SchemaField("category_key", "INTEGER"),
        bigquery.SchemaField("quantity", "INTEGER"),
        bigquery.SchemaField("price_per_unit", "FLOAT"),
        bigquery.SchemaField("total_amount", "FLOAT"),
        bigquery.SchemaField("customer_id", "STRING"),
        bigquery.SchemaField("product_category", "STRING"),
        bigquery.SchemaField("gender", "STRING"),
        bigquery.SchemaField("age", "INTEGER"),
        bigquery.SchemaField("_extracted_at", "TIMESTAMP"),
        bigquery.SchemaField("_source", "STRING"),
        bigquery.SchemaField("_loaded_at", "TIMESTAMP"),
    ],
    'mart_sales_performance': [
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("month", "INTEGER"),
        bigquery.SchemaField("month_name", "STRING"),
        bigquery.SchemaField("total_revenue", "FLOAT"),
        bigquery.SchemaField("total_transactions", "INTEGER"),
        bigquery.SchemaField("total_quantity", "INTEGER"),
        bigquery.SchemaField("avg_order_value", "FLOAT"),
        bigquery.SchemaField("unique_customers", "INTEGER"),
        bigquery.SchemaField("revenue_prev_month", "FLOAT"),
        bigquery.SchemaField("revenue_growth_pct", "FLOAT"),
        bigquery.SchemaField("_mart_generated_at", "TIMESTAMP"),
    ],
    'mart_category_analysis': [
        bigquery.SchemaField("product_category", "STRING"),
        bigquery.SchemaField("total_revenue", "FLOAT"),
        bigquery.SchemaField("total_transactions", "INTEGER"),
        bigquery.SchemaField("total_quantity", "INTEGER"),
        bigquery.SchemaField("avg_price", "FLOAT"),
        bigquery.SchemaField("avg_order_value", "FLOAT"),
        bigquery.SchemaField("unique_customers", "INTEGER"),
        bigquery.SchemaField("avg_customer_age", "FLOAT"),
        bigquery.SchemaField("revenue_share_pct", "FLOAT"),
        bigquery.SchemaField("female_revenue_pct", "FLOAT"),
        bigquery.SchemaField("male_revenue_pct", "FLOAT"),
        bigquery.SchemaField("category_name", "STRING"),
        bigquery.SchemaField("category_group", "STRING"),
        bigquery.SchemaField("_mart_generated_at", "TIMESTAMP"),
    ],
}


# =====================================================================
# TABLE LOADING FUNCTIONS
# =====================================================================

def load_table(
    client: bigquery.Client,
    df: pd.DataFrame,
    table_id: str,
    table_name: str,
    write_disposition: str = "WRITE_TRUNCATE"
) -> int:
    """
    Load a DataFrame into a BigQuery table.
    
    Args:
        client: BigQuery client
        df: DataFrame to load
        table_id: Full table ID (project.dataset.table)
        table_name: Short name for schema lookup
        write_disposition: WRITE_TRUNCATE or WRITE_APPEND
        
    Returns:
        Number of rows loaded
    """
    logger.info(f"[LOAD] Loading {table_name} -> {table_id} ({write_disposition})")
    
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMAS.get(table_name, []),
        write_disposition=write_disposition,
    )
    
    # Convert datetime columns for BigQuery compatibility
    for col in df.columns:
        if df[col].dtype == 'datetime64[ns]':
            df[col] = df[col].dt.tz_localize(None)
    
    try:
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()  # Wait for completion
        
        table = client.get_table(table_id)
        logger.info(f"[OK] Loaded {table.num_rows} rows into {table_id}")
        return table.num_rows
        
    except Exception as e:
        logger.error(f"[ERROR] Failed to load {table_name}: {e}")
        raise


# =====================================================================
# SCD TYPE 2 MERGE FOR DIMENSIONS
# =====================================================================

def scd_type2_merge_customer(
    client: bigquery.Client,
    df_new: pd.DataFrame
):
    """
    Perform SCD Type 2 merge for the customer dimension.
    Compares incoming rows with existing rows by row_hash.
    New/changed records are inserted; old versions are expired.
    """
    logger.info("[SCD2] Performing SCD Type 2 merge for Customer dimension...")
    
    table_id = DIM_CUSTOMER
    
    # Check if table exists
    try:
        client.get_table(table_id)
        table_exists = True
    except NotFound:
        table_exists = False
    
    if not table_exists:
        # First load - just insert everything
        load_table(client, df_new, table_id, 'dim_customer', 'WRITE_TRUNCATE')
        logger.info("   First load - inserted all records as new")
        return
    
    # Load new data to a temp staging table
    temp_table = f"{table_id}_staging"
    load_table(client, df_new, temp_table, 'dim_customer', 'WRITE_TRUNCATE')
    
    # Execute SCD Type 2 merge query
    merge_query = f"""
    -- Step 1: Expire changed records in the target
    UPDATE `{table_id}` target
    SET 
        target.effective_end_date = CURRENT_TIMESTAMP(),
        target.is_current = FALSE
    WHERE target.is_current = TRUE
    AND target.customer_id IN (
        SELECT staging.customer_id 
        FROM `{temp_table}` staging
        WHERE staging.row_hash != target.row_hash
    );
    
    -- Step 2: Insert new / changed records
    INSERT INTO `{table_id}`
    SELECT * FROM `{temp_table}` staging
    WHERE NOT EXISTS (
        SELECT 1 FROM `{table_id}` target
        WHERE target.customer_id = staging.customer_id
        AND target.row_hash = staging.row_hash
        AND target.is_current = TRUE
    );
    """
    
    try:
        # BigQuery doesn't support multi-statement in one query easily,
        # so split them
        queries = [q.strip() for q in merge_query.split(';') if q.strip()]
        for q in queries:
            query_job = client.query(q)
            query_job.result()
        
        logger.info("[OK] SCD Type 2 merge complete for Customer dimension")
    except Exception as e:
        logger.error(f"[ERROR] SCD merge failed: {e}")
        # Fallback to full load
        load_table(client, df_new, table_id, 'dim_customer', 'WRITE_TRUNCATE')
    
    # Clean up staging table
    try:
        client.delete_table(temp_table)
    except Exception:
        pass


# =====================================================================
# ORCHESTRATOR
# =====================================================================

def load_all(transformed_data: dict) -> dict:
    """
    Load all transformed data into BigQuery.
    
    Args:
        transformed_data: dict from transform.transform_all()
        
    Returns:
        dict with load statistics
    """
    logger.info("=" * 60)
    logger.info("[LOAD] STARTING DATA LOADING TO BIGQUERY")
    logger.info("=" * 60)
    
    client = get_bq_client()
    ensure_dataset_exists(client)
    
    stats = {}
    
    # 1. Load staging tables (full refresh)
    stats['stg_retail_sales'] = load_table(
        client, transformed_data['stg_retail_sales'],
        STG_RETAIL_SALES, 'stg_retail_sales'
    )
    
    # Prepare API products for loading (rename columns)
    stg_api = transformed_data['stg_api_products'].copy()
    if 'id' in stg_api.columns:
        stg_api.rename(columns={
            'id': 'api_product_id',
            'title': 'product_name',
            'price': 'api_price',
            'category': 'product_category',
            'image': 'product_image_url',
        }, inplace=True)
    stats['stg_api_products'] = load_table(
        client, stg_api,
        STG_API_PRODUCTS, 'stg_api_products'
    )
    
    # 2. Load dimension tables
    stats['dim_date'] = load_table(
        client, transformed_data['dim_date'],
        DIM_DATE, 'dim_date'
    )
    
    # SCD Type 2 for Customer dimension
    scd_type2_merge_customer(client, transformed_data['dim_customer'])
    stats['dim_customer'] = len(transformed_data['dim_customer'])
    
    stats['dim_product'] = load_table(
        client, transformed_data['dim_product'],
        DIM_PRODUCT, 'dim_product'
    )
    
    stats['dim_product_category'] = load_table(
        client, transformed_data['dim_product_category'],
        DIM_PRODUCT_CATEGORY, 'dim_product_category'
    )
    
    # 3. Load fact table
    stats['fact_sales'] = load_table(
        client, transformed_data['fact_sales'],
        FACT_SALES, 'fact_sales'
    )
    
    # 4. Load data marts (full refresh)
    stats['mart_sales_performance'] = load_table(
        client, transformed_data['mart_sales_performance'],
        MART_SALES_PERFORMANCE, 'mart_sales_performance'
    )
    
    stats['mart_category_analysis'] = load_table(
        client, transformed_data['mart_category_analysis'],
        MART_CATEGORY_ANALYSIS, 'mart_category_analysis'
    )
    
    logger.info("=" * 60)
    logger.info("[OK] ALL DATA LOADED TO BIGQUERY")
    for table, count in stats.items():
        logger.info(f"   {table}: {count} rows")
    logger.info("=" * 60)
    
    return stats
