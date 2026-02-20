"""
Centralized configuration for the Data Integration Hub.
Loads settings from environment variables and .env file.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ─── Google Cloud / BigQuery ───────────────────────────────────────────
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "multi-source-retail-data")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    "multi-source-retail-data-684b168166c6.json"
)
BQ_DATASET = os.getenv("BQ_DATASET", "retail_dw")

# Set credentials path for Google SDK
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

# ─── MySQL (Staging) ──────────────────────────────────────────────────
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", 3306)),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DATABASE", "retail_staging"),
}

# ─── API Configuration ────────────────────────────────────────────────
FAKE_STORE_API_URL = os.getenv("FAKE_STORE_API_URL", "https://fakestoreapi.com")
FAKE_STORE_PRODUCTS_ENDPOINT = f"{FAKE_STORE_API_URL}/products"
FAKE_STORE_CATEGORIES_ENDPOINT = f"{FAKE_STORE_API_URL}/products/categories"

# ─── Data File Paths ──────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RETAIL_SALES_CSV = os.path.join(
    BASE_DIR, os.getenv("RETAIL_SALES_CSV", "retail_sales_dataset.csv")
)

# ─── BigQuery Table Names ─────────────────────────────────────────────
# Staging tables
STG_RETAIL_SALES = f"{GCP_PROJECT_ID}.{BQ_DATASET}.stg_retail_sales"
STG_API_PRODUCTS = f"{GCP_PROJECT_ID}.{BQ_DATASET}.stg_api_products"

# Dimension tables (SCD Type 2)
DIM_CUSTOMER = f"{GCP_PROJECT_ID}.{BQ_DATASET}.dim_customer"
DIM_PRODUCT = f"{GCP_PROJECT_ID}.{BQ_DATASET}.dim_product"
DIM_DATE = f"{GCP_PROJECT_ID}.{BQ_DATASET}.dim_date"
DIM_PRODUCT_CATEGORY = f"{GCP_PROJECT_ID}.{BQ_DATASET}.dim_product_category"

# Fact tables
FACT_SALES = f"{GCP_PROJECT_ID}.{BQ_DATASET}.fact_sales"

# Data Mart tables
MART_SALES_PERFORMANCE = f"{GCP_PROJECT_ID}.{BQ_DATASET}.mart_sales_performance"
MART_CATEGORY_ANALYSIS = f"{GCP_PROJECT_ID}.{BQ_DATASET}.mart_category_analysis"

# ─── ETL Configuration ────────────────────────────────────────────────
ETL_BATCH_SIZE = 500
ETL_LOG_LEVEL = os.getenv("ETL_LOG_LEVEL", "INFO")
