-- ═══════════════════════════════════════════════════════════════════════
-- DataFoundation: BigQuery Data Warehouse Schema
-- Multi-source Retail Data Integration Hub
-- ═══════════════════════════════════════════════════════════════════════

-- ─── Create Dataset ──────────────────────────────────────────────────
-- CREATE SCHEMA IF NOT EXISTS `multi-source-retail-data.retail_dw`;

-- ═══════════════════════════════════════════════════════════════════════
-- STAGING LAYER
-- ═══════════════════════════════════════════════════════════════════════

-- Staging: Retail Sales (from Kaggle CSV)
CREATE OR REPLACE TABLE `multi-source-retail-data.retail_dw.stg_retail_sales` (
    transaction_id    INT64,
    date              TIMESTAMP,
    customer_id       STRING,
    gender            STRING,
    age               INT64,
    product_category  STRING,
    quantity          INT64,
    price_per_unit    FLOAT64,
    total_amount      FLOAT64,
    row_hash          STRING,
    _extracted_at     TIMESTAMP,
    _source           STRING
);

-- Staging: API Products (from Fake Store API)
CREATE OR REPLACE TABLE `multi-source-retail-data.retail_dw.stg_api_products` (
    api_product_id    INT64,
    product_name      STRING,
    api_price         FLOAT64,
    description       STRING,
    product_category  STRING,
    product_image_url STRING,
    rating_rate       FLOAT64,
    rating_count      INT64,
    _extracted_at     TIMESTAMP,
    _source           STRING
);


-- ═══════════════════════════════════════════════════════════════════════
-- DIMENSION TABLES
-- ═══════════════════════════════════════════════════════════════════════

-- Dimension: Date
CREATE OR REPLACE TABLE `multi-source-retail-data.retail_dw.dim_date` (
    date_key          INT64 NOT NULL,
    full_date         DATE,
    year              INT64,
    quarter           INT64,
    month             INT64,
    month_name        STRING,
    week_of_year      INT64,
    day_of_month      INT64,
    day_of_week       INT64,
    day_name          STRING,
    is_weekend        BOOL,
    fiscal_year       INT64,
    fiscal_quarter    INT64
);

-- Dimension: Customer (SCD Type 2)
CREATE OR REPLACE TABLE `multi-source-retail-data.retail_dw.dim_customer` (
    customer_key          INT64 NOT NULL,
    customer_id           STRING,
    gender                STRING,
    age                   INT64,
    age_group             STRING,
    customer_segment      STRING,
    first_purchase_date   TIMESTAMP,
    last_purchase_date    TIMESTAMP,
    total_transactions    INT64,
    effective_start_date  TIMESTAMP,
    effective_end_date    TIMESTAMP,
    is_current            BOOL,
    version               INT64,
    row_hash              STRING,
    _loaded_at            TIMESTAMP
);

-- Dimension: Product (SCD Type 2)
CREATE OR REPLACE TABLE `multi-source-retail-data.retail_dw.dim_product` (
    product_key           INT64 NOT NULL,
    api_product_id        INT64,
    product_name          STRING,
    api_price             FLOAT64,
    description           STRING,
    product_category      STRING,
    product_image_url     STRING,
    rating_rate           FLOAT64,
    rating_count          INT64,
    effective_start_date  TIMESTAMP,
    effective_end_date    TIMESTAMP,
    is_current            BOOL,
    version               INT64,
    row_hash              STRING,
    _loaded_at            TIMESTAMP
);

-- Dimension: Product Category
CREATE OR REPLACE TABLE `multi-source-retail-data.retail_dw.dim_product_category` (
    category_key      INT64 NOT NULL,
    category_name     STRING,
    category_source   STRING,
    category_group    STRING,
    _loaded_at        TIMESTAMP
);


-- ═══════════════════════════════════════════════════════════════════════
-- FACT TABLES
-- ═══════════════════════════════════════════════════════════════════════

-- Fact: Sales
CREATE OR REPLACE TABLE `multi-source-retail-data.retail_dw.fact_sales` (
    sales_key         INT64 NOT NULL,
    transaction_id    INT64,
    date_key          INT64,
    customer_key      INT64,
    category_key      INT64,
    quantity          INT64,
    price_per_unit    FLOAT64,
    total_amount      FLOAT64,
    customer_id       STRING,
    product_category  STRING,
    gender            STRING,
    age               INT64,
    _extracted_at     TIMESTAMP,
    _source           STRING,
    _loaded_at        TIMESTAMP
);


-- ═══════════════════════════════════════════════════════════════════════
-- DATA MARTS
-- ═══════════════════════════════════════════════════════════════════════

-- Mart: Sales Performance (monthly aggregation)
CREATE OR REPLACE TABLE `multi-source-retail-data.retail_dw.mart_sales_performance` (
    year                  INT64,
    month                 INT64,
    month_name            STRING,
    total_revenue         FLOAT64,
    total_transactions    INT64,
    total_quantity        INT64,
    avg_order_value       FLOAT64,
    unique_customers      INT64,
    revenue_prev_month    FLOAT64,
    revenue_growth_pct    FLOAT64,
    _mart_generated_at    TIMESTAMP
);

-- Mart: Category Analysis
CREATE OR REPLACE TABLE `multi-source-retail-data.retail_dw.mart_category_analysis` (
    product_category      STRING,
    total_revenue         FLOAT64,
    total_transactions    INT64,
    total_quantity        INT64,
    avg_price             FLOAT64,
    avg_order_value       FLOAT64,
    unique_customers      INT64,
    avg_customer_age      FLOAT64,
    revenue_share_pct     FLOAT64,
    female_revenue_pct    FLOAT64,
    male_revenue_pct      FLOAT64,
    category_name         STRING,
    category_group        STRING,
    _mart_generated_at    TIMESTAMP
);
