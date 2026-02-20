"""
TRANSFORM Module
---------------------------------------------------------
Cleans, validates and transforms raw data into the 
dimensional model schema (star schema) for the data warehouse.

Implements:
  - Data cleaning & validation
  - Date dimension generation
  - Customer dimension (SCD Type 2 preparation)
  - Product dimension (SCD Type 2 preparation)  
  - Product category dimension
  - Fact sales table
  - Data mart transformations
"""

import pandas as pd
import numpy as np
import hashlib
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# =====================================================================
# DATA CLEANING & VALIDATION
# =====================================================================

def clean_retail_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate the retail sales DataFrame."""
    logger.info("[CLEAN] Cleaning retail sales data...")
    
    df_clean = df.copy()
    
    # Standardize column names
    df_clean.columns = [
        col.strip().lower().replace(' ', '_') for col in df_clean.columns
    ]
    
    # Parse dates
    df_clean['date'] = pd.to_datetime(df_clean['date'], errors='coerce')
    
    # Remove rows with null dates
    null_dates = df_clean['date'].isna().sum()
    if null_dates > 0:
        logger.warning(f"[WARN] Removing {null_dates} rows with invalid dates")
        df_clean = df_clean.dropna(subset=['date'])
    
    # Validate numeric columns
    numeric_cols = ['quantity', 'price_per_unit', 'total_amount']
    for col in numeric_cols:
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    # Remove rows with negative or zero quantities
    df_clean = df_clean[df_clean['quantity'] > 0]
    
    # Recalculate total_amount for consistency
    df_clean['total_amount_calculated'] = (
        df_clean['quantity'] * df_clean['price_per_unit']
    )
    
    # Check for discrepancies
    discrepancies = (
        df_clean['total_amount'] != df_clean['total_amount_calculated']
    ).sum()
    if discrepancies > 0:
        logger.warning(
            f"[WARN] Found {discrepancies} rows with amount discrepancies. "
            "Using calculated values."
        )
        df_clean['total_amount'] = df_clean['total_amount_calculated']
    
    df_clean.drop(columns=['total_amount_calculated'], inplace=True)
    
    # Standardize gender values
    df_clean['gender'] = df_clean['gender'].str.strip().str.title()
    
    # Standardize product categories
    df_clean['product_category'] = (
        df_clean['product_category'].str.strip().str.title()
    )
    
    # Validate age range (18-100)
    df_clean['age'] = df_clean['age'].clip(lower=18, upper=100)
    
    # Generate surrogate keys
    df_clean['row_hash'] = df_clean.apply(
        lambda row: hashlib.md5(
            f"{row['transaction_id']}_{row['date']}_{row['customer_id']}".encode()
        ).hexdigest(),
        axis=1
    )
    
    logger.info(f"[OK] Cleaned retail sales: {len(df_clean)} records remain")
    return df_clean


def clean_api_products(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate the API products DataFrame."""
    logger.info("[CLEAN] Cleaning API products data...")
    
    df_clean = df.copy()
    
    # Standardize column names
    df_clean.columns = [
        col.strip().lower().replace(' ', '_') for col in df_clean.columns
    ]
    
    # Ensure price is numeric
    df_clean['price'] = pd.to_numeric(df_clean['price'], errors='coerce')
    
    # Clean category names
    df_clean['category'] = df_clean['category'].str.strip().str.title()
    
    # Truncate long descriptions
    df_clean['description'] = df_clean['description'].str[:500]
    
    # Clean title
    df_clean['title'] = df_clean['title'].str.strip()
    
    # Validate rating range
    df_clean['rating_rate'] = df_clean['rating_rate'].clip(lower=0, upper=5)
    df_clean['rating_count'] = df_clean['rating_count'].clip(lower=0)
    
    logger.info(f"[OK] Cleaned API products: {len(df_clean)} records")
    return df_clean


# =====================================================================
# DIMENSION TABLE TRANSFORMATIONS
# =====================================================================

def build_dim_date(df_sales: pd.DataFrame) -> pd.DataFrame:
    """
    Build the Date dimension table covering the full date range 
    of the sales data.
    """
    logger.info("[DIM] Building Date dimension...")
    
    min_date = df_sales['date'].min()
    max_date = df_sales['date'].max()
    
    # Extend range to full years
    start_date = pd.Timestamp(year=min_date.year, month=1, day=1)
    end_date = pd.Timestamp(year=max_date.year, month=12, day=31)
    
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    dim_date = pd.DataFrame({'full_date': date_range})
    dim_date['date_key'] = dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
    dim_date['year'] = dim_date['full_date'].dt.year
    dim_date['quarter'] = dim_date['full_date'].dt.quarter
    dim_date['month'] = dim_date['full_date'].dt.month
    dim_date['month_name'] = dim_date['full_date'].dt.month_name()
    dim_date['week_of_year'] = dim_date['full_date'].dt.isocalendar().week.astype(int)
    dim_date['day_of_month'] = dim_date['full_date'].dt.day
    dim_date['day_of_week'] = dim_date['full_date'].dt.dayofweek
    dim_date['day_name'] = dim_date['full_date'].dt.day_name()
    dim_date['is_weekend'] = dim_date['day_of_week'].isin([5, 6])
    dim_date['fiscal_year'] = dim_date['full_date'].apply(
        lambda d: d.year + 1 if d.month >= 10 else d.year
    )
    dim_date['fiscal_quarter'] = dim_date['full_date'].apply(
        lambda d: (d.month - 10) % 12 // 3 + 1
    )
    
    logger.info(
        f"[OK] Date dimension: {len(dim_date)} days "
        f"({start_date.date()} to {end_date.date()})"
    )
    return dim_date


def build_dim_customer(df_sales: pd.DataFrame) -> pd.DataFrame:
    """
    Build the Customer dimension with SCD Type 2 support.
    Tracks changes in customer attributes (age, gender) over time.
    """
    logger.info("[DIM] Building Customer dimension (SCD Type 2)...")
    
    # Get unique customer profiles
    customers = df_sales.groupby('customer_id').agg(
        gender=('gender', 'first'),
        age=('age', 'first'),
        first_purchase_date=('date', 'min'),
        last_purchase_date=('date', 'max'),
        total_transactions=('transaction_id', 'nunique'),
    ).reset_index()
    
    # Add SCD Type 2 columns
    customers['customer_key'] = range(1, len(customers) + 1)
    customers['effective_start_date'] = customers['first_purchase_date']
    customers['effective_end_date'] = pd.Timestamp('9999-12-31')
    customers['is_current'] = True
    customers['version'] = 1
    
    # Generate row hash for change detection
    customers['row_hash'] = customers.apply(
        lambda row: hashlib.md5(
            f"{row['customer_id']}_{row['gender']}_{row['age']}".encode()
        ).hexdigest(),
        axis=1
    )
    
    # Add age group classification
    customers['age_group'] = pd.cut(
        customers['age'],
        bins=[0, 25, 35, 45, 55, 65, 100],
        labels=['18-25', '26-35', '36-45', '46-55', '56-65', '65+']
    )
    
    # Customer segment based on transaction count
    customers['customer_segment'] = pd.cut(
        customers['total_transactions'],
        bins=[0, 1, 3, 5, float('inf')],
        labels=['New', 'Occasional', 'Regular', 'Loyal']
    )
    
    customers['_loaded_at'] = datetime.utcnow()
    
    logger.info(f"[OK] Customer dimension: {len(customers)} unique customers")
    return customers


def build_dim_product(
    df_api_products: pd.DataFrame,
    df_sales: pd.DataFrame
) -> pd.DataFrame:
    """
    Build the Product dimension by combining API catalog data 
    with sales data. Implements SCD Type 2 for product changes.
    """
    logger.info("[DIM] Building Product dimension (SCD Type 2)...")
    
    # Start with API products as master product list
    products = df_api_products[[
        'id', 'title', 'price', 'description', 'category',
        'image', 'rating_rate', 'rating_count'
    ]].copy()
    
    products.rename(columns={
        'id': 'api_product_id',
        'title': 'product_name',
        'price': 'api_price',
        'category': 'product_category',
        'image': 'product_image_url',
    }, inplace=True)
    
    # Also add retail-only categories not in API
    retail_categories = set(
        df_sales['product_category'].unique()
    )
    api_categories = set(products['product_category'].unique())
    
    # Map retail categories to API products where possible
    category_mapping = {
        'Electronics': 'electronics',
        'Clothing': "men's clothing",  # default mapping
        'Beauty': 'jewelery',
    }
    
    # Add SCD Type 2 columns
    products['product_key'] = range(1, len(products) + 1)
    products['effective_start_date'] = datetime.utcnow()
    products['effective_end_date'] = pd.Timestamp('9999-12-31')
    products['is_current'] = True
    products['version'] = 1
    
    products['row_hash'] = products.apply(
        lambda row: hashlib.md5(
            f"{row['api_product_id']}_{row['product_name']}_{row['api_price']}".encode()
        ).hexdigest(),
        axis=1
    )
    
    products['_loaded_at'] = datetime.utcnow()
    
    logger.info(f"[OK] Product dimension: {len(products)} products")
    return products


def build_dim_product_category(
    df_sales: pd.DataFrame,
    df_api_products: pd.DataFrame,
    api_categories: list
) -> pd.DataFrame:
    """
    Build the Product Category dimension combining 
    categories from both sources.
    """
    logger.info("[DIM] Building Product Category dimension...")
    
    # Categories from retail sales
    retail_cats = df_sales['product_category'].unique().tolist()
    
    # Categories from API
    api_cats = [cat.strip().title() for cat in api_categories]
    
    # Combine all unique categories
    all_categories = sorted(set(retail_cats + api_cats))
    
    dim_category = pd.DataFrame({
        'category_key': range(1, len(all_categories) + 1),
        'category_name': all_categories,
    })
    
    # Add category metadata
    dim_category['category_source'] = dim_category['category_name'].apply(
        lambda c: 'both' if c in retail_cats and c.lower() in [
            x.lower() for x in api_cats
        ] else ('retail' if c in retail_cats else 'api')
    )
    
    # Category group classification
    electronics_keywords = ['electronics', 'tech', 'computer']
    clothing_keywords = ['clothing', 'fashion', 'apparel', "men's", "women's"]
    beauty_keywords = ['beauty', 'jewelery', 'jewelry', 'cosmetics']
    
    def classify_category(name):
        name_lower = name.lower()
        if any(kw in name_lower for kw in electronics_keywords):
            return 'Electronics'
        elif any(kw in name_lower for kw in clothing_keywords):
            return 'Fashion & Apparel'
        elif any(kw in name_lower for kw in beauty_keywords):
            return 'Beauty & Accessories'
        else:
            return 'Other'
    
    dim_category['category_group'] = dim_category['category_name'].apply(
        classify_category
    )
    dim_category['_loaded_at'] = datetime.utcnow()
    
    logger.info(
        f"[OK] Product Category dimension: {len(dim_category)} categories"
    )
    return dim_category


# =====================================================================
# FACT TABLE TRANSFORMATIONS
# =====================================================================

def build_fact_sales(
    df_sales: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_category: pd.DataFrame,
    dim_date: pd.DataFrame
) -> pd.DataFrame:
    """
    Build the Fact Sales table by joining cleaned sales data 
    with dimension surrogate keys.
    """
    logger.info("[FACT] Building Fact Sales table...")
    
    fact = df_sales.copy()
    
    # Add date key
    fact['date_key'] = fact['date'].dt.strftime('%Y%m%d').astype(int)
    
    # Join customer dimension key
    customer_key_map = dim_customer.set_index('customer_id')['customer_key'].to_dict()
    fact['customer_key'] = fact['customer_id'].map(customer_key_map)
    
    # Join category dimension key
    category_key_map = dim_category.set_index('category_name')['category_key'].to_dict()
    fact['category_key'] = fact['product_category'].map(category_key_map)
    
    # Select fact table columns
    fact_sales = fact[[
        'transaction_id', 'date_key', 'customer_key', 'category_key',
        'quantity', 'price_per_unit', 'total_amount',
        'customer_id', 'product_category', 'gender', 'age',
        '_extracted_at', '_source'
    ]].copy()
    
    fact_sales['sales_key'] = range(1, len(fact_sales) + 1)
    fact_sales['_loaded_at'] = datetime.utcnow()
    
    logger.info(f"[OK] Fact Sales: {len(fact_sales)} records")
    logger.info(
        f"   Total revenue: ${fact_sales['total_amount'].sum():,.2f}"
    )
    return fact_sales


# =====================================================================
# DATA MART TRANSFORMATIONS
# =====================================================================

def build_mart_sales_performance(
    fact_sales: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_customer: pd.DataFrame
) -> pd.DataFrame:
    """
    Build the Sales Performance data mart.
    Aggregates sales metrics by various time periods.
    """
    logger.info("[MART] Building Sales Performance data mart...")
    
    # Merge with date dimension
    mart = fact_sales.merge(
        dim_date[['date_key', 'year', 'quarter', 'month', 'month_name', 'day_name', 'is_weekend']],
        on='date_key',
        how='left'
    )
    
    # Monthly performance
    monthly = mart.groupby(
        ['year', 'month', 'month_name']
    ).agg(
        total_revenue=('total_amount', 'sum'),
        total_transactions=('transaction_id', 'nunique'),
        total_quantity=('quantity', 'sum'),
        avg_order_value=('total_amount', 'mean'),
        unique_customers=('customer_id', 'nunique'),
    ).reset_index()
    
    # Calculate month-over-month growth
    monthly = monthly.sort_values(['year', 'month'])
    monthly['revenue_prev_month'] = monthly['total_revenue'].shift(1)
    monthly['revenue_growth_pct'] = (
        (monthly['total_revenue'] - monthly['revenue_prev_month'])
        / monthly['revenue_prev_month'] * 100
    ).round(2)
    
    monthly['_mart_generated_at'] = datetime.utcnow()
    
    logger.info(f"[OK] Sales Performance mart: {len(monthly)} monthly records")
    return monthly


def build_mart_category_analysis(
    fact_sales: pd.DataFrame,
    dim_category: pd.DataFrame,
    dim_customer: pd.DataFrame
) -> pd.DataFrame:
    """
    Build the Product Category Analysis data mart.
    Provides category-level analytics including demographics.
    """
    logger.info("[MART] Building Category Analysis data mart...")
    
    # Category performance
    category_perf = fact_sales.groupby('product_category').agg(
        total_revenue=('total_amount', 'sum'),
        total_transactions=('transaction_id', 'nunique'),
        total_quantity=('quantity', 'sum'),
        avg_price=('price_per_unit', 'mean'),
        avg_order_value=('total_amount', 'mean'),
        unique_customers=('customer_id', 'nunique'),
        avg_customer_age=('age', 'mean'),
    ).reset_index()
    
    # Revenue share
    total_revenue = category_perf['total_revenue'].sum()
    category_perf['revenue_share_pct'] = (
        category_perf['total_revenue'] / total_revenue * 100
    ).round(2)
    
    # Gender split per category
    gender_split = fact_sales.groupby(
        ['product_category', 'gender']
    ).agg(
        gender_revenue=('total_amount', 'sum')
    ).reset_index()
    
    gender_pivot = gender_split.pivot_table(
        index='product_category',
        columns='gender',
        values='gender_revenue',
        fill_value=0
    ).reset_index()
    
    if 'Female' in gender_pivot.columns and 'Male' in gender_pivot.columns:
        gender_pivot['female_revenue_pct'] = (
            gender_pivot['Female']
            / (gender_pivot['Female'] + gender_pivot['Male']) * 100
        ).round(2)
        gender_pivot['male_revenue_pct'] = (
            100 - gender_pivot['female_revenue_pct']
        )
    
    # Merge category performance with gender data
    mart = category_perf.merge(
        gender_pivot[['product_category', 'female_revenue_pct', 'male_revenue_pct']]
        if 'female_revenue_pct' in gender_pivot.columns
        else gender_pivot[['product_category']],
        on='product_category',
        how='left'
    )
    
    # Join category group from dimension
    mart = mart.merge(
        dim_category[['category_name', 'category_group']],
        left_on='product_category',
        right_on='category_name',
        how='left'
    )
    
    mart['_mart_generated_at'] = datetime.utcnow()
    
    logger.info(f"[OK] Category Analysis mart: {len(mart)} categories")
    return mart


# =====================================================================
# ORCHESTRATOR
# =====================================================================

def transform_all(extracted_data: dict) -> dict:
    """
    Run all transformations on extracted data.
    
    Args:
        extracted_data: dict from extract.extract_all()
        
    Returns:
        dict with all dimension, fact, and mart DataFrames
    """
    logger.info("=" * 60)
    logger.info("[TRANSFORM] STARTING DATA TRANSFORMATIONS")
    logger.info("=" * 60)
    
    results = {}
    
    # 1. Clean raw data
    clean_sales = clean_retail_sales(extracted_data['retail_sales'])
    clean_products = clean_api_products(extracted_data['api_products'])
    
    results['stg_retail_sales'] = clean_sales
    results['stg_api_products'] = clean_products
    
    # 2. Build dimension tables
    results['dim_date'] = build_dim_date(clean_sales)
    results['dim_customer'] = build_dim_customer(clean_sales)
    results['dim_product'] = build_dim_product(clean_products, clean_sales)
    results['dim_product_category'] = build_dim_product_category(
        clean_sales, clean_products, extracted_data['api_categories']
    )
    
    # 3. Build fact table
    results['fact_sales'] = build_fact_sales(
        clean_sales,
        results['dim_customer'],
        results['dim_product_category'],
        results['dim_date']
    )
    
    # 4. Build data marts
    results['mart_sales_performance'] = build_mart_sales_performance(
        results['fact_sales'],
        results['dim_date'],
        results['dim_customer']
    )
    results['mart_category_analysis'] = build_mart_category_analysis(
        results['fact_sales'],
        results['dim_product_category'],
        results['dim_customer']
    )
    
    logger.info("=" * 60)
    logger.info("[OK] ALL TRANSFORMATIONS COMPLETE")
    for key, val in results.items():
        if isinstance(val, pd.DataFrame):
            logger.info(f"   {key}: {len(val)} records")
    logger.info("=" * 60)
    
    return results
