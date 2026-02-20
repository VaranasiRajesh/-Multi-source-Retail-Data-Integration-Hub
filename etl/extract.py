"""
EXTRACT Module
---------------------------------------------------------
Extracts data from two sources:
  1. Kaggle Retail Sales dataset (CSV file)
  2. Fake Store API (REST API - product catalog)
"""

import pandas as pd
import requests
import logging
from datetime import datetime

from config.settings import (
    RETAIL_SALES_CSV,
    FAKE_STORE_PRODUCTS_ENDPOINT,
    FAKE_STORE_CATEGORIES_ENDPOINT,
)

logger = logging.getLogger(__name__)


# =====================================================================
# Source 1: Kaggle Retail Sales CSV
# =====================================================================

def extract_retail_sales() -> pd.DataFrame:
    """
    Extract retail sales data from the local CSV dataset.
    
    Returns:
        pd.DataFrame with columns:
            Transaction ID, Date, Customer ID, Gender, Age,
            Product Category, Quantity, Price per Unit, Total Amount
    """
    logger.info(f"[CSV] Extracting retail sales data from: {RETAIL_SALES_CSV}")
    
    try:
        df = pd.read_csv(RETAIL_SALES_CSV)
        df['_extracted_at'] = datetime.utcnow()
        df['_source'] = 'kaggle_retail_sales'
        
        logger.info(f"[OK] Extracted {len(df)} retail sales records")
        logger.info(f"   Columns: {list(df.columns)}")
        logger.info(f"   Date range: {df['Date'].min()} to {df['Date'].max()}")
        logger.info(f"   Categories: {df['Product Category'].unique().tolist()}")
        
        return df
        
    except FileNotFoundError:
        logger.error(f"[ERROR] CSV file not found: {RETAIL_SALES_CSV}")
        raise
    except Exception as e:
        logger.error(f"[ERROR] Error extracting retail sales: {e}")
        raise


# =====================================================================
# Source 2: Fake Store API (Product Catalog)
# =====================================================================

def extract_api_products() -> pd.DataFrame:
    """
    Extract product catalog data from the Fake Store API.
    
    Returns:
        pd.DataFrame with columns:
            id, title, price, description, category, image,
            rating_rate, rating_count
    """
    logger.info(f"[API] Extracting product data from API: {FAKE_STORE_PRODUCTS_ENDPOINT}")
    
    try:
        response = requests.get(FAKE_STORE_PRODUCTS_ENDPOINT, timeout=30)
        response.raise_for_status()
        
        products = response.json()
        
        # Flatten the nested rating object
        for product in products:
            rating = product.pop('rating', {})
            product['rating_rate'] = rating.get('rate', 0.0)
            product['rating_count'] = rating.get('count', 0)
        
        df = pd.DataFrame(products)
        df['_extracted_at'] = datetime.utcnow()
        df['_source'] = 'fake_store_api'
        
        logger.info(f"[OK] Extracted {len(df)} products from API")
        logger.info(f"   Categories: {df['category'].unique().tolist()}")
        logger.info(f"   Price range: ${df['price'].min():.2f} - ${df['price'].max():.2f}")
        
        return df
        
    except requests.exceptions.RequestException as e:
        logger.error(f"[ERROR] API request failed: {e}")
        raise
    except Exception as e:
        logger.error(f"[ERROR] Error extracting API products: {e}")
        raise


def extract_api_categories() -> list:
    """
    Extract product categories from the Fake Store API.
    
    Returns:
        list of category strings
    """
    logger.info(f"[API] Extracting categories from API: {FAKE_STORE_CATEGORIES_ENDPOINT}")
    
    try:
        response = requests.get(FAKE_STORE_CATEGORIES_ENDPOINT, timeout=30)
        response.raise_for_status()
        
        categories = response.json()
        logger.info(f"[OK] Extracted {len(categories)} categories: {categories}")
        
        return categories
        
    except requests.exceptions.RequestException as e:
        logger.error(f"[ERROR] API request failed: {e}")
        raise


# =====================================================================
# Combined Extraction
# =====================================================================

def extract_all() -> dict:
    """
    Run all extractions and return a dictionary of DataFrames.
    
    Returns:
        dict with keys: 'retail_sales', 'api_products', 'api_categories'
    """
    logger.info("=" * 60)
    logger.info(">> STARTING DATA EXTRACTION FROM ALL SOURCES")
    logger.info("=" * 60)
    
    results = {}
    
    # Source 1: Retail Sales CSV
    results['retail_sales'] = extract_retail_sales()
    
    # Source 2: Fake Store API Products
    results['api_products'] = extract_api_products()
    
    # Source 2b: Fake Store API Categories
    results['api_categories'] = extract_api_categories()
    
    logger.info("=" * 60)
    logger.info("[OK] EXTRACTION COMPLETE")
    logger.info(f"   Total retail sales records: {len(results['retail_sales'])}")
    logger.info(f"   Total API products: {len(results['api_products'])}")
    logger.info(f"   Total API categories: {len(results['api_categories'])}")
    logger.info("=" * 60)
    
    return results
