"""
MySQL Staging Module
─────────────────────────────────────────────────────────
Optional MySQL staging layer for intermediate data storage
before loading to BigQuery.
"""

import pandas as pd
import logging
from datetime import datetime

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

from config.settings import MYSQL_CONFIG

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════
# DATABASE CONNECTION
# ═══════════════════════════════════════════════════════════════════════

def get_mysql_connection():
    """Create and return a MySQL connection."""
    if not MYSQL_AVAILABLE:
        raise ImportError(
            "mysql-connector-python is not installed. "
            "Install with: pip install mysql-connector-python"
        )
    
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        if conn.is_connected():
            logger.info("✅ Connected to MySQL database")
            return conn
    except MySQLError as e:
        logger.error(f"❌ MySQL connection failed: {e}")
        raise


def create_staging_database():
    """Create the staging database and tables."""
    config_no_db = {k: v for k, v in MYSQL_CONFIG.items() if k != 'database'}
    
    try:
        conn = mysql.connector.connect(**config_no_db)
        cursor = conn.cursor()
        
        db_name = MYSQL_CONFIG['database']
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
        cursor.execute(f"USE `{db_name}`")
        
        # Create staging tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stg_retail_sales (
                id INT AUTO_INCREMENT PRIMARY KEY,
                transaction_id INT,
                sale_date DATE,
                customer_id VARCHAR(20),
                gender VARCHAR(10),
                age INT,
                product_category VARCHAR(50),
                quantity INT,
                price_per_unit DECIMAL(10, 2),
                total_amount DECIMAL(10, 2),
                row_hash VARCHAR(64),
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source VARCHAR(50),
                INDEX idx_customer (customer_id),
                INDEX idx_date (sale_date),
                INDEX idx_category (product_category)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stg_api_products (
                id INT AUTO_INCREMENT PRIMARY KEY,
                api_product_id INT,
                product_name VARCHAR(255),
                api_price DECIMAL(10, 2),
                description TEXT,
                product_category VARCHAR(100),
                product_image_url VARCHAR(500),
                rating_rate DECIMAL(3, 1),
                rating_count INT,
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source VARCHAR(50),
                INDEX idx_product_id (api_product_id),
                INDEX idx_category (product_category)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS etl_run_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                run_id VARCHAR(36),
                stage VARCHAR(20),
                status VARCHAR(20),
                records_processed INT,
                duration_seconds DECIMAL(10, 2),
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        
        conn.commit()
        logger.info(f"✅ MySQL staging database '{db_name}' created/verified")
        
        cursor.close()
        conn.close()
        
    except MySQLError as e:
        logger.error(f"❌ Failed to create staging database: {e}")
        raise


def load_to_staging(df: pd.DataFrame, table_name: str):
    """Load a DataFrame into a MySQL staging table."""
    conn = get_mysql_connection()
    cursor = conn.cursor()
    
    try:
        # Truncate existing data
        cursor.execute(f"TRUNCATE TABLE `{table_name}`")
        
        if table_name == 'stg_retail_sales':
            insert_sql = """
                INSERT INTO stg_retail_sales 
                (transaction_id, sale_date, customer_id, gender, age,
                 product_category, quantity, price_per_unit, total_amount,
                 row_hash, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            for _, row in df.iterrows():
                cursor.execute(insert_sql, (
                    int(row['transaction_id']),
                    row['date'].strftime('%Y-%m-%d'),
                    row['customer_id'],
                    row['gender'],
                    int(row['age']),
                    row['product_category'],
                    int(row['quantity']),
                    float(row['price_per_unit']),
                    float(row['total_amount']),
                    row.get('row_hash', ''),
                    row.get('_source', 'kaggle'),
                ))
                
        elif table_name == 'stg_api_products':
            insert_sql = """
                INSERT INTO stg_api_products
                (api_product_id, product_name, api_price, description,
                 product_category, product_image_url, rating_rate,
                 rating_count, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            for _, row in df.iterrows():
                cursor.execute(insert_sql, (
                    int(row.get('id', row.get('api_product_id', 0))),
                    row.get('title', row.get('product_name', '')),
                    float(row.get('price', row.get('api_price', 0))),
                    str(row.get('description', ''))[:500],
                    row.get('category', row.get('product_category', '')),
                    row.get('image', row.get('product_image_url', '')),
                    float(row.get('rating_rate', 0)),
                    int(row.get('rating_count', 0)),
                    row.get('_source', 'fake_store_api'),
                ))
        
        conn.commit()
        logger.info(
            f"✅ Loaded {len(df)} rows into MySQL staging: {table_name}"
        )
        
    except MySQLError as e:
        conn.rollback()
        logger.error(f"❌ MySQL staging load failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def read_from_staging(table_name: str) -> pd.DataFrame:
    """Read data from a MySQL staging table."""
    conn = get_mysql_connection()
    
    try:
        df = pd.read_sql(f"SELECT * FROM `{table_name}`", conn)
        logger.info(
            f"✅ Read {len(df)} rows from MySQL staging: {table_name}"
        )
        return df
    finally:
        conn.close()
