"""
ETL Pipeline Orchestrator
---------------------------------------------------------
Main entry point for running the complete ETL pipeline.
Coordinates Extract -> Transform -> Load operations.
"""

import sys
import os
import io
import logging
import time
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.extract import extract_all
from etl.transform import transform_all
from etl.load import load_all


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Configure logging for the ETL pipeline."""
    log_format = (
        "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s"
    )
    
    # Use UTF-8 stream for stdout to handle special characters on Windows
    stdout_handler = logging.StreamHandler(
        stream=io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    )
    
    file_handler = logging.FileHandler(
        f"etl_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
        encoding='utf-8'
    )
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format=log_format,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[stdout_handler, file_handler],
    )
    return logging.getLogger("ETL_PIPELINE")


def run_pipeline(skip_load: bool = False):
    """
    Execute the full ETL pipeline.
    
    Args:
        skip_load: If True, skip BigQuery loading (useful for testing)
    """
    logger = setup_logging()
    
    pipeline_start = time.time()
    
    logger.info("+" + "=" * 58 + "+")
    logger.info("|  DataFoundation: Multi-source Retail Data Integration   |")
    logger.info("|  ETL Pipeline - Full Execution                          |")
    logger.info("+" + "=" * 58 + "+")
    logger.info(f"Pipeline started at: {datetime.now().isoformat()}")
    
    results = {
        'start_time': datetime.now(),
        'status': 'running',
        'stages': {},
    }
    
    try:
        # =========== STAGE 1: EXTRACT ===========
        logger.info("\n" + "#" * 60)
        logger.info("# STAGE 1: EXTRACT")
        logger.info("#" * 60)
        
        extract_start = time.time()
        extracted_data = extract_all()
        extract_time = time.time() - extract_start
        
        results['stages']['extract'] = {
            'status': 'success',
            'duration_seconds': round(extract_time, 2),
            'records': {
                'retail_sales': len(extracted_data['retail_sales']),
                'api_products': len(extracted_data['api_products']),
                'api_categories': len(extracted_data['api_categories']),
            }
        }
        
        # =========== STAGE 2: TRANSFORM ===========
        logger.info("\n" + "#" * 60)
        logger.info("# STAGE 2: TRANSFORM")
        logger.info("#" * 60)
        
        transform_start = time.time()
        transformed_data = transform_all(extracted_data)
        transform_time = time.time() - transform_start
        
        results['stages']['transform'] = {
            'status': 'success',
            'duration_seconds': round(transform_time, 2),
            'tables': {
                key: len(val) for key, val in transformed_data.items()
                if hasattr(val, '__len__')
            }
        }
        
        # =========== STAGE 3: LOAD ===========
        if not skip_load:
            logger.info("\n" + "#" * 60)
            logger.info("# STAGE 3: LOAD TO BIGQUERY")
            logger.info("#" * 60)
            
            load_start = time.time()
            load_stats = load_all(transformed_data)
            load_time = time.time() - load_start
            
            results['stages']['load'] = {
                'status': 'success',
                'duration_seconds': round(load_time, 2),
                'rows_loaded': load_stats,
            }
        else:
            logger.info("\n>> SKIPPING LOAD STAGE (skip_load=True)")
            results['stages']['load'] = {'status': 'skipped'}
        
        # =========== SUMMARY ===========
        total_time = time.time() - pipeline_start
        results['status'] = 'success'
        results['end_time'] = datetime.now()
        results['total_duration_seconds'] = round(total_time, 2)
        
        logger.info("\n" + "=" * 60)
        logger.info("[OK] PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Status: SUCCESS")
        logger.info(f"Total Duration: {total_time:.2f} seconds")
        for stage, info in results['stages'].items():
            status = info.get('status', 'unknown')
            duration = info.get('duration_seconds', 'N/A')
            logger.info(f"  {stage.upper()}: {status} ({duration}s)")
        logger.info("=" * 60)
        
        return results
        
    except Exception as e:
        total_time = time.time() - pipeline_start
        results['status'] = 'failed'
        results['error'] = str(e)
        results['end_time'] = datetime.now()
        results['total_duration_seconds'] = round(total_time, 2)
        
        logger.error(f"\n[FAILED] PIPELINE FAILED: {e}")
        logger.exception("Full traceback:")
        
        return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="DataFoundation ETL Pipeline"
    )
    parser.add_argument(
        "--skip-load", 
        action="store_true",
        help="Skip BigQuery loading (run extract + transform only)"
    )
    parser.add_argument(
        "--extract-only",
        action="store_true", 
        help="Run extraction only"
    )
    
    args = parser.parse_args()
    
    if args.extract_only:
        logger = setup_logging()
        logger.info("Running extraction only...")
        data = extract_all()
        logger.info("Extraction complete!")
    else:
        results = run_pipeline(skip_load=args.skip_load)
        
        if results['status'] == 'failed':
            sys.exit(1)
