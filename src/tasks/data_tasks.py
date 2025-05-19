import logging
import os
import shutil
from datetime import datetime, timedelta
from src.modules.db.data_manager import load_data, clear_duckdb_locks
from src.modules.db.reference_data_loader import (
    load_companies,
    load_stocks, 
    load_indices,
    load_indices_stocks,
    load_market_metadata,
    load_all_reference_data
)
from src.utils.logger import get_logger

logger = get_logger("data_tasks")


def update_reference_data():
    """
    Task to update only the reference data tables directly in the main DuckDB database.
    Updates market metadata, companies, indices, and stock indices tables.
    """
    logger.info("Starting reference data update task")
    
    try:
        # Call the reference data loader to update the tables directly
        # The force=True parameter ensures the tables are refreshed
        logger.info("Updating reference data tables")
        success = load_all_reference_data(force=True)
        
        if not success:
            logger.error("Failed to update reference data tables")
            return False
            
        logger.info("Reference data update completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error in reference data update: {str(e)}")
        return False

def purge_old_stock_data():
    """
    Task to purge old stock price data from DuckDB.
    Removes data older than specified thresholds:
    - Stock price data older than 4 months
    - Daily EOD data older than 30 days
    - Historical data older than 12 years
    """
    logger.info("Starting purge of old stock price data")
    
    try:
        # Calculate cutoff dates
        now = datetime.now()
        four_months_ago = now - timedelta(days=140)  # ~4 months 20 days
        twelve_years_ago = now - timedelta(days=365 * 12)
        
        # Format dates for SQL queries
        four_months_ago_str = four_months_ago.strftime('%Y-%m-%d')
        twelve_years_ago_str = twelve_years_ago.strftime('%Y-%m-%d')
        
        # Connect to DuckDB
        import duckdb
        conn = duckdb.connect('data/pydb.duckdb')
        
        try:
            # Delete intraday stock price data older than 4 months
            logger.info(f"Purging intraday stock price data older than {four_months_ago_str}")
            intraday_result = conn.execute("""
                DELETE FROM stock_prices 
                WHERE created_at < ?
                AND price_type = 'intraday'
            """, [four_months_ago_str])
            intraday_count = intraday_result.fetchone()[0] if intraday_result else 0
            
            
            # Delete historical EOD data older than 12 years
            logger.info(f"Purging historical stock data older than {twelve_years_ago_str}")
            historical_result = conn.execute("""
                DELETE FROM eod_stock_data 
                WHERE date_stamp < ?
            """, [twelve_years_ago_str])
            historical_count = historical_result.fetchone()[0] if historical_result else 0
            
            logger.info(f"Purged {intraday_count} intraday records,"
                        f"and {historical_count} historical records")
            
            # Commit changes
            conn.commit()
            logger.info("Data purge completed successfully")
            return True
            
        finally:
            # Close connection
            conn.close()
            
    except Exception as e:
        logger.error(f"Error during stock data purge: {str(e)}")
        return False

def clean_rrg_export_directories():
    """
    Task to delete all files from RRG export input and output directories.
    Removes all files from src/modules/rrg/exports/input and src/modules/rrg/exports/output.
    """
    logger.info("Starting cleanup of RRG export directories")
    
    input_dir = "src/modules/rrg/exports/input"
    output_dir = "src/modules/rrg/exports/output"
    
    try:
        # Clean input directory
        if os.path.exists(input_dir):
            logger.info(f"Cleaning input directory: {input_dir}")
            for filename in os.listdir(input_dir):
                file_path = os.path.join(input_dir, filename)
                try:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                    logger.debug(f"Removed {file_path}")
                except Exception as e:
                    logger.error(f"Error removing {file_path}: {str(e)}")
        
        # Clean output directory
        if os.path.exists(output_dir):
            logger.info(f"Cleaning output directory: {output_dir}")
            for filename in os.listdir(output_dir):
                file_path = os.path.join(output_dir, filename)
                try:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                    logger.debug(f"Removed {file_path}")
                except Exception as e:
                    logger.error(f"Error removing {file_path}: {str(e)}")
        
        logger.info("RRG export directories cleanup completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error during RRG export directories cleanup: {str(e)}")
        return False 