import logging
import time
import os
from src.modules.db.eod_data_loader import load_eod_data
from src.modules.db.minute_data_loader import load_hourly_data
from src.utils.logger import get_logger
from src.utils.duck_pool import DuckDBPool

logger = get_logger(__name__)

def cleanup_duckdb_locks():
    """Clean up any existing DuckDB locks"""
    try:
        # Get the DuckDB file path
        db_path = os.path.join("data", "pydb.duckdb")
        lock_path = f"{db_path}.lock"
        
        # Remove lock file if it exists
        if os.path.exists(lock_path):
            os.remove(lock_path)
            logger.info("Removed existing DuckDB lock file")
            
        # Close all connections in the pool
        DuckDBPool.close_all()
        logger.info("Closed all DuckDB connections")
        
        # Wait a moment for cleanup to take effect
        time.sleep(1)
        
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")

def test_data_loaders():
    """Test both EOD and hourly data loading"""
    try:
        # Clean up any existing locks
        cleanup_duckdb_locks()
        
        # Test EOD data loading
        logger.info("Testing EOD data loading...")
        eod_success = load_eod_data(days=5)  # Test with 5 days of data
        logger.info(f"EOD data loading {'succeeded' if eod_success else 'failed'}")
        
        # Clean up before next test
        cleanup_duckdb_locks()
        
        # Test hourly data loading
        logger.info("\nTesting hourly data loading...")
        hourly_success = load_hourly_data(days=5)  # Test with 5 days of data
        logger.info(f"Hourly data loading {'succeeded' if hourly_success else 'failed'}")
        
        # Final cleanup
        cleanup_duckdb_locks()
        
        return eod_success and hourly_success

    except Exception as e:
        logger.error(f"Error in test_data_loaders: {str(e)}")
        return False

if __name__ == "__main__":
    test_data_loaders() 
