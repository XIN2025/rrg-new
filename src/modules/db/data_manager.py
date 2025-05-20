import logging
import duckdb
import os
import time
import signal
from src.modules.db.db_common import ensure_duckdb_schema, remove_old_data
from src.modules.db.reference_data_loader import load_all_reference_data
from src.modules.db.eod_data_loader import load_eod_data
from src.modules.db.minute_data_loader import load_hourly_data as load_hourly_data_impl
from src.utils.logger import get_logger

logger = get_logger("data_manager")

def get_duckdb_path():
    """Get the DuckDB file path from environment or use default"""
    return os.environ.get("DUCKDB_PATH", "data/pydb.duckdb")


def ensure_tables_exist():
    """Ensure all required tables exist, creating them if needed"""
    try:
        # Try to get a connection with a timeout
        dd_con = get_duckdb_connection_with_retry()
        if not dd_con:
            logging.error("Failed to establish DuckDB connection after retries")
            return False
            
        # Check if tables exist
        required_tables = ["companies", "stocks", "eod_stock_data", "indices", "indices_stocks", "market_metadata"]
        missing_tables = []
        
        for table in required_tables:
            result = dd_con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table}'").fetchone()[0]
            if result == 0:
                missing_tables.append(table)
                
        if missing_tables:
            logging.info(f"Missing tables: {', '.join(missing_tables)}")
            load_data(force=True)  # Force creation of all tables
            return True
        else:
            logging.info("All required tables exist")
            return True
    except Exception as e:
        logging.error(f"Error ensuring tables exist: {str(e)}")
        return False
    finally:
        if 'dd_con' in locals() and dd_con:
            try:
                dd_con.close()
                logging.debug("DuckDB connection closed")
            except:
                pass


def get_duckdb_connection_with_retry(max_retries=5, retry_delay=2):
    """Attempt to get a DuckDB connection with retries"""
    db_path = get_duckdb_path()
    logging.debug(f"Using DuckDB path: {db_path}")
    
    for attempt in range(max_retries):
        try:
            # Check if another process has locked the DB
            if is_duckdb_locked(db_path):
                logging.warning(f"DuckDB file {db_path} is locked. Attempt {attempt+1}/{max_retries}...")
                time.sleep(retry_delay)
                continue
                
            # Try to get a connection
            dd_con = ensure_duckdb_schema(db_path)
            logging.info(f"Successfully connected to DuckDB at {db_path}")
            return dd_con
        except Exception as e:
            if "lock" in str(e).lower():
                logging.warning(f"Lock conflict on DuckDB file {db_path}. Attempt {attempt+1}/{max_retries}...")
                time.sleep(retry_delay)
            else:
                logging.error(f"Error connecting to DuckDB: {str(e)}")
                return None
    
    logging.error(f"Max retries reached. Could not get DuckDB connection to {db_path}.")
    return None


def is_duckdb_locked(db_path=None):
    """Check if the DuckDB file is locked by another process"""
    if db_path is None:
        db_path = get_duckdb_path()
        
    try:
        # Try a quick connection and immediate close to check lock status
        conn = duckdb.connect(db_path, read_only=True)
        conn.close()
        return False
    except Exception as e:
        if "lock" in str(e).lower():
            return True
        # If it's some other error, we'll assume it's not a lock issue
        return False


def load_data(force=False):
    """
    Load reference data and EOD stock data.
    This function maintains the original API from clickhouse-utils.py.
    """
    try:
        db_path = get_duckdb_path()
        logging.info(f"Starting data load process for {db_path}")
        
        # Get a connection with retry
        dd_con = get_duckdb_connection_with_retry()
        if not dd_con:
            logging.error(f"Could not obtain DuckDB connection for data loading on {db_path}")
            return False
            
        try:
            # Load reference data (companies, stocks, indices, etc.)
            logging.info("Loading reference data...")
            ref_result = load_all_reference_data(force)
            logging.info(f"Reference data load {'completed successfully' if ref_result else 'had some failures'}")
            
            # Load EOD stock data
            logging.info("Loading EOD stock data...")
            eod_result = load_eod_data(force)
            logging.info(f"EOD data load {'completed successfully' if eod_result else 'failed'}")

            # Load hourly stock data
            logging.info("Loading hourly stock data...")
            hourly_result = load_hourly_data_impl(force)
            logging.info(f"Hourly data load {'completed successfully' if hourly_result else 'failed'}")
            
            # Overall success only if both operations succeeded
            success = ref_result and eod_result and hourly_result
            logging.info(f"Data load process {'completed successfully' if success else 'had some failures'}")
            
            return success
        finally:
            # Make sure we close the connection
            try:
                dd_con.close()
                logging.debug("DuckDB connection closed")
            except:
                pass
    except Exception as e:
        logging.error(f"Error in load_data: {str(e)}")
        return False


def load_hourly_data(force=False):
    """Load hourly stock data with improved lock handling"""
    try:
        db_path = get_duckdb_path()
        logging.info(f"Starting hourly data load process for {db_path}")
        
        # Get a connection with retry
        dd_con = get_duckdb_connection_with_retry()
        if not dd_con:
            logging.error(f"Could not obtain DuckDB connection for hourly data loading on {db_path}")
            return False
            
        try:
            # Load hourly data 
            logging.info("Loading hourly stock data...")
            hourly_result = load_hourly_data_impl(force)  # Use the imported implementation
            logging.info(f"Hourly data load {'completed successfully' if hourly_result else 'failed'}")
            
            return hourly_result
        finally:
            try:
                dd_con.close()
                logging.debug("DuckDB connection closed")
            except:
                pass
    except Exception as e:
        logging.error(f"Error in load_hourly_data: {str(e)}")
        return False


def clear_duckdb_locks(db_path=None):
    """Attempt to clear any locks on the DuckDB file"""
    if db_path is None:
        db_path = get_duckdb_path()
    
    logger.info(f"🛠️ Attempting to clear locks for DuckDB at {db_path}")
    
    try:
        # More robust process cleanup using psutil
        import psutil
        for proc in psutil.process_iter(['pid', 'name', 'open_files']):
            try:
                if any(f.path == db_path for f in proc.open_files()):
                    logger.warning(f"🔒 Found locking process: PID={proc.pid} NAME={proc.name()}")
                    proc.terminate()
                    logger.info(f"✅ Successfully terminated process {proc.pid}")
            except Exception as e:
                logger.error(f"🚨 Error handling process {proc.pid}: {str(e)}")
        
        # Add retry logic for connection attempts
        max_retries = 3
        for attempt in range(max_retries):
            try:
                conn = duckdb.connect(db_path)
                conn.close()
                logger.info("🔓 DuckDB lock cleared successfully")
                return True
            except Exception as e:
                logger.warning(f"⚠️ Lock clearance attempt {attempt+1} failed: {str(e)}")
                time.sleep(1)
        
        return False
    except Exception as e:
        logger.error(f"🚨 Critical error in lock clearance: {str(e)}")
        return False


# Expose these functions to maintain the original API
__all__ = [
    'load_data',
    'load_hourly_data',
    'ensure_tables_exist',
    'remove_old_data',
    'clear_duckdb_locks',
    'get_duckdb_path'
]
