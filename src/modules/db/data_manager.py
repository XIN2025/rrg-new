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
from src.utils.duck_pool import get_duckdb_connection
from src.modules.db.config import MAX_RECORDS
from src.utils.clickhouse_pool import pool

# Define logger at module level
logger = get_logger("data_manager")

# Fallback in case logger is not in scope
try:
    logger
except NameError:
    import logging
    logger = logging.getLogger("data_manager")

def ensure_synced_table_exists(dd_con):
    """Ensure the synced_tables table exists with proper schema"""
    try:
        # First ensure public schema exists
        dd_con.execute("CREATE SCHEMA IF NOT EXISTS public;")
        
        # Then create the table
        dd_con.execute("""
            CREATE TABLE IF NOT EXISTS public.synced_tables (
                table_name VARCHAR PRIMARY KEY,
                updated_at TIMESTAMP WITH TIME ZONE,
                full_refresh_required BOOLEAN DEFAULT FALSE
            )
        """)
        return True
    except Exception as e:
        logger.error(f"Error creating synced_tables: {str(e)}")
        return False

def get_duckdb_path():
    """Get the DuckDB file path from environment or use default"""
    return os.environ.get("DUCKDB_PATH", "data/pydb.duckdb")

def ensure_tables_exist():
    """Ensure all required tables exist, creating them if needed"""
    try:
        with get_duckdb_connection() as dd_con:
            if not dd_con:
                logger.error("Failed to establish DuckDB connection")
                return False
            
            # First ensure public schema exists
            dd_con.execute("CREATE SCHEMA IF NOT EXISTS public;")
            
            # Create market_metadata table with proper schema
            dd_con.execute("""
                CREATE TABLE IF NOT EXISTS public.market_metadata (
                    security_token VARCHAR,
                    security_code VARCHAR,
                    company_name VARCHAR,
                    symbol VARCHAR,
                    alternate_symbol VARCHAR,
                    nse_index_name VARCHAR,
                    ticker VARCHAR,
                    is_fno BOOLEAN,
                    stocks_count INTEGER,
                    stocks VARCHAR[],
                    security_codes VARCHAR[],
                    security_tokens VARCHAR[],
                    series VARCHAR,
                    category VARCHAR,
                    exchange_group VARCHAR,
                    security_type_code INTEGER,
                    slug VARCHAR
                )
            """)
            
            # Create other required tables
            dd_con.execute("""
                CREATE TABLE IF NOT EXISTS public.companies (
                    name VARCHAR,
                    slug VARCHAR
                )
            """)
            
            dd_con.execute("""
                CREATE TABLE IF NOT EXISTS public.stocks (
                    company_name VARCHAR,
                    security_code VARCHAR
                )
            """)
            
            dd_con.execute("""
                CREATE TABLE IF NOT EXISTS public.indices (
                    security_code VARCHAR,
                    name VARCHAR,
                    slug VARCHAR,
                    symbol VARCHAR
                )
            """)
            
            dd_con.execute("""
                CREATE TABLE IF NOT EXISTS public.indices_stocks (
                    security_code VARCHAR
                )
            """)
            
            dd_con.execute("""
                CREATE TABLE IF NOT EXISTS public.hourly_stock_data (
                    timestamp TIMESTAMP WITH TIME ZONE,
                    symbol VARCHAR,
                    open_price DOUBLE,
                    high_price DOUBLE,
                    low_price DOUBLE,
                    close_price DOUBLE,
                    volume BIGINT,
                    security_code VARCHAR,
                    ticker VARCHAR
                )
            """)

            # Create stock_prices table
            dd_con.execute("""
                CREATE TABLE IF NOT EXISTS public.stock_prices (
                    current_price DOUBLE,
                    created_at TIMESTAMP WITH TIME ZONE,
                    security_code VARCHAR,
                    ticker VARCHAR,
                    symbol VARCHAR,
                    previous_close DOUBLE,
                    PRIMARY KEY (created_at, ticker)
                )
            """)

            # Create indexes for stock_prices
            dd_con.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_prices_created_at 
                ON public.stock_prices(created_at)
            """)
            
            dd_con.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker 
                ON public.stock_prices(ticker)
            """)

            # Create synced_tables
            dd_con.execute("""
                CREATE TABLE IF NOT EXISTS public.synced_tables (
                    table_name VARCHAR PRIMARY KEY,
                    updated_at TIMESTAMP WITH TIME ZONE,
                    full_refresh_required BOOLEAN DEFAULT FALSE
                )
            """)
            
            # Verify tables were created
            required_tables = [
                "companies", 
                "stocks", 
                "hourly_stock_data", 
                "indices", 
                "indices_stocks", 
                "market_metadata",
                "synced_tables"
            ]
            
            for table in required_tables:
                result = dd_con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table}'").fetchone()[0]
                if result == 0:
                    logger.error(f"Failed to create table: {table}")
                    return False
            
            logger.info("All required tables created successfully")
            return True
            
    except Exception as e:
        logger.error(f"Error ensuring tables exist: {str(e)}")
        return False

def get_duckdb_connection_with_retry(max_retries=5, retry_delay=2):
    """Attempt to get a DuckDB connection with retries"""
    db_path = get_duckdb_path()
    logger.debug(f"Using DuckDB path: {db_path}")
    
    for attempt in range(max_retries):
        try:
            # Try to get a connection using the context manager
            with get_duckdb_connection() as dd_con:
                logger.info(f"Successfully connected to DuckDB at {db_path}")
                return dd_con
        except Exception as e:
            logger.warning(f"Connection attempt {attempt+1}/{max_retries} failed: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error(f"Max retries reached. Could not get DuckDB connection to {db_path}.")
                return None
    
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
    """Load all data into DuckDB"""
    start_time = time.time()
    logger.info("Starting full data load...")
    
    try:
        # Test database setup first
        if not test_database_setup():
            logger.error("Database setup test failed")
            return False
            
        # Get DuckDB connection
        dd_con = get_duckdb_connection_with_retry()
        if not dd_con:
            logger.error("Failed to establish DuckDB connection")
            return False
            
        # Ensure tables exist
        if not ensure_tables_exist():
            logger.error("Failed to ensure tables exist")
            return False
            
        # Load reference data
        logger.info("Loading reference data...")
        if not load_all_reference_data(force):
            logger.error("Failed to load reference data")
            return False
            
        # Load EOD data
        logger.info("Loading EOD data...")
        if not load_eod_data():
            logger.error("Failed to load EOD data")
            return False
            
        # Load hourly data
        logger.info("Loading hourly data...")
        if not load_hourly_data_impl():
            logger.error("Failed to load hourly data")
            return False
            
        # Verify tables
        logger.info("Verifying tables...")
        if not verify_tables():
            logger.error("Table verification failed")
            return False
            
        elapsed_time = time.time() - start_time
        logger.info(f"Full data load completed successfully in {elapsed_time:.2f}s")
        return True
        
    except Exception as e:
        logger.error(f"Error in load_data: {str(e)}", exc_info=True)
        return False

def load_hourly_data(force=False):
    """Load hourly stock data with improved lock handling"""
    try:
        db_path = get_duckdb_path()
        logger.info(f"Starting hourly data load process for {db_path}")
        
        with get_duckdb_connection() as dd_con:
            if not dd_con:
                logger.error(f"Could not obtain DuckDB connection for hourly data loading on {db_path}")
                return False
            
            try:
                # Load hourly data 
                logger.info("Loading hourly stock data...")
                hourly_result = load_hourly_data_impl(force)  # Use the imported implementation
                logger.info(f"Hourly data load {'completed successfully' if hourly_result else 'failed'}")
                
                return hourly_result
            finally:
                try:
                    dd_con.close()
                    logger.debug("DuckDB connection closed")
                except:
                    pass
    except Exception as e:
        logger.error(f"Error in load_hourly_data: {str(e)}")
        return False

def clear_duckdb_locks(db_path=None):
    """Attempt to clear any locks on the DuckDB file"""
    if db_path is None:
        db_path = get_duckdb_path()
    
    logger.info(f"Attempting to clear locks for DuckDB at {db_path}")
    
    try:
        # Try to remove lock file directly
        lock_path = f"{db_path}.lock"
        if os.path.exists(lock_path):
            try:
                os.remove(lock_path)
                logger.info("Successfully removed lock file")
                return True
            except Exception as e:
                logger.warning(f"Could not remove lock file: {e}")
        
        # If direct removal fails, try process cleanup
        import psutil
        locks_cleared = False
        
        # Only check processes that are likely to be using the database
        target_processes = ['python', 'uvicorn', 'duckdb']
        
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                # Skip processes that aren't likely to be using the database
                if not any(target in proc.info['name'].lower() for target in target_processes):
                    continue
                    
                # Check if process has the file open
                try:
                    for file in proc.open_files():
                        if file.path == db_path:
                            logger.info(f"Found locking process: {proc.info['name']} (PID={proc.info['pid']})")
                            proc.terminate()
                            locks_cleared = True
                except (psutil.AccessDenied, psutil.NoSuchProcess):
                    # Skip processes we can't access
                    continue
                    
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                # Skip processes that no longer exist or we can't access
                continue
                
        return locks_cleared
        
    except Exception as e:
        logger.error(f"Error clearing DuckDB locks: {str(e)}")
        return False

def verify_tables():
    """Verify that all required tables exist and have correct schemas"""
    try:
        with get_duckdb_connection() as dd_con:
            if not dd_con:
                logger.error("Failed to establish DuckDB connection")
                return False
                
            # Check if public schema exists
            schema_exists = dd_con.execute("""
                SELECT COUNT(*) 
                FROM information_schema.schemata 
                WHERE schema_name = 'public'
            """).fetchone()[0]
            
            if not schema_exists:
                logger.error("Public schema does not exist")
                return False
                
            # Check all required tables
            required_tables = [
                "companies", 
                "stocks", 
                "hourly_stock_data", 
                "indices", 
                "indices_stocks", 
                "market_metadata",
                "synced_tables"
            ]
            
            for table in required_tables:
                # Check if table exists
                table_exists = dd_con.execute(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table}'
                """).fetchone()[0]
                
                if not table_exists:
                    logger.error(f"Table {table} does not exist")
                    return False
                    
                # Get column info
                columns = dd_con.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table}'
                """).fetchall()
                
                logger.info(f"Table {table} has {len(columns)} columns:")
                for col in columns:
                    logger.info(f"  - {col[0]}: {col[1]}")
            
            return True
    except Exception as e:
        logger.error(f"Error verifying tables: {str(e)}")
        return False

def test_database_setup():
    """Test database setup and data loading"""
    try:
        # 1. Test connection
        with get_duckdb_connection() as dd_con:
            if not dd_con:
                logger.error("❌ Failed to establish DuckDB connection")
                return False
            logger.info("✅ Successfully connected to DuckDB")

            # 2. Test schema
            dd_con.execute("CREATE SCHEMA IF NOT EXISTS public;")
            schema_exists = dd_con.execute("""
                SELECT COUNT(*) 
                FROM information_schema.schemata 
                WHERE schema_name = 'public'
            """).fetchone()[0]
            if not schema_exists:
                logger.error("❌ Public schema does not exist")
                return False
            logger.info("✅ Public schema exists")

            # 3. Test market_metadata table
            dd_con.execute("DROP TABLE IF EXISTS public.market_metadata;")
            dd_con.execute("""
                CREATE TABLE IF NOT EXISTS public.market_metadata (
                    security_token VARCHAR,
                    security_code VARCHAR,
                    company_name VARCHAR,
                    symbol VARCHAR,
                    alternate_symbol VARCHAR,
                    nse_index_name VARCHAR,
                    ticker VARCHAR,
                    is_fno BOOLEAN,
                    stocks_count INTEGER,
                    stocks VARCHAR[],
                    security_codes VARCHAR[],
                    security_tokens VARCHAR[],
                    series VARCHAR,
                    category VARCHAR,
                    exchange_group VARCHAR,
                    security_type_code INTEGER,
                    slug VARCHAR
                )
            """)
            
            # 4. Test data loading
            try:
                # Load indices data (fill missing columns with NULL/defaults)
                indices_query = """
                SELECT 
                    security_token,
                    security_code,
                    company_name,
                    symbol,
                    alternate_symbol,
                    nse_index_name,
                    ticker,
                    is_fno,
                    stocks_count,
                    stocks,
                    security_codes,
                    security_tokens,
                    NULL as series,
                    NULL as category,
                    NULL as exchange_group,
                    5 as security_type_code
                FROM strike.mv_indices
                """
                indices_result = pool.execute_query(indices_query)
                if indices_result:
                    # Insert into market_metadata
                    for row in indices_result:
                        dd_con.execute("""
                            INSERT INTO public.market_metadata (
                                security_token, security_code, company_name, symbol, alternate_symbol, nse_index_name, ticker, is_fno, stocks_count, stocks, security_codes, security_tokens, series, category, exchange_group, security_type_code
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [
                            row[0] if isinstance(row, tuple) else row.get('security_token'),
                            row[1] if isinstance(row, tuple) else row.get('security_code'),
                            row[2] if isinstance(row, tuple) else row.get('company_name'),
                            row[3] if isinstance(row, tuple) else row.get('symbol'),
                            row[4] if isinstance(row, tuple) else row.get('alternate_symbol'),
                            row[5] if isinstance(row, tuple) else row.get('nse_index_name'),
                            row[6] if isinstance(row, tuple) else row.get('ticker'),
                            row[7] if isinstance(row, tuple) else row.get('is_fno'),
                            row[8] if isinstance(row, tuple) else row.get('stocks_count'),
                            row[9] if isinstance(row, tuple) else row.get('stocks'),
                            row[10] if isinstance(row, tuple) else row.get('security_codes'),
                            row[11] if isinstance(row, tuple) else row.get('security_tokens'),
                            None,
                            None,
                            None,
                            5
                        ])
                    logger.info(f"✅ Loaded {len(indices_result)} indices records")
                else:
                    logger.error("❌ No indices data found")
                    return False

                # Load stocks data (fill missing columns with NULL/defaults)
                stocks_query = """
                SELECT 
                    security_token,
                    security_code,
                    company_name,
                    symbol,
                    alternate_symbol,
                    NULL as nse_index_name,
                    ticker,
                    is_fno,
                    NULL as stocks_count,
                    NULL as stocks,
                    NULL as security_codes,
                    NULL as security_tokens,
                    series,
                    category,
                    exchange_group,
                    26 as security_type_code
                FROM strike.mv_stocks
                """
                stocks_result = pool.execute_query(stocks_query)
                if stocks_result:
                    for row in stocks_result:
                        dd_con.execute("""
                            INSERT INTO public.market_metadata (
                                security_token, security_code, company_name, symbol, alternate_symbol, nse_index_name, ticker, is_fno, stocks_count, stocks, security_codes, security_tokens, series, category, exchange_group, security_type_code
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [
                            row[0] if isinstance(row, tuple) else row.get('security_token'),
                            row[1] if isinstance(row, tuple) else row.get('security_code'),
                            row[2] if isinstance(row, tuple) else row.get('company_name'),
                            row[3] if isinstance(row, tuple) else row.get('symbol'),
                            row[4] if isinstance(row, tuple) else row.get('alternate_symbol'),
                            None,
                            row[6] if isinstance(row, tuple) else row.get('ticker'),
                            row[7] if isinstance(row, tuple) else row.get('is_fno'),
                            None,
                            None,
                            None,
                            None,
                            row[12] if isinstance(row, tuple) else row.get('series'),
                            row[13] if isinstance(row, tuple) else row.get('category'),
                            row[14] if isinstance(row, tuple) else row.get('exchange_group'),
                            26
                        ])
                    logger.info(f"✅ Loaded {len(stocks_result)} stocks records")
                else:
                    logger.error("❌ No stocks data found")
                    return False

                # 5. Verify data was loaded
                indices_count = dd_con.execute("""
                    SELECT COUNT(*) 
                    FROM public.market_metadata 
                    WHERE security_type_code = 5
                """).fetchone()[0]
                
                stocks_count = dd_con.execute("""
                    SELECT COUNT(*) 
                    FROM public.market_metadata 
                    WHERE security_type_code = 26
                """).fetchone()[0]
                
                logger.info(f"✅ Verified data: {indices_count} indices and {stocks_count} stocks")

                # 6. Test specific queries that RRG needs
                indices_symbols = dd_con.execute("""
                    SELECT symbol 
                    FROM public.market_metadata 
                    WHERE security_type_code = 5
                """).fetchall()
                
                if not indices_symbols:
                    logger.error("❌ No index symbols found in market_metadata")
                    return False
                logger.info(f"✅ Found {len(indices_symbols)} index symbols")

                stocks_symbols = dd_con.execute("""
                    SELECT symbol 
                    FROM public.market_metadata 
                    WHERE security_type_code = 26
                """).fetchall()
                
                if not stocks_symbols:
                    logger.error("❌ No stock symbols found in market_metadata")
                    return False
                logger.info(f"✅ Found {len(stocks_symbols)} stock symbols")

                return True

            except Exception as e:
                logger.error(f"❌ Error during data loading: {str(e)}")
                return False

    except Exception as e:
        logger.error(f"❌ Error in test_database_setup: {str(e)}")
        return False

# Expose these functions to maintain the original API
__all__ = [
    'load_data',
    'load_hourly_data',
    'ensure_tables_exist',
    'remove_old_data',
    'clear_duckdb_locks',
    'get_duckdb_path',
    'verify_tables',
    'test_database_setup'
]
