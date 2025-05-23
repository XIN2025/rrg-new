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
from src.modules.db.config import CHUNK_SIZE, MAX_RECORDS
from src.utils.clickhouse_pool import ClickHousePool

logger = get_logger("data_manager")

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
                logging.error("Failed to establish DuckDB connection")
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
                    logging.error(f"Failed to create table: {table}")
                    return False
            
            logging.info("All required tables created successfully")
            return True
            
    except Exception as e:
        logging.error(f"Error ensuring tables exist: {str(e)}")
        return False

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
                
            # Try to get a connection using the context manager
            with get_duckdb_connection() as dd_con:
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
    """Load all reference data into DuckDB"""
    try:
        # First test the database setup
        if not test_database_setup():
            logging.error("❌ Database setup test failed")
            return False
            
        with get_duckdb_connection() as dd_con:
            if not dd_con:
                logging.error("❌ Failed to establish DuckDB connection")
                return False
            
            # First ensure schema and tables exist
            if not ensure_tables_exist():
                logging.error("❌ Failed to create required tables")
                return False
            
            # Then ensure synced_tables exists
            if not ensure_synced_table_exists(dd_con):
                logging.error("❌ Failed to create synced_tables")
                return False
            
            # Load reference data
            if not load_all_reference_data(dd_con, force):
                logging.error("❌ Failed to load reference data")
                return False
                
            # Load EOD data
            if not load_eod_data(force):
                logging.error("❌ Failed to load EOD data")
                return False
                
            # Load hourly data
            if not load_hourly_data_impl(force):
                logging.error("❌ Failed to load hourly data")
                return False
            
            # Final verification
            if not verify_tables():
                logging.error("❌ Final table verification failed")
                return False
                
            return True
    except Exception as e:
        logging.error(f"❌ Error in load_data: {str(e)}")
        return False

def load_hourly_data(force=False):
    """Load hourly stock data with improved lock handling"""
    try:
        db_path = get_duckdb_path()
        logging.info(f"Starting hourly data load process for {db_path}")
        
        with get_duckdb_connection() as dd_con:
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

def verify_tables():
    """Verify that all required tables exist and have correct schemas"""
    try:
        with get_duckdb_connection() as dd_con:
            if not dd_con:
                logging.error("Failed to establish DuckDB connection")
                return False
                
            # Check if public schema exists
            schema_exists = dd_con.execute("""
                SELECT COUNT(*) 
                FROM information_schema.schemata 
                WHERE schema_name = 'public'
            """).fetchone()[0]
            
            if not schema_exists:
                logging.error("Public schema does not exist")
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
                    logging.error(f"Table {table} does not exist")
                    return False
                    
                # Get column info
                columns = dd_con.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table}'
                """).fetchall()
                
                logging.info(f"Table {table} has {len(columns)} columns:")
                for col in columns:
                    logging.info(f"  - {col[0]}: {col[1]}")
            
            return True
    except Exception as e:
        logging.error(f"Error verifying tables: {str(e)}")
        return False

def test_database_setup():
    """Test database setup and data loading"""
    try:
        # 1. Test connection
        with get_duckdb_connection() as dd_con:
            if not dd_con:
                logging.error("❌ Failed to establish DuckDB connection")
                return False
            logging.info("✅ Successfully connected to DuckDB")

            # 2. Test schema
            dd_con.execute("CREATE SCHEMA IF NOT EXISTS public;")
            schema_exists = dd_con.execute("""
                SELECT COUNT(*) 
                FROM information_schema.schemata 
                WHERE schema_name = 'public'
            """).fetchone()[0]
            if not schema_exists:
                logging.error("❌ Public schema does not exist")
                return False
            logging.info("✅ Public schema exists")

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
                indices_result = ClickHousePool.execute_query(indices_query)
                if indices_result:
                    # Insert into market_metadata
                    for row in indices_result:
                        dd_con.execute("""
                            INSERT INTO public.market_metadata (
                                security_token, security_code, company_name, symbol, alternate_symbol, nse_index_name, ticker, is_fno, stocks_count, stocks, security_codes, security_tokens, series, category, exchange_group, security_type_code
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [
                            row.get('security_token'),
                            row.get('security_code'),
                            row.get('company_name'),
                            row.get('symbol'),
                            row.get('alternate_symbol'),
                            row.get('nse_index_name'),
                            row.get('ticker'),
                            row.get('is_fno'),
                            row.get('stocks_count'),
                            row.get('stocks'),
                            row.get('security_codes'),
                            row.get('security_tokens'),
                            None, None, None, 5
                        ])
                    logging.info(f"✅ Loaded {len(indices_result)} indices records")
                else:
                    logging.error("❌ No indices data found")
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
                stocks_result = ClickHousePool.execute_query(stocks_query)
                if stocks_result:
                    for row in stocks_result:
                        dd_con.execute("""
                            INSERT INTO public.market_metadata (
                                security_token, security_code, company_name, symbol, alternate_symbol, nse_index_name, ticker, is_fno, stocks_count, stocks, security_codes, security_tokens, series, category, exchange_group, security_type_code
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [
                            row.get('security_token'),
                            row.get('security_code'),
                            row.get('company_name'),
                            row.get('symbol'),
                            row.get('alternate_symbol'),
                            None,
                            row.get('ticker'),
                            row.get('is_fno'),
                            None,
                            None,
                            None,
                            None,
                            row.get('series'),
                            row.get('category'),
                            row.get('exchange_group'),
                            26
                        ])
                    logging.info(f"✅ Loaded {len(stocks_result)} stocks records")
                else:
                    logging.error("❌ No stocks data found")
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
                
                logging.info(f"✅ Verified data: {indices_count} indices and {stocks_count} stocks")

                # 6. Test specific queries that RRG needs
                indices_symbols = dd_con.execute("""
                    SELECT symbol 
                    FROM public.market_metadata 
                    WHERE security_type_code = 5
                """).fetchall()
                
                if not indices_symbols:
                    logging.error("❌ No index symbols found in market_metadata")
                    return False
                logging.info(f"✅ Found {len(indices_symbols)} index symbols")

                stocks_symbols = dd_con.execute("""
                    SELECT symbol 
                    FROM public.market_metadata 
                    WHERE security_type_code = 26
                """).fetchall()
                
                if not stocks_symbols:
                    logging.error("❌ No stock symbols found in market_metadata")
                    return False
                logging.info(f"✅ Found {len(stocks_symbols)} stock symbols")

                return True

            except Exception as e:
                logging.error(f"❌ Error during data loading: {str(e)}")
                return False

    except Exception as e:
        logging.error(f"❌ Error in test_database_setup: {str(e)}")
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
