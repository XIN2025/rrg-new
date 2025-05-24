from src.utils.logger import get_logger
from src.utils.duck_pool import get_duckdb_connection
from src.utils.clickhouse import get_clickhouse_connection
import polars as pl

logger = get_logger(__name__)

def init_database():
    """Initialize database with required tables and schemas."""
    try:
        with get_duckdb_connection() as conn:
            if not conn:
                logger.error("Failed to establish DuckDB connection")
                return False
            
            # Create public schema if not exists
            conn.execute("CREATE SCHEMA IF NOT EXISTS public;")
            
            # Drop and recreate market_metadata table to ensure correct schema
            conn.execute("DROP TABLE IF EXISTS public.market_metadata;")
            conn.execute("""
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
            
            # Create hourly_stock_data table
            conn.execute("""
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
            
            # Load initial metadata from ClickHouse if market_metadata is empty
            count = conn.execute("SELECT COUNT(*) FROM public.market_metadata").fetchone()[0]
            if count == 0:
                with get_clickhouse_connection() as ch_client:
                    # Fetch indices metadata
                    indices_query = """
                    SELECT 
                        security_token,
                        security_code,
                        company_name,
                        symbol,
                        alternate_symbol,
                        company_name as nse_index_name,
                        ticker,
                        is_fno,
                        stocks_count,
                        stocks,
                        security_codes,
                        security_tokens,
                        NULL as series,
                        NULL as category,
                        NULL as exchange_group,
                        5 as security_type_code,
                        LOWER(REPLACE(company_name, ' ', '-')) as slug
                    FROM strike.mv_indices
                    """
                    indices_result = ch_client.query(indices_query).result_rows
                    print('DEBUG: indices_result first row:', indices_result[0] if indices_result else 'EMPTY')
                    print('DEBUG: indices_result row length:', len(indices_result[0]) if indices_result else 'EMPTY')
                    
                    # Fetch stocks metadata
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
                        26 as security_type_code,
                        LOWER(REPLACE(company_name, ' ', '-')) as slug
                    FROM strike.mv_stocks
                    """
                    stocks_result = ch_client.query(stocks_query).result_rows
                    print('DEBUG: stocks_result first row:', stocks_result[0] if stocks_result else 'EMPTY')
                    print('DEBUG: stocks_result row length:', len(stocks_result[0]) if stocks_result else 'EMPTY')
                    
                    # Combine results
                    all_metadata = indices_result + stocks_result
                    
                    if all_metadata:
                        # Convert to DataFrame
                        df = pl.DataFrame(all_metadata, schema=[
                            "security_token", "security_code", "company_name", "symbol", "alternate_symbol",
                            "nse_index_name", "ticker", "is_fno", "stocks_count", "stocks", "security_codes", "security_tokens",
                            "series", "category", "exchange_group", "security_type_code", "slug"
                        ])
                        
                        # Insert into DuckDB
                        conn.execute("BEGIN TRANSACTION")
                        try:
                            conn.execute("INSERT INTO public.market_metadata SELECT * FROM df")
                            conn.execute("COMMIT")
                            logger.info("Successfully loaded initial metadata from ClickHouse")
                        except Exception as e:
                            conn.execute("ROLLBACK")
                            logger.error(f"Error loading initial metadata: {str(e)}")
                            return False
            
            return True
            
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        return False

def verify_database():
    """Verify database setup and data integrity."""
    try:
        with get_duckdb_connection() as conn:
            if not conn:
                logger.error("Failed to establish DuckDB connection")
                return False
            
            # Check if required tables exist
            required_tables = ["market_metadata", "hourly_stock_data"]
            for table in required_tables:
                exists = conn.execute(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table}'
                """).fetchone()[0]
                
                if not exists:
                    logger.error(f"Required table {table} does not exist")
                    return False
            
            # Check if market_metadata has data
            count = conn.execute("SELECT COUNT(*) FROM public.market_metadata").fetchone()[0]
            if count == 0:
                logger.error("market_metadata table is empty")
                return False
            
            # Check if hourly_stock_data has data
            count = conn.execute("SELECT COUNT(*) FROM public.hourly_stock_data").fetchone()[0]
            if count == 0:
                logger.warning("hourly_stock_data table is empty")
            
            return True
            
    except Exception as e:
        logger.error(f"Error verifying database: {str(e)}")
        return False

if __name__ == "__main__":
    # Initialize database
    if init_database():
        logger.info("Database initialized successfully")
        
        # Verify database
        if verify_database():
            logger.info("Database verification passed")
        else:
            logger.error("Database verification failed")
    else:
        logger.error("Database initialization failed") 
