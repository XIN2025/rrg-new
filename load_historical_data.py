from src.utils.duck_pool import get_duckdb_connection
from src.utils.clickhouse_pool import pool
import polars as pl
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def check_data_exists():
    """Check if data already exists in DuckDB."""
    try:
        with get_duckdb_connection() as conn:
            # Check if schema exists
            schema_exists = conn.execute("""
                SELECT COUNT(*) 
                FROM information_schema.schemata 
                WHERE schema_name = 'public'
            """).fetchone()[0] > 0
            
            if not schema_exists:
                print("Public schema does not exist")
                return False
                
            # Check if table exists and has data
            table_exists = conn.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'eod_stock_data'
            """).fetchone()[0] > 0
            
            if not table_exists:
                print("eod_stock_data table does not exist")
                return False
                
            # Check data
            result = conn.execute("""
                SELECT COUNT(*) as count,
                       MIN(created_at) as earliest_date,
                       MAX(created_at) as latest_date
                FROM public.eod_stock_data
            """).fetchone()
            
            if result and result[0] > 0:
                print(f"Data already exists in DuckDB:")
                print(f"Total records: {result[0]:,}")
                print(f"Date range: {result[1]} to {result[2]}")
                return True
            return False
    except Exception as e:
        print(f"Error checking data: {str(e)}")
        return False

def load_market_metadata(force_reload=False):
    """Load market metadata from ClickHouse into DuckDB."""
    try:
        with get_duckdb_connection() as conn:
            # Check if data exists
            if not force_reload:
                try:
                    count = conn.execute("SELECT COUNT(*) FROM public.market_metadata").fetchone()[0]
                    if count > 0:
                        print(f"Market metadata already exists with {count} records")
                        return True
                except Exception:
                    pass  # Table doesn't exist, continue with loading

        print("Loading market metadata...")
        query = """
            SELECT 
                security_token,
                security_code,
                company_name,
                symbol,
                alternate_symbol,
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
        
        result = pool.execute_query(query)
        if not result:
            print("No market metadata returned from ClickHouse")
            return False
        df = pl.DataFrame(result, schema=[
            "security_token", "security_code", "company_name", "symbol", "alternate_symbol",
            "ticker", "is_fno", "stocks_count", "stocks", "security_codes", "security_tokens",
            "series", "category", "exchange_group", "security_type_code"
        ])
        print(f"Fetched {len(df)} market metadata records")
        
        with get_duckdb_connection() as conn:
            # Create schema if it doesn't exist
            conn.execute("CREATE SCHEMA IF NOT EXISTS public")
            
            # Drop and recreate market_metadata table
            conn.execute("DROP TABLE IF EXISTS public.market_metadata")
            conn.execute("""
                CREATE TABLE public.market_metadata (
                    security_token VARCHAR,
                    security_code VARCHAR,
                    company_name VARCHAR,
                    symbol VARCHAR,
                    alternate_symbol VARCHAR,
                    ticker VARCHAR,
                    is_fno BOOLEAN,
                    stocks_count INTEGER,
                    stocks VARCHAR,
                    security_codes VARCHAR,
                    security_tokens VARCHAR,
                    series VARCHAR,
                    category VARCHAR,
                    exchange_group VARCHAR,
                    security_type_code INTEGER
                )
            """)
            
            # Insert metadata
            df_arrow = df.to_arrow()
            conn.execute("INSERT INTO public.market_metadata SELECT * FROM df_arrow")
            
            # Verify insertion
            count = conn.execute("SELECT COUNT(*) FROM public.market_metadata").fetchone()[0]
            print(f"Successfully loaded {count} market metadata records")
            
            return True
            
    except Exception as e:
        print(f"Error loading market metadata: {str(e)}")
        return False

def load_historical_data():
    """Load historical data with proper technical indicators."""
    try:
        # First check if data already exists
        if check_data_exists():
            print("Data already loaded. Skipping load process.")
            return True

        # Load market metadata first
        if not load_market_metadata():
            print("Failed to load market metadata")
            return False

        # Calculate date range (3 months)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)
        
        print(f"Loading data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Query to get historical data with all required calculations, in the correct column order
        query = f"""
            WITH ordered_data AS (
                SELECT
                    e.date_time as created_at,
                    e.close / NULLIF(lagInFrame(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time), 0) as ratio,
                    e.close / NULLIF(avg(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time ROWS BETWEEN 20 PRECEDING AND CURRENT ROW), 0) as momentum,
                    e.close as close_price,
                    (e.close - NULLIF(lagInFrame(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time), 0)) / 
                    NULLIF(lagInFrame(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time), 0) * 100 as change_percentage,
                    e.close / NULLIF(avg(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), 0) as metric_1,
                    e.close / NULLIF(avg(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time ROWS BETWEEN 10 PRECEDING AND CURRENT ROW), 0) as metric_2,
                    e.close / NULLIF(avg(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time ROWS BETWEEN 15 PRECEDING AND CURRENT ROW), 0) as metric_3,
                    CASE 
                        WHEN e.close > lagInFrame(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time) THEN 1
                        WHEN e.close < lagInFrame(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time) THEN 2
                        ELSE 0
                    END as signal,
                    s.security_code,
                    lagInFrame(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time) as previous_close,
                    s.ticker
                FROM strike.equity_prices_1d e
                JOIN strike.mv_stocks s ON e.security_code = s.security_code
                WHERE e.date_time BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
                AND e.close > 0
                ORDER BY e.date_time DESC
            )
            SELECT * FROM ordered_data
        """
        
        print("Executing ClickHouse query...")
        result = pool.execute_query(query)
        
        if not result:
            print("No data returned from ClickHouse")
            return False
            
        # Define the correct column order
        columns = [
            "created_at", "ratio", "momentum", "close_price", "change_percentage",
            "metric_1", "metric_2", "metric_3", "signal", "security_code", "previous_close", "ticker"
        ]
        # Convert to Polars DataFrame with explicit columns
        df = pl.DataFrame(result, schema=columns)
        print(f"Fetched {len(df)} records")
        print("DataFrame columns:", df.columns)
        print("DataFrame dtypes:", df.dtypes)
        print(df.head(3))
        
        # Load into DuckDB
        with get_duckdb_connection() as conn:
            # Create schema if it doesn't exist
            conn.execute("CREATE SCHEMA IF NOT EXISTS public")
            
            # Create market_metadata table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS public.market_metadata (
                    security_code VARCHAR,
                    ticker VARCHAR,
                    symbol VARCHAR,
                    security_type_code INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create eod_stock_data table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS public.eod_stock_data (
                    created_at TIMESTAMP,
                    ratio DECIMAL(18,6),
                    momentum DECIMAL(18,6),
                    close_price DECIMAL(18,2),
                    change_percentage DECIMAL(18,6),
                    metric_1 DECIMAL(18,6),
                    metric_2 DECIMAL(18,6),
                    metric_3 DECIMAL(18,6),
                    signal INTEGER,
                    security_code VARCHAR,
                    previous_close DECIMAL(18,2),
                    ticker VARCHAR
                )
            """)
            
            # Insert new data
            df_arrow = df.to_arrow()
            conn.execute("INSERT INTO public.eod_stock_data SELECT * FROM df_arrow")
            
            # Verify insertion
            count = conn.execute("SELECT COUNT(*) FROM public.eod_stock_data").fetchone()[0]
            print(f"Successfully loaded {count} records into DuckDB")
            
            # Verify data completeness
            completeness = conn.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT ticker) as unique_tickers,
                    MIN(created_at) as earliest_date,
                    MAX(created_at) as latest_date,
                    COUNT(CASE WHEN ratio IS NOT NULL THEN 1 END) as has_ratio,
                    COUNT(CASE WHEN momentum IS NOT NULL THEN 1 END) as has_momentum,
                    COUNT(CASE WHEN change_percentage IS NOT NULL THEN 1 END) as has_change,
                    COUNT(CASE WHEN signal IS NOT NULL THEN 1 END) as has_signal,
                    COUNT(CASE WHEN metric_1 IS NOT NULL THEN 1 END) as has_metric_1,
                    COUNT(CASE WHEN metric_2 IS NOT NULL THEN 1 END) as has_metric_2,
                    COUNT(CASE WHEN metric_3 IS NOT NULL THEN 1 END) as has_metric_3
                FROM public.eod_stock_data
            """).fetchdf()
            
            print("\nData completeness after load:")
            print(completeness)
            
            return True
            
    except Exception as e:
        print(f"Error loading historical data: {str(e)}")
        return False

if __name__ == "__main__":
    # Force reload market metadata
    load_market_metadata(force_reload=True)
    # Then load historical data
    load_historical_data() 
