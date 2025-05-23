import logging
import duckdb
import polars as pl
from datetime import datetime, timezone, timedelta
import pytz
from src.db.clickhouse import pool as ClickHousePool
from src.modules.db.db_common import (
    get_pg_connection,
    ensure_duckdb_schema,
    get_last_sync_timestamp,
    update_sync_timestamp,
    create_duckdb_table
)
from src.utils.duck_pool import get_duckdb_connection
from src.utils.clickhouse_pool import ClickHousePool
from src.utils.metrics import TimerMetric, DuckDBQueryTimer
from src.utils.logger import get_logger
from src.modules.db.config import CHUNK_SIZE, MAX_RECORDS

# Define timezone
INDIAN_TZ = pytz.timezone('Asia/Kolkata')

# Define columns for stock_prices table
stock_prices_columns = ["current_price", "created_at", "security_code", "ticker", "symbol", "previous_close"]

# Base ClickHouse query for stock data( loads 0 to 2 months data)
base_stock_data_query1 = """
WITH active_stocks AS (
    SELECT security_token
    FROM strike.mv_stocks
    LIMIT 1000
)
SELECT 
    CAST(ep.close AS Float64) as current_price,
    ep.date_time as created_at,
    ms.security_code AS security_code,
    ms.ticker AS ticker,
    ms.symbol AS symbol,
    lagInFrame(CAST(ep.close AS Float64)) OVER (PARTITION BY ep.security_token ORDER BY ep.date_time) AS previous_close
FROM 
    strike.equity_prices_1min AS ep
INNER JOIN active_stocks AS as
    ON ep.security_token = as.security_token
INNER JOIN (
    SELECT 
        security_token,
        any(security_code) AS security_code,
        any(ticker) AS ticker,
        any(symbol) AS symbol
    FROM strike.mv_stocks
    GROUP BY security_token
) AS ms 
    ON ep.security_token = ms.security_token
WHERE 
    ep.date_time > '{current_time}' - INTERVAL '1 months' - INTERVAL '15 days'
    AND toMinute(ep.date_time) % 5 = 0
    AND ep.close != 0
ORDER BY ep.date_time DESC
LIMIT 10000
"""

# Base ClickHouse query for stock data( loads 2 months to 4 months data)
base_stock_data_query2 = """
WITH active_stocks AS (
    SELECT security_token
    FROM strike.mv_stocks
    LIMIT 1000
)
SELECT 
    CAST(ep.close AS Float64) as current_price,
    ep.date_time as created_at,
    ms.security_code AS security_code,
    ms.ticker AS ticker,
    ms.symbol AS symbol,
    lagInFrame(CAST(ep.close AS Float64)) OVER (PARTITION BY ep.security_token ORDER BY ep.date_time) AS previous_close
FROM 
    strike.equity_prices_1min AS ep
INNER JOIN active_stocks AS as
    ON ep.security_token = as.security_token
INNER JOIN (
    SELECT 
        security_token,
        any(security_code) AS security_code,
        any(ticker) AS ticker,
        any(symbol) AS symbol
    FROM strike.mv_stocks
    GROUP BY security_token
) AS ms 
    ON ep.security_token = ms.security_token
WHERE 
    ep.date_time > '{current_time}' - INTERVAL '3 months'
    AND ep.date_time <= '{current_time}' - INTERVAL '1 months' - INTERVAL '15 days'
    AND toMinute(ep.date_time) % 5 = 0
    AND ep.close != 0
ORDER BY ep.date_time DESC
LIMIT 10000
"""

base_stock_data_query3 = """
WITH active_stocks AS (
    SELECT security_token
    FROM strike.mv_stocks
    LIMIT 1000
)
SELECT 
    CAST(ep.close AS Float64) as current_price,
    ep.date_time as created_at,
    ms.security_code AS security_code,
    ms.ticker AS ticker,
    ms.symbol AS symbol,
    lagInFrame(CAST(ep.close AS Float64)) OVER (PARTITION BY ep.security_token ORDER BY ep.date_time) AS previous_close
FROM 
    strike.equity_prices_1min AS ep
INNER JOIN active_stocks AS as
    ON ep.security_token = as.security_token
INNER JOIN (
    SELECT 
        security_token,
        any(security_code) AS security_code,
        any(ticker) AS ticker,
        any(symbol) AS symbol
    FROM strike.mv_stocks
    GROUP BY security_token
) AS ms 
    ON ep.security_token = ms.security_token
WHERE 
    ep.date_time > '{current_time}' - INTERVAL '4 months' - INTERVAL '10 days'
    AND ep.date_time <= '{current_time}' - INTERVAL '3 months'
    AND toMinute(ep.date_time) % 5 = 0
    AND ep.close != 0
ORDER BY ep.date_time DESC
LIMIT 10000
"""

# Incremental query for updates after initial load
incremental_stock_data_query = """
WITH active_stocks AS (
    SELECT security_token
    FROM strike.mv_stocks
    LIMIT 1000
)
SELECT 
    CAST(ep.close AS Float64) as current_price,
    ep.date_time as created_at,
    ms.security_code AS security_code,
    ms.ticker AS ticker,
    ms.symbol AS symbol,
    lagInFrame(CAST(ep.close AS Float64)) OVER (PARTITION BY ep.security_token ORDER BY ep.date_time) AS previous_close
FROM 
    strike.equity_prices_1min AS ep
INNER JOIN active_stocks AS as
    ON ep.security_token = as.security_token
INNER JOIN (
    SELECT 
        security_token,
        any(security_code) AS security_code,
        any(ticker) AS ticker,
        any(symbol) AS symbol
    FROM strike.mv_stocks
    GROUP BY security_token
) AS ms 
    ON ep.security_token = ms.security_token
WHERE 
    ep.date_time >= '{last_updated}'
    AND ep.close != 0
    AND toMinute(ep.date_time) % 5 = 0
ORDER BY ep.date_time DESC
LIMIT 10000
"""

# Base ClickHouse query for index data
base_index_data_query = """
WITH active_indices AS (
    SELECT security_token
    FROM strike.mv_indices
    LIMIT 100
)
SELECT 
    CAST(ip.close AS Float64) AS current_price,
    ip.date_time AS created_at,
    mi.security_code AS security_code,
    mi.ticker AS ticker,
    mi.symbol AS symbol,
    lagInFrame(CAST(ip.close AS Float64)) OVER (PARTITION BY ip.security_token ORDER BY ip.date_time) AS previous_close
FROM 
    strike.index_prices_1min AS ip
INNER JOIN active_indices AS ai
    ON ip.security_token = ai.security_token
INNER JOIN (
    SELECT 
        security_token,
        any(security_code) AS security_code,
        any(ticker) AS ticker,
        any(symbol) AS symbol
    FROM strike.mv_indices
    GROUP BY security_token
) AS mi
    ON ip.security_token = mi.security_token
WHERE 
    ip.date_time > '{current_time}' - interval '4 months' - interval '10 days'
    AND toMinute(ip.date_time) % 5 = 0
    AND ip.close != 0
ORDER BY 
    ip.date_time DESC
LIMIT 10000
"""

# Incremental query for index data updates
incremental_index_data_query = """
WITH active_indices AS (
    SELECT security_token
    FROM strike.mv_indices
    LIMIT 100
)
SELECT 
    CAST(ip.close AS Float64) AS current_price,
    ip.date_time AS created_at,
    mi.security_code AS security_code,
    mi.ticker AS ticker,
    mi.symbol AS symbol,
    lagInFrame(CAST(ip.close AS Float64)) OVER (PARTITION BY ip.security_token ORDER BY ip.date_time) AS previous_close
FROM 
    strike.index_prices_1min AS ip
INNER JOIN active_indices AS ai
    ON ip.security_token = ai.security_token
INNER JOIN (
    SELECT 
        security_token,
        any(security_code) AS security_code,
        any(ticker) AS ticker,
        any(symbol) AS symbol
    FROM strike.mv_indices
    GROUP BY security_token
) AS mi
    ON ip.security_token = mi.security_token
WHERE 
    ip.date_time >= '{last_updated}'
    AND toMinute(ip.date_time) % 5 = 0
    AND ip.close != 0
ORDER BY 
    ip.date_time DESC
LIMIT 10000
"""

def insert_data_into_duckdb(df, dd_con):
    """Insert data into DuckDB"""
    # Convert to arrow for DuckDB
    df_arrow = df.to_arrow()
    
    # Insert the new records
    insert_result = dd_con.execute("""
        INSERT INTO public.stock_prices 
        SELECT * FROM df_arrow
        WHERE NOT EXISTS (
            SELECT 1 FROM public.stock_prices s
            WHERE s.created_at = df_arrow.created_at
            AND s.ticker = df_arrow.ticker
        )
    """)

    inserted_count = insert_result.fetchone()[0]
    logging.info(f"Inserted {inserted_count} records")

def process_stock_data(df):
    df = df.with_columns(
        pl.col("created_at").dt.replace_time_zone(INDIAN_TZ)
    )
    df = df.with_columns(
        pl.col("created_at").dt.replace_time_zone(None).alias("created_at_naive")
    )
    df = df.with_columns(
        pl.col("created_at").dt.replace_time_zone(None).alias("created_at")
    )
    return df

def validate_query_result(result, expected_columns):
    """Validate query result structure and data"""
    if not result:
        return False, "Empty result"
        
    if not isinstance(result, (list, tuple)):
        return False, "Invalid result type"
        
    if not result[0]:
        return False, "Empty first row"
        
    # Check if all expected columns are present
    row = result[0]
    if isinstance(row, dict):
        values = [row.get(col) for col in expected_columns]
    else:
        values = row
    if len(values) != len(expected_columns):
        return False, f"Column count mismatch. Expected {len(expected_columns)}, got {len(values)}"
        
    # Check data types
    for i, (value, col_name) in enumerate(zip(values, expected_columns)):
        if value is None:
            return False, f"Null value in column {col_name}"
        if col_name in ["current_price", "previous_close"]:
            # Accept float, int, or numeric string
            if not (isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit())):
                return False, f"Invalid price value in {col_name}: {value} ({type(value)})"
        elif col_name == "created_at":
            if not isinstance(value, str):
                return False, f"Invalid date value in {col_name}: {value} ({type(value)})"
        elif col_name in ["security_code", "ticker", "symbol"]:
            if not isinstance(value, str):
                return False, f"Invalid string value in {col_name}: {value} ({type(value)})"
    return True, "Valid result"

def process_data_in_chunks(query, dd_con, max_retries=3):
    """Process data in chunks with better error handling and validation"""
    offset = 0
    total_processed = 0
    max_records = 1000000  # Maximum records to process
    retry_count = 0
    
    # Expected columns in the result
    expected_columns = ["current_price", "created_at", "security_code", "ticker", "symbol", "previous_close"]
    
    while True:
        try:
            if total_processed >= max_records:
                logging.info(f"Reached maximum record limit of {max_records}")
                break
                
            # Validate query before execution
            if not query.strip() or "SELECT" not in query.upper():
                raise ValueError("Invalid query format")
                
            # Add LIMIT and OFFSET to the query properly
            chunk_query = f"""
            {query}
            LIMIT {CHUNK_SIZE}
            OFFSET {offset}
            """
            
            # Add timeout to query execution
            result = ClickHousePool.execute_query(chunk_query, timeout=300)  # 5 minute timeout
            
            # Validate result
            is_valid, validation_msg = validate_query_result(result, expected_columns)
            if not is_valid:
                raise ValueError(f"Invalid query result: {validation_msg}")
            
            if not result:
                break
                
            df = pl.DataFrame(result)
            if len(df) == 0:
                break
                
            # Convert to Arrow and load into DuckDB
            df_arrow = df.to_arrow()
            
            # Begin transaction
            dd_con.execute("BEGIN TRANSACTION")
            try:
                # Insert the new records
                insert_result = dd_con.execute("""
                    INSERT INTO public.stock_prices 
                    SELECT * FROM df_arrow
                    WHERE NOT EXISTS (
                        SELECT 1 FROM public.stock_prices s
                        WHERE s.created_at = df_arrow.created_at
                        AND s.ticker = df_arrow.ticker
                    )
                """)
                
                inserted_count = insert_result.fetchone()[0]
                logging.info(f"Inserted {inserted_count} records in current chunk")
                
                # Commit transaction
                dd_con.execute("COMMIT")
                
                processed_count = len(df)
                total_processed += processed_count
                logging.info(f"Processed chunk of {processed_count} records (Total: {total_processed})")
                
                offset += CHUNK_SIZE
                retry_count = 0  # Reset retry count on success
                
            except Exception as e:
                # Rollback on error
                dd_con.execute("ROLLBACK")
                raise e
                
            finally:
                # Clear memory
                del df
                del df_arrow
                import gc
                gc.collect()
                
                # Add a small delay to prevent overwhelming the system
                import time
                time.sleep(0.2)  # Increased delay to 0.2 seconds
                
        except Exception as e:
            retry_count += 1
            logging.error(f"Error processing chunk (attempt {retry_count}/{max_retries}): {str(e)}")
            
            if retry_count >= max_retries:
                logging.error("Max retries reached, stopping processing")
                raise
                
            # Wait before retrying
            time.sleep(1)
            continue

def handle_hourly_stock_data(dd_con=None, pg_connection=None, force=False):
    """Handle incremental updates and bulk loading for hourly stock and index data with proper chunking"""
    try:
        if dd_con is None:
            dd_con = ensure_duckdb_schema()
            
        if pg_connection is None:
            pg_connection = get_pg_connection()
            
        # Check if table exists
        table_exists = dd_con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'stock_prices'"
        ).fetchone()[0]
        
        # Define chunk size for processing - reduced to prevent memory issues
        CHUNK_SIZE = 200000  # Further reduced chunk size
        
        if not table_exists or force:
            # If table doesn't exist or force refresh, load all data in chunks
            logging.info("Stock prices table doesn't exist or force refresh requested, loading full data")
            
            # Create the table schema with proper constraints
            dd_con.execute(f"""
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
            
            # Create indexes for better performance
            dd_con.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_prices_created_at 
                ON public.stock_prices(created_at)
            """)
            
            dd_con.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker 
                ON public.stock_prices(ticker)
            """)
            
            logging.info("Created table and indexes")
            current_time = datetime.now(timezone(timedelta(hours=5, minutes=30)))
            current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
            
            # Process each query in chunks with proper error handling
            queries = [
                (base_stock_data_query1, "First month of stock data"),
                (base_stock_data_query2, "Second month of stock data"),
                (base_stock_data_query3, "Third month of stock data"),
                (base_index_data_query, "Index data")
            ]
            
            max_timestamp = None
            
            for query_template, description in queries:
                try:
                    logging.info(f"Processing {description}...")
                    query = query_template.format(current_time=current_time_str)
                    process_data_in_chunks(query, dd_con)
                    
                    # Get max timestamp from this chunk
                    result = dd_con.execute("""
                        SELECT MAX(created_at) FROM public.stock_prices
                    """).fetchone()[0]
                    
                    if result and (max_timestamp is None or result > max_timestamp):
                        max_timestamp = result
                        
                except Exception as e:
                    logging.error(f"Error processing {description}: {str(e)}")
                    # Continue with next query instead of failing completely
                    continue
            
            if max_timestamp:
                try:
                    # Update the timestamp in DuckDB
                    dd_con.execute("""
                        INSERT INTO public.synced_tables (table_name, updated_at, full_refresh_required)
                        VALUES (?, ?, ?)
                        ON CONFLICT (table_name) 
                        DO UPDATE SET updated_at = EXCLUDED.updated_at
                    """, ['stock_prices', max_timestamp, False])
                    
                    logging.info(f"Successfully committed full data load")
                except Exception as e:
                    logging.error(f"Error updating timestamp: {str(e)}")
                    raise
            else:
                logging.info(f"No data found for the specified period")
                
        else:
            # Get the last update timestamp from DuckDB
            last_updated = get_last_sync_timestamp(dd_con, "stock_prices")
            
            if not last_updated:
                # If no timestamp, trigger a full reload
                logging.info("No timestamp found, performing full reload")
                return handle_hourly_stock_data(dd_con, pg_connection, force=True)
            
            # Format the timestamp for ClickHouse
            last_updated_str = last_updated.strftime("%Y-%m-%d %H:%M:%S")
            
            # Process incremental updates in chunks
            queries = [
                (incremental_stock_data_query, "Incremental stock data"),
                (incremental_index_data_query, "Incremental index data")
            ]
            
            max_timestamp = last_updated
            
            for query_template, description in queries:
                try:
                    logging.info(f"Processing {description}...")
                    query = query_template.format(last_updated=last_updated_str)
                    process_data_in_chunks(query, dd_con)
                    
                    # Get max timestamp from this chunk
                    result = dd_con.execute("""
                        SELECT MAX(created_at) FROM public.stock_prices
                    """).fetchone()[0]
                    
                    if result and result > max_timestamp:
                        max_timestamp = result
                        
                except Exception as e:
                    logging.error(f"Error processing {description}: {str(e)}")
                    # Continue with next query instead of failing completely
                    continue
            
            if max_timestamp and max_timestamp > last_updated:
                try:
                    # Update the timestamp in DuckDB
                    dd_con.execute("""
                        INSERT INTO public.synced_tables (table_name, updated_at, full_refresh_required)
                        VALUES (?, ?, ?)
                        ON CONFLICT (table_name) 
                        DO UPDATE SET updated_at = EXCLUDED.updated_at
                    """, ['stock_prices', max_timestamp, False])
                    
                    logging.info(f"Successfully updated data with new records")
                except Exception as e:
                    logging.error(f"Error updating timestamp: {str(e)}")
                    raise
            else:
                logging.info("No new data found")
                
                # Update the timestamp to current time in Indian timezone
                current_time = datetime.now(INDIAN_TZ)
                update_sync_timestamp(dd_con, "stock_prices", current_time)
                
        return True
    except Exception as e:
        logging.error(f"Error handling hourly stock and index data: {str(e)}")
        return False

def process_query_result(result, columns):
    """Process query result with proper type handling for decimal values"""
    try:
        if not result:
            return None
            
        # Convert to DataFrame
        df = pl.DataFrame(result, schema=columns)
        
        # Convert decimal columns to float
        for col in df.columns:
            if df[col].dtype == pl.Decimal:
                df = df.with_columns(pl.col(col).cast(pl.Float64))
                
        return df
    except Exception as e:
        logger.error(f"Error processing query result: {str(e)}")
        return None

def load_hourly_data(force=False):
    """Load hourly stock data into DuckDB with proper chunking and memory management"""
    try:
        with get_duckdb_connection() as conn:
            if not conn:
                logging.error("Failed to establish DuckDB connection")
                return False

            # Ensure the 'public' schema exists
            conn.execute("CREATE SCHEMA IF NOT EXISTS public;")

            # Create market_metadata table if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS public.market_metadata (
                    security_code VARCHAR,
                    company_name VARCHAR,
                    symbol VARCHAR
                )
            """)

            # Create hourly_stock_data table if it doesn't exist
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

            # Load market metadata from ClickHouse
            metadata_query = """
            SELECT 
                security_code,
                company_name,
                symbol
            FROM strike.mv_stocks
            UNION ALL
            SELECT 
                security_code,
                company_name,
                symbol
            FROM strike.mv_indices
            """
            
            try:
                metadata_result = ClickHousePool.execute_query(metadata_query)
                if metadata_result:
                    metadata_df = pl.DataFrame(metadata_result)
                    metadata_arrow = metadata_df.to_arrow()
                    conn.execute("""
                        INSERT INTO public.market_metadata 
                        SELECT * FROM metadata_arrow
                        WHERE NOT EXISTS (
                            SELECT 1 FROM public.market_metadata m
                            WHERE m.security_code = metadata_arrow.security_code
                        )
                    """)
                    logging.info("Successfully loaded market metadata")
            except Exception as e:
                logging.error(f"Error loading market metadata: {str(e)}")
                # Continue with hourly data load even if metadata fails

            # Process data in chunks
            offset = 0
            total_records = 0
            
            while True:
                # Query for chunk of data
                query = f"""
                SELECT 
                    timestamp,
                    symbol,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    security_code,
                    ticker
                FROM strike.hourly_stock_data
                ORDER BY timestamp
                LIMIT {CHUNK_SIZE}
                OFFSET {offset}
                """
                
                try:
                    result = ClickHousePool.execute_query(query)
                    if not result:
                        break
                        
                    # Process the chunk
                    df = process_query_result(result, [
                        "timestamp", "symbol", "open_price", "high_price",
                        "low_price", "close_price", "volume", "security_code", "ticker"
                    ])
                    
                    if df is not None and not df.is_empty():
                        # Convert to Arrow for DuckDB
                        df_arrow = df.to_arrow()
                        
                        # Insert into DuckDB
                        conn.execute("""
                            INSERT INTO public.hourly_stock_data
                            SELECT * FROM df_arrow
                        """)
                        
                        # Update counters
                        chunk_size = len(df)
                        total_records += chunk_size
                        offset += chunk_size
                        
                        logging.info(f"Processed chunk of {chunk_size} records (Total: {total_records})")
                        
                        # Check if we've reached the maximum record limit
                        if total_records >= MAX_RECORDS:
                            logging.info(f"Reached maximum record limit of {MAX_RECORDS}")
                            break
                    else:
                        break
                        
                except Exception as e:
                    logging.error(f"Error processing chunk at offset {offset}: {str(e)}")
                    break
            
            logging.info(f"Successfully loaded all hourly data into DuckDB")
            return True
            
    except Exception as e:
        logging.error(f"Error loading hourly data: {str(e)}")
        return False

def test_queries():
    """Test all queries to ensure they work correctly"""
    try:
        current_time = datetime.now(timezone(timedelta(hours=5, minutes=30)))
        current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Test stock queries
        queries = [
            (base_stock_data_query1, "First month stock data"),
            (base_stock_data_query2, "Second month stock data"),
            (base_stock_data_query3, "Third month stock data"),
            (base_index_data_query, "Index data")
        ]
        
        for query_template, description in queries:
            try:
                logging.info(f"Testing {description}...")
                query = query_template.format(current_time=current_time_str)
                result = ClickHousePool.execute_query(query)
                
                if not result:
                    logging.warning(f"No results for {description}")
                    continue
                
                # Print the entire first row and its types for debugging
                first_row = result[0] if result else None
                if first_row:
                    logging.info(f"First row: {first_row}")
                    logging.info(f"Types: {[type(x) for x in first_row]}")
                else:
                    logging.warning(f"Result is not empty but first row is None for {description}")
                
                # Validate result structure
                is_valid, validation_msg = validate_query_result(result, [
                    "current_price", "created_at", "security_code", 
                    "ticker", "symbol", "previous_close"
                ])
                
                if is_valid:
                    logging.info(f"✓ {description} query is valid")
                    logging.info(f"Sample data: {result[0]}")
                else:
                    logging.error(f"✗ {description} query validation failed: {validation_msg}")
                    
            except Exception as e:
                logging.error(f"Error testing {description}: {str(e)}")
                continue
                
        # Test incremental queries
        last_updated = datetime.now(timezone(timedelta(hours=5, minutes=30))) - timedelta(days=1)
        last_updated_str = last_updated.strftime("%Y-%m-%d %H:%M:%S")
        
        incremental_queries = [
            (incremental_stock_data_query, "Incremental stock data"),
            (incremental_index_data_query, "Incremental index data")
        ]
        
        for query_template, description in incremental_queries:
            try:
                logging.info(f"Testing {description}...")
                query = query_template.format(last_updated=last_updated_str)
                result = ClickHousePool.execute_query(query)
                
                if not result:
                    logging.warning(f"No results for {description}")
                    continue
                
                # Print the entire first row and its types for debugging
                first_row = result[0] if result else None
                if first_row:
                    logging.info(f"First row: {first_row}")
                    logging.info(f"Types: {[type(x) for x in first_row]}")
                else:
                    logging.warning(f"Result is not empty but first row is None for {description}")
                
                # Validate result structure
                is_valid, validation_msg = validate_query_result(result, [
                    "current_price", "created_at", "security_code", 
                    "ticker", "symbol", "previous_close"
                ])
                
                if is_valid:
                    logging.info(f"✓ {description} query is valid")
                    logging.info(f"Sample data: {result[0]}")
                else:
                    logging.error(f"✗ {description} query validation failed: {validation_msg}")
                    
            except Exception as e:
                logging.error(f"Error testing {description}: {str(e)}")
                continue
                
        return True
        
    except Exception as e:
        logging.error(f"Error in test_queries: {str(e)}")
        return False
