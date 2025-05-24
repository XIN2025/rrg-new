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
from src.utils.duck_pool import get_duckdb_connection, DuckDBPool
from src.utils.clickhouse_pool import pool
from src.utils.metrics import TimerMetric, DuckDBQueryTimer
from src.utils.logger import get_logger
from src.modules.db.config import MAX_RECORDS
import os
import time
import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.utils.clickhouse import get_clickhouse_connection
import pandas as pd

logger = get_logger(__name__)

# Define timezone
INDIAN_TZ = pytz.timezone('Asia/Kolkata')

# Define columns for stock_prices table
stock_prices_columns = ["current_price", "created_at", "security_code", "ticker", "symbol", "previous_close"]

# Base ClickHouse query for stock data( loads 0 to 2 months data)
base_stock_data_query1 = """
WITH active_stocks AS (
    SELECT security_token
    FROM mv_stocks
)
SELECT 
    ms.ticker AS ticker,
    ep.date_time as created_at,
    CAST(ep.close AS Float64) as current_price,
    lagInFrame(CAST(ep.close AS Float64)) OVER (PARTITION BY ep.security_token ORDER BY ep.date_time) AS previous_close,
    ms.security_code AS security_code,
    CAST(ep.high AS Float64) as high_price,
    CAST(ep.low AS Float64) as low_price,
    CAST(ep.volume AS Float64) as volume,
    NULL as turnover
FROM 
    equity_prices_1min AS ep
INNER JOIN active_stocks AS ast
    ON ep.security_token = ast.security_token
INNER JOIN (
    SELECT 
        security_token,
        any(security_code) AS security_code,
        any(ticker) AS ticker
    FROM mv_stocks
    GROUP BY security_token
) AS ms 
    ON ep.security_token = ms.security_token
WHERE 
    ep.date_time > '{current_time}' - INTERVAL '1 months' - INTERVAL '15 days'
    AND toMinute(ep.date_time) % 5 = 0
    AND ep.close != 0
ORDER BY ep.date_time DESC
"""

# Base ClickHouse query for stock data( loads 2 months to 4 months data)
base_stock_data_query2 = """
WITH active_stocks AS (
    SELECT security_token
    FROM mv_stocks
)
SELECT 
    ms.ticker AS ticker,
    ep.date_time as created_at,
    CAST(ep.close AS Float64) as current_price,
    lagInFrame(CAST(ep.close AS Float64)) OVER (PARTITION BY ep.security_token ORDER BY ep.date_time) AS previous_close,
    ms.security_code AS security_code,
    CAST(ep.high AS Float64) as high_price,
    CAST(ep.low AS Float64) as low_price,
    CAST(ep.volume AS Float64) as volume,
    NULL as turnover
FROM 
    equity_prices_1min AS ep
INNER JOIN active_stocks AS ast
    ON ep.security_token = ast.security_token
INNER JOIN (
    SELECT 
        security_token,
        any(security_code) AS security_code,
        any(ticker) AS ticker
    FROM mv_stocks
    GROUP BY security_token
) AS ms 
    ON ep.security_token = ms.security_token
WHERE 
    ep.date_time > '{current_time}' - INTERVAL '3 months'
    AND ep.date_time <= '{current_time}' - INTERVAL '1 months' - INTERVAL '15 days'
    AND toMinute(ep.date_time) % 5 = 0
    AND ep.close != 0
ORDER BY ep.date_time DESC
"""

base_stock_data_query3 = """
WITH active_stocks AS (
    SELECT security_token
    FROM mv_stocks
)
SELECT 
    ms.ticker AS ticker,
    ep.date_time as created_at,
    CAST(ep.close AS Float64) as current_price,
    lagInFrame(CAST(ep.close AS Float64)) OVER (PARTITION BY ep.security_token ORDER BY ep.date_time) AS previous_close,
    ms.security_code AS security_code,
    CAST(ep.high AS Float64) as high_price,
    CAST(ep.low AS Float64) as low_price,
    CAST(ep.volume AS Float64) as volume,
    NULL as turnover
FROM 
    equity_prices_1min AS ep
INNER JOIN active_stocks AS ast
    ON ep.security_token = ast.security_token
INNER JOIN (
    SELECT 
        security_token,
        any(security_code) AS security_code,
        any(ticker) AS ticker
    FROM mv_stocks
    GROUP BY security_token
) AS ms 
    ON ep.security_token = ms.security_token
WHERE 
    ep.date_time > '{current_time}' - INTERVAL '4 months' - INTERVAL '10 days'
    AND ep.date_time <= '{current_time}' - INTERVAL '3 months'
    AND toMinute(ep.date_time) % 5 = 0
    AND ep.close != 0
ORDER BY ep.date_time DESC
"""

# Incremental query for updates after initial load
incremental_stock_data_query = """
WITH active_stocks AS (
    SELECT security_token
    FROM mv_stocks
)
SELECT 
    ms.ticker AS ticker,
    ep.date_time as created_at,
    CAST(ep.close AS Float64) as current_price,
    lagInFrame(CAST(ep.close AS Float64)) OVER (PARTITION BY ep.security_token ORDER BY ep.date_time) AS previous_close,
    ms.security_code AS security_code,
    CAST(ep.high AS Float64) as high_price,
    CAST(ep.low AS Float64) as low_price,
    CAST(ep.volume AS Float64) as volume,
    NULL as turnover
FROM 
    equity_prices_1min AS ep
INNER JOIN active_stocks AS ast
    ON ep.security_token = ast.security_token
INNER JOIN (
    SELECT 
        security_token,
        any(security_code) AS security_code,
        any(ticker) AS ticker
    FROM mv_stocks
    GROUP BY security_token
) AS ms 
    ON ep.security_token = ms.security_token
WHERE 
    ep.date_time >= '{last_updated}'
    AND ep.close != 0
    AND toMinute(ep.date_time) % 5 = 0
ORDER BY ep.date_time DESC
"""

# Base ClickHouse query for index data
base_index_data_query = """
WITH active_indices AS (
    SELECT security_token
    FROM mv_indices
)
SELECT 
    mi.ticker AS ticker,
    ip.date_time as created_at,
    CAST(ip.close AS Float64) as current_price,
    lagInFrame(CAST(ip.close AS Float64)) OVER (PARTITION BY ip.security_token ORDER BY ip.date_time) AS previous_close,
    mi.security_code AS security_code,
    CAST(ip.high AS Float64) as high_price,
    CAST(ip.low AS Float64) as low_price,
    NULL as volume,
    NULL as turnover
FROM 
    index_prices_1min AS ip
INNER JOIN active_indices AS ai
    ON ip.security_token = ai.security_token
INNER JOIN (
    SELECT 
        security_token,
        any(security_code) AS security_code,
        any(ticker) AS ticker
    FROM mv_indices
    GROUP BY security_token
) AS mi
    ON ip.security_token = mi.security_token
WHERE 
    ip.date_time > '{current_time}' - interval '4 months' - interval '10 days'
    AND toMinute(ip.date_time) % 5 = 0
    AND ip.close != 0
ORDER BY 
    ip.date_time DESC
"""

# Incremental query for index data updates
incremental_index_data_query = """
WITH active_indices AS (
    SELECT security_token
    FROM mv_indices
)
SELECT 
    mi.ticker AS ticker,
    ip.date_time as created_at,
    CAST(ip.close AS Float64) as current_price,
    lagInFrame(CAST(ip.close AS Float64)) OVER (PARTITION BY ip.security_token ORDER BY ip.date_time) AS previous_close,
    mi.security_code AS security_code,
    CAST(ip.high AS Float64) as high_price,
    CAST(ip.low AS Float64) as low_price,
    NULL as volume,
    NULL as turnover
FROM 
    index_prices_1min AS ip
INNER JOIN active_indices AS ai
    ON ip.security_token = ai.security_token
INNER JOIN (
    SELECT 
        security_token,
        any(security_code) AS security_code,
        any(ticker) AS ticker
    FROM mv_indices
    GROUP BY security_token
) AS mi
    ON ip.security_token = mi.security_token
WHERE 
    ip.date_time >= '{last_updated}'
    AND toMinute(ip.date_time) % 5 = 0
    AND ip.close != 0
ORDER BY 
    ip.date_time DESC
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
    logger.info(f"Inserted {inserted_count} records")

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
    """Validate query result structure and data with enhanced checks"""
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
        
    # Enhanced data validation
    for i, (value, col_name) in enumerate(zip(values, expected_columns)):
        # Allow NULL for turnover and volume (for index data)
        if value is None and col_name not in ["turnover", "volume"]:
            return False, f"Null value in column {col_name}"
            
        if col_name in ["current_price", "previous_close", "high_price", "low_price"]:
            # Enhanced price validation
            if not (isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit())):
                return False, f"Invalid price value in {col_name}: {value} ({type(value)})"
            # Check for negative prices
            if float(value) < 0:
                return False, f"Negative price value in {col_name}: {value}"
        elif col_name == "created_at":
            # Enhanced timestamp validation
            if not (isinstance(value, (str, datetime))):
                return False, f"Invalid date value in {col_name}: {value} ({type(value)})"
            # Check for future dates
            if isinstance(value, str):
                try:
                    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                    if dt > datetime.now():
                        return False, f"Future date in {col_name}: {value}"
                except ValueError:
                    return False, f"Invalid date format in {col_name}: {value}"
        elif col_name in ["security_code", "ticker"]:
            # Enhanced string validation
            if not isinstance(value, str) or not value.strip():
                return False, f"Invalid or empty string value in {col_name}: {value}"
        elif col_name == "volume":
            # Enhanced volume validation
            if value is not None:
                if not (isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit())):
                    return False, f"Invalid volume value in {col_name}: {value}"
                if float(value) < 0:
                    return False, f"Negative volume value in {col_name}: {value}"
        elif col_name == "turnover":
            # Enhanced turnover validation
            if value is not None:
                if not (isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit())):
                    return False, f"Invalid turnover value in {col_name}: {value}"
                if float(value) < 0:
                    return False, f"Negative turnover value in {col_name}: {value}"
                
    return True, "Valid result"

def process_data_in_chunks(query, dd_con, max_retries=3):
    """Process all data at once with enhanced error handling and validation"""
    retry_count = 0
    
    # Match the actual ClickHouse result columns
    expected_columns = [
        "ticker", "created_at", "current_price", "previous_close",
        "security_code", "high_price", "low_price", "volume", "turnover"
    ]
    
    try:
        # Validate query before execution
        if not query.strip() or "SELECT" not in query.upper():
            raise ValueError("Invalid query format")
            
        result = ClickHousePool.execute_query(query)
        
        if not result:
            logger.warning("No data returned from query")
            return
            
        # Convert to DataFrame first
        df = pd.DataFrame(result.result_rows, columns=result.column_names)
        
        # Enhanced data filtering
        df = df.loc[
            (df["current_price"].notna()) &
            (df["previous_close"].notna()) &
            (df["high_price"].notna()) &
            (df["low_price"].notna()) &
            (df["current_price"] > 0) &
            (df["previous_close"] > 0) &
            (df["high_price"] > 0) &
            (df["low_price"] > 0) &
            (df["high_price"] >= df["low_price"]) &
            (df["created_at"].notna())
        ]
        
        if len(df) == 0:
            logger.warning("No valid data after filtering")
            return
            
        # Convert to Arrow and load into DuckDB
        df_arrow = df.to_arrow()
        
        # Begin transaction
        dd_con.execute("BEGIN TRANSACTION")
        try:
            # Use a CTE for df_arrow with enhanced validation
            insert_result = dd_con.execute("""
                WITH df_arrow AS (
                    SELECT 
                        ticker,
                        created_at,
                        current_price,
                        previous_close,
                        security_code,
                        high_price,
                        low_price,
                        volume,
                        turnover
                    FROM df_arrow
                    WHERE 
                        current_price > 0 AND
                        previous_close > 0 AND
                        high_price > 0 AND
                        low_price > 0 AND
                        high_price >= low_price AND
                        created_at IS NOT NULL
                )
                INSERT INTO public.stock_prices (
                    ticker, created_at, current_price, previous_close, security_code, high_price, low_price, volume, turnover
                )
                SELECT 
                    ticker, created_at, current_price, previous_close, security_code, high_price, low_price, volume, turnover
                FROM df_arrow
                WHERE NOT EXISTS (
                    SELECT 1 FROM public.stock_prices s
                    WHERE s.created_at = df_arrow.created_at
                    AND s.ticker = df_arrow.ticker
                )
            """)
            
            inserted_count = insert_result.fetchone()[0]
            logger.info(f"Inserted {inserted_count} records")
            
            # Verify inserted data
            verify_result = dd_con.execute("""
                SELECT COUNT(*) 
                FROM public.stock_prices 
                WHERE created_at >= (
                    SELECT MIN(created_at) 
                    FROM df_arrow
                )
            """).fetchone()[0]
            
            if verify_result != inserted_count:
                raise ValueError(f"Data verification failed. Expected {inserted_count} records, got {verify_result}")
            
            # Commit transaction
            dd_con.execute("COMMIT")
            
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
            
    except Exception as e:
        retry_count += 1
        logger.error(f"Error processing data (attempt {retry_count}/{max_retries}): {str(e)}")
        
        if retry_count >= max_retries:
            logger.error("Max retries reached, stopping processing")
            raise
            
        # Wait before retrying
        time.sleep(1)
        return process_data_in_chunks(query, dd_con, max_retries)

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
        
        if not table_exists or force:
            # If table doesn't exist or force refresh, load all data in chunks
            logger.info("Stock prices table doesn't exist or force refresh requested, loading full data")
            
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
            
            logger.info("Created table and indexes")
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
                    logger.info(f"Processing {description}...")
                    query = query_template.format(current_time=current_time_str)
                    process_data_in_chunks(query, dd_con)
                    
                    # Get max timestamp from this chunk
                    result = dd_con.execute("""
                        SELECT MAX(created_at) FROM public.stock_prices
                    """).fetchone()[0]
                    
                    if result and (max_timestamp is None or result > max_timestamp):
                        max_timestamp = result
                        
                except Exception as e:
                    logger.error(f"Error processing {description}: {str(e)}")
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
                    
                    logger.info(f"Successfully committed full data load")
                except Exception as e:
                    logger.error(f"Error updating timestamp: {str(e)}")
                    raise
            else:
                logger.info(f"No data found for the specified period")
                
        else:
            # Get the last update timestamp from DuckDB
            last_updated = get_last_sync_timestamp(dd_con, "stock_prices")
            
            if not last_updated:
                # If no timestamp, trigger a full reload
                logger.info("No timestamp found, performing full reload")
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
                    logger.info(f"Processing {description}...")
                    query = query_template.format(last_updated=last_updated_str)
                    process_data_in_chunks(query, dd_con)
                    
                    # Get max timestamp from this chunk
                    result = dd_con.execute("""
                        SELECT MAX(created_at) FROM public.stock_prices
                    """).fetchone()[0]
                    
                    if result and result > max_timestamp:
                        max_timestamp = result
                        
                except Exception as e:
                    logger.error(f"Error processing {description}: {str(e)}")
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
                    
                    logger.info(f"Successfully updated data with new records")
                except Exception as e:
                    logger.error(f"Error updating timestamp: {str(e)}")
                    raise
            else:
                logger.info("No new data found")
                
                # Update the timestamp to current time in Indian timezone
                current_time = datetime.now(INDIAN_TZ)
                update_sync_timestamp(dd_con, "stock_prices", current_time)
                
        return True
    except Exception as e:
        logger.error(f"Error handling hourly stock and index data: {str(e)}")
        return False

def process_query_result(result, columns):
    """Process query result with proper type handling for decimal values"""
    try:
        if not result:
            return None
            
        # Convert to DataFrame
        df = pd.DataFrame(result.result_rows, columns=result.column_names)
        
        # Convert decimal columns to float
        for col in df.columns:
            if df[col].dtype == pl.Decimal:
                df = df.with_columns(pl.col(col).cast(pl.Float64))
                
        return df
    except Exception as e:
        logger.error(f"Error processing query result: {str(e)}")
        return None

def process_batch(query, dd_con):
    """Process a batch of data from ClickHouse into DuckDB."""
    try:
        # Execute the query
        result = pool.execute_query(query)
        if not result:
            logger.warning("No data returned from query")
            return 0

        # Convert to DataFrame
        df = pl.DataFrame(result)
        if len(df) == 0:
            logger.warning("Empty DataFrame after conversion")
            return 0

        # Convert to Arrow
        df_arrow = df.to_arrow()

        # Insert into public.hourly_stock_data using positional column references
        insert_result = dd_con.execute("""
            INSERT INTO public.hourly_stock_data (
                timestamp, symbol, open_price, high_price, low_price, close_price, volume, security_code, ticker
            )
            SELECT 
                column_1 AS timestamp,
                column_8 AS symbol,
                column_2 AS open_price,
                column_3 AS high_price,
                column_4 AS low_price,
                column_5 AS close_price,
                column_6 AS volume,
                column_7 AS security_code,
                column_0 AS ticker
            FROM df_arrow
        """)

        inserted_count = insert_result.fetchone()[0]
        logger.info(f"Inserted {inserted_count} records into public.hourly_stock_data")
        return inserted_count

    except Exception as e:
        logger.error(f"Error processing batch: {str(e)}")
        return 0

def load_hourly_data(days=7):
    """Load hourly stock data from ClickHouse into DuckDB."""
    try:
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        # Format dates as strings for the query
        start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
        
        # Define the query
        query = f"""
        WITH active_stocks AS (
            SELECT security_token
            FROM mv_stocks
        )
        SELECT
            s.ticker AS ticker,
            e.date_time AS timestamp,
            e.open AS open_price,
            e.high AS high_price,
            e.low AS low_price,
            e.close AS close_price,
            e.volume AS volume,
            s.security_code AS security_code,
            s.symbol AS symbol
        FROM strike.equity_prices_1min e
        JOIN strike.mv_stocks s ON e.security_token = s.security_token
        WHERE e.date_time BETWEEN '{start_date_str}' AND '{end_date_str}'
        AND e.security_token IN (SELECT security_token FROM active_stocks)
        AND e.close > 0
        AND toMinute(e.date_time) % 5 = 0
        AND e.volume > 0
        AND e.high >= e.low
        ORDER BY e.date_time, e.security_token
        """
        
        # Process the batch
        with get_duckdb_connection() as dd_con:
            if not dd_con:
                logger.error("Failed to establish DuckDB connection")
                return False
            inserted_count = process_batch(query, dd_con)
            if inserted_count > 0:
                logger.info(f"Successfully loaded {inserted_count} records into public.hourly_stock_data")
                return True
            else:
                logger.error("No records were inserted")
                return False
    except Exception as e:
        logger.error(f"Error in load_hourly_data: {str(e)}")
        return False

def test_queries():
    """Test all queries to ensure they work correctly"""
    try:
        current_time = datetime.now(timezone(timedelta(hours=5, minutes=30)))
        current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Define expected columns in correct order
        expected_columns = [
            "ticker", "created_at", "current_price", "previous_close",
            "security_code", "high_price", "low_price", "volume", "turnover"
        ]
        
        # Test stock queries with more detailed logging
        queries = [
            (base_stock_data_query1, "First month stock data"),
            (base_stock_data_query2, "Second month stock data"),
            (base_stock_data_query3, "Third month stock data"),
            (base_index_data_query, "Index data")
        ]
        
        successful_queries = 0
        total_records = 0
        
        for query_template, description in queries:
            try:
                logger.info(f"\n{'='*50}")
                logger.info(f"Testing {description}...")
                query = query_template.format(current_time=current_time_str)
                result = ClickHousePool.execute_query(query)
                
                if not result:
                    logger.warning(f"No results for {description}")
                    continue
                
                # Count records
                record_count = len(result)
                total_records += record_count
                logger.info(f"Found {record_count:,} records")
                
                # Print the entire first row and its types for debugging
                first_row = result[0] if result else None
                if first_row:
                    logger.info(f"First row: {first_row}")
                    logger.info(f"Types: {[type(x) for x in first_row]}")
                else:
                    logger.warning(f"Result is not empty but first row is None for {description}")
                
                # Validate result structure with all 9 columns
                is_valid, validation_msg = validate_query_result(result, expected_columns)
                
                if is_valid:
                    logger.info(f"✓ {description} query is valid")
                    logger.info(f"Sample data: {result[0]}")
                    successful_queries += 1
                else:
                    logger.error(f"✗ {description} query validation failed: {validation_msg}")
                    
            except Exception as e:
                logger.error(f"Error testing {description}: {str(e)}")
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
                logger.info(f"\n{'='*50}")
                logger.info(f"Testing {description}...")
                query = query_template.format(last_updated=last_updated_str)
                result = ClickHousePool.execute_query(query)
                
                if not result:
                    logger.warning(f"No results for {description}")
                    continue
                
                # Count records
                record_count = len(result)
                total_records += record_count
                logger.info(f"Found {record_count:,} records")
                
                # Print the entire first row and its types for debugging
                first_row = result[0] if result else None
                if first_row:
                    logger.info(f"First row: {first_row}")
                    logger.info(f"Types: {[type(x) for x in first_row]}")
                else:
                    logger.warning(f"Result is not empty but first row is None for {description}")
                
                # Validate result structure with all 9 columns
                is_valid, validation_msg = validate_query_result(result, expected_columns)
                
                if is_valid:
                    logger.info(f"✓ {description} query is valid")
                    logger.info(f"Sample data: {result[0]}")
                    successful_queries += 1
                else:
                    logger.error(f"✗ {description} query validation failed: {validation_msg}")
                    
            except Exception as e:
                logger.error(f"Error testing {description}: {str(e)}")
                continue
        
        # Summary of test results
        logger.info(f"\n{'='*50}")
        logger.info("Test Summary:")
        logger.info(f"Total queries tested: {len(queries) + len(incremental_queries)}")
        logger.info(f"Successful queries: {successful_queries}")
        logger.info(f"Total records found: {total_records:,}")
        
        # Return True only if at least 3 sections loaded successfully
        return successful_queries >= 3
        
    except Exception as e:
        logger.error(f"Error in test_queries: {str(e)}")
        return False

def load_minute_data(self, start_date: str, end_date: str) -> None:
    """Load minute data for the specified date range"""
    try:
        # Get total count of records
        count_query = f"""
            SELECT count(*) as total
            FROM {self.table_name}
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
        """
        result = pool.execute_query(count_query)
        total_records = result[0][0]
        
        if total_records == 0:
            logger.info(f"No minute data found for period {start_date} to {end_date}")
            return
            
        logger.info(f"Found {total_records:,} minute records to load")
        
        # Load all data at once
        query = f"""
            SELECT *
            FROM {self.table_name}
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
        """
        
        logger.info(f"Loading all {total_records:,} records at once")
        result = pool.execute_query(query)
        
        if not result:
            logger.warning("No data returned from query")
            return
            
        logger.info(f"Successfully loaded {len(result):,} minute records")
        
    except Exception as e:
        logger.error(f"Error loading minute data: {str(e)}")
        raise
