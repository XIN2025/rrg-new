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
import pyarrow as pa

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
    start_time = time.time()
    total_processed = 0
    
    # Match the actual ClickHouse result columns
    expected_columns = [
        "ticker", "created_at", "current_price", "previous_close",
        "security_code", "high_price", "low_price", "volume", "turnover"
    ]
    
    try:
        # Validate query before execution
        if not query.strip() or "SELECT" not in query.upper():
            raise ValueError("Invalid query format")
            
        logger.info("Executing ClickHouse query...")
        result = ClickHousePool.execute_query(query)
        
        if not result:
            logger.warning("No data returned from query")
            return
            
        total_records = len(result)
        logger.info(f"Retrieved {total_records:,} records from ClickHouse")
        
        # Convert to DataFrame first
        logger.info("Converting to DataFrame...")
        df = pd.DataFrame(result.result_rows, columns=result.column_names)
        
        # Enhanced data filtering
        logger.info("Filtering invalid data...")
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
            
        logger.info(f"Valid records after filtering: {len(df):,}")
        
        # Process in chunks
        chunk_size = 100000
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            chunk_num = i // chunk_size + 1
            total_chunks = (len(df) + chunk_size - 1) // chunk_size
            
            logger.info(f"Processing chunk {chunk_num}/{total_chunks}")
            logger.info(f"Progress: {total_processed}/{len(df)} records ({(total_processed/len(df)*100):.1f}%)")
            
            # Convert to Arrow and load into DuckDB
            logger.info(f"Converting chunk {chunk_num} to Arrow format...")
            df_arrow = chunk.to_arrow()
            
            # Begin transaction
            logger.info(f"Inserting chunk {chunk_num} into DuckDB...")
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
                total_processed += inserted_count
                
                # Calculate and log progress metrics
                elapsed_time = time.time() - start_time
                avg_speed = total_processed / elapsed_time if elapsed_time > 0 else 0
                estimated_remaining = (len(df) - total_processed) / avg_speed if avg_speed > 0 else 0
                
                logger.info(f"Chunk {chunk_num} inserted: {inserted_count} records")
                logger.info(f"Total processed: {total_processed}/{len(df)} records ({(total_processed/len(df)*100):.1f}%)")
                logger.info(f"Average speed: {int(avg_speed)} records/sec")
                logger.info(f"Estimated time remaining: {int(estimated_remaining)} seconds")
                
                # Log memory usage
                process = psutil.Process()
                memory_info = process.memory_info()
                logger.info(f"Memory usage: {memory_info.rss / 1024 / 1024:.1f} MB")
                
                # Commit transaction
                dd_con.execute("COMMIT")
                logger.info(f"Chunk {chunk_num} committed successfully")
                
            except Exception as e:
                # Rollback on error
                dd_con.execute("ROLLBACK")
                raise e
                
            finally:
                # Clear memory
                del chunk
                del df_arrow
                import gc
                gc.collect()
                
        logger.info("Data processing completed successfully")
        logger.info(f"Total records processed: {total_processed}")
        logger.info(f"Total time: {time.time() - start_time:.2f} seconds")
            
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

def ensure_hourly_stock_data_table(dd_con):
    dd_con.execute("""
        CREATE TABLE IF NOT EXISTS hourly_stock_data (
            ticker VARCHAR,
            timestamp TIMESTAMP,
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            close_price DOUBLE,
            volume BIGINT,
            security_code VARCHAR,
            symbol VARCHAR
        )
    """)

def process_batch(df):
    """Process a batch of data from ClickHouse into DuckDB."""
    try:
        if len(df) == 0:
            logger.warning("Empty DataFrame received")
            return 0

        # Ensure the data directory exists
        os.makedirs('data', exist_ok=True)

        logger.info("Connecting to DuckDB...")
        dd_con = duckdb.connect('data/pydb.duckdb')
        logger.info("Connected to DuckDB successfully")

        # Ensure table exists
        ensure_hourly_stock_data_table(dd_con)

        # Process in chunks of 100,000 rows
        chunk_size = 100000
        total_inserted = 0
        
        for i in range(0, len(df), chunk_size):
            chunk = df.slice(i, chunk_size)
            logger.info(f"Processing chunk {i//chunk_size + 1} of {(len(df) + chunk_size - 1)//chunk_size}")
            
            # Convert to pandas DataFrame for easier manipulation
            chunk_pd = chunk.to_pandas()
            
            # Ensure timestamp is in the correct format
            chunk_pd['timestamp'] = pd.to_datetime(chunk_pd['timestamp'])
            
            # Convert numeric columns
            numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
            for col in numeric_cols:
                chunk_pd[col] = pd.to_numeric(chunk_pd[col], errors='coerce')
            
            # Fill NaN values in volume with 0
            chunk_pd['volume'] = chunk_pd['volume'].fillna(0).astype('int64')
            
            # Drop any rows with NaN values in critical columns
            critical_cols = ['ticker', 'timestamp', 'open_price', 'high_price', 'low_price', 'close_price']
            chunk_pd = chunk_pd.dropna(subset=critical_cols)
            
            if len(chunk_pd) == 0:
                logger.warning("No valid data in chunk after cleaning")
                continue

            logger.info("Converting chunk to Arrow format...")
            df_arrow = pa.Table.from_pandas(chunk_pd)
            logger.info("Converted to Arrow format successfully")

            # Insert into hourly_stock_data using named columns
            logger.info("Inserting data into DuckDB...")
            insert_result = dd_con.execute("""
                INSERT INTO hourly_stock_data (
                    ticker, timestamp, open_price, high_price, low_price, close_price, volume, security_code, symbol
                )
                SELECT 
                    ticker, timestamp, open_price, high_price, low_price, close_price, volume, security_code, symbol
                FROM df_arrow
            """)

            inserted_count = insert_result.fetchone()[0]
            total_inserted += inserted_count
            logger.info(f"Inserted chunk of {inserted_count} records (total: {total_inserted})")

        logger.info(f"Successfully inserted {total_inserted} records into hourly_stock_data")
        return total_inserted

    except Exception as e:
        logger.error(f"Error processing batch: {str(e)}")
        return 0

def load_hourly_data(days=90):
    """Load hourly data from ClickHouse into DuckDB."""
    try:
        logger.info("Initializing ClickHouse connection...")
        pool = ClickHousePool
        logger.info("ClickHouse connection initialized successfully")

        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        logger.info(f"Loading data from {start_date} to {end_date}")

        # Query to get 5-minute interval data with proper table references
        query = f"""
        WITH active_stocks AS (
            SELECT security_token, ticker, security_code, symbol
            FROM mv_stocks
        )
        SELECT 
            ast.ticker,
            ep.date_time as timestamp,
            CAST(ep.open AS Float64) as open_price,
            CAST(ep.high AS Float64) as high_price,
            CAST(ep.low AS Float64) as low_price,
            CAST(ep.close AS Float64) as close_price,
            CAST(ep.volume AS Int64) as volume,
            ast.security_code,
            ast.symbol
        FROM equity_prices_1min AS ep
        INNER JOIN active_stocks AS ast ON ep.security_token = ast.security_token
        WHERE ep.date_time >= '{start_date.strftime('%Y-%m-%d')}'
        AND ep.date_time <= '{end_date.strftime('%Y-%m-%d')}'
        AND toMinute(ep.date_time) % 5 = 0
        AND ep.close != 0
        ORDER BY ast.ticker, ep.date_time
        """

        logger.info("Executing ClickHouse query...")
        result = pool.execute_query(query)
        if not result:
            logger.error("No data returned from ClickHouse")
            return

        logger.info(f"Retrieved {len(result)} rows from ClickHouse")
        
        # Process in batches
        batch_size = 1000000
        total_processed = 0
        
        for i in range(0, len(result), batch_size):
            batch = result[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} of {(len(result) + batch_size - 1)//batch_size}")
            
            # Convert batch to DataFrame with explicit column names
            df = pl.DataFrame(batch, schema={
                'ticker': pl.Utf8,
                'timestamp': pl.Datetime,
                'open_price': pl.Float64,
                'high_price': pl.Float64,
                'low_price': pl.Float64,
                'close_price': pl.Float64,
                'volume': pl.Int64,
                'security_code': pl.Utf8,
                'symbol': pl.Utf8
            })
            logger.info(f"Batch DataFrame shape: {df.shape}")
            
            # Process the batch
            inserted = process_batch(df)
            total_processed += inserted
            logger.info(f"Processed {total_processed} records so far")

        logger.info(f"Data loading completed. Total records processed: {total_processed}")

    except Exception as e:
        logger.error(f"Error loading hourly data: {str(e)}")
        raise

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

def load_minute_data(self, start_date: str = None, end_date: str = None) -> None:
    if start_date is None:
        start_date = (datetime.now(INDIAN_TZ) - timedelta(days=90)).strftime('%Y-%m-%d')
    if end_date is None:
        end_date = datetime.now(INDIAN_TZ).strftime('%Y-%m-%d')
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

def test_data_loading():
    """Test the data loading process with a small sample."""
    try:
        logger.info("Starting data loading test...")
        
        # Test with a small date range (1 day)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        # Test query with proper table references
        test_query = f"""
        WITH active_stocks AS (
            SELECT security_token, ticker, security_code, symbol
            FROM mv_stocks
            LIMIT 10  -- Limit to 10 stocks for testing
        )
        SELECT 
            ast.ticker,
            ep.date_time as timestamp,
            CAST(ep.open AS Float64) as open_price,
            CAST(ep.high AS Float64) as high_price,
            CAST(ep.low AS Float64) as low_price,
            CAST(ep.close AS Float64) as close_price,
            CAST(ep.volume AS Int64) as volume,
            ast.security_code,
            ast.symbol
        FROM equity_prices_1min AS ep
        INNER JOIN active_stocks AS ast ON ep.security_token = ast.security_token
        WHERE ep.date_time >= '{start_date.strftime('%Y-%m-%d')}'
        AND ep.date_time <= '{end_date.strftime('%Y-%m-%d')}'
        AND toMinute(ep.date_time) % 5 = 0
        AND ep.close != 0
        ORDER BY ast.ticker, ep.date_time
        LIMIT 1000  -- Limit to 1000 records for testing
        """
        
        logger.info("Executing test query...")
        result = ClickHousePool.execute_query(test_query)
        
        if not result:
            logger.error("No data returned from test query")
            return False
            
        logger.info(f"Retrieved {len(result)} test records")
        
        # Convert to DataFrame with explicit schema
        df = pl.DataFrame(result, schema={
            'ticker': pl.Utf8,
            'timestamp': pl.Datetime,
            'open_price': pl.Float64,
            'high_price': pl.Float64,
            'low_price': pl.Float64,
            'close_price': pl.Float64,
            'volume': pl.Int64,
            'security_code': pl.Utf8,
            'symbol': pl.Utf8
        })
        
        # Verify DataFrame structure
        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"DataFrame columns: {df.columns}")
        logger.info(f"DataFrame dtypes: {df.dtypes}")
        
        # Test data processing
        inserted = process_batch(df)
        logger.info(f"Test batch processing result: {inserted} records inserted")
        
        # Verify data in DuckDB
        dd_con = duckdb.connect('data/pydb.duckdb')
        ensure_hourly_stock_data_table(dd_con)
        verify_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT ticker) as unique_tickers,
            MIN(timestamp) as earliest_timestamp,
            MAX(timestamp) as latest_timestamp
        FROM hourly_stock_data
        """
        
        verify_result = dd_con.execute(verify_query).fetchone()
        logger.info(f"Verification results: {verify_result}")
        
        # Clean up test data
        dd_con.execute("DELETE FROM hourly_stock_data WHERE timestamp >= ?", [start_date])
        logger.info("Test data cleaned up")
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        return False

if __name__ == "__main__":
    # Run the test
    test_success = test_data_loading()
    if test_success:
        logger.info("Test completed successfully!")
    else:
        logger.error("Test failed!")
