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

# Define timezone
INDIAN_TZ = pytz.timezone('Asia/Kolkata')

# Define columns for eod_stock_data table
eod_stock_data_columns = ["created_at", "symbol", "close_price", "security_code", "previous_close", "ticker"]

# Batch query template for EOD stock data loads
batch_eod_stock_query_template = """
SELECT 
    ep.date_time AS created_at,
    ms.symbol,
    ep.close AS close_price,
    ep.security_code,
    lagInFrame(ep.close) OVER (PARTITION BY ep.security_code ORDER BY ep.date_time) AS previous_close,
    ms.ticker
FROM strike.equity_prices_1d AS ep
INNER JOIN strike.mv_stocks AS ms 
    ON ep.security_code = ms.security_code
WHERE ep.date_time > '{start_date}' AND ep.date_time <= '{end_date}'
ORDER BY ep.date_time DESC
"""

incremental_index_data_query = """
SELECT 
    ip.date_time AS created_at,
    mi.symbol,
    ip.close AS close_price,
    mi.security_code,
    lagInFrame(ip.close) OVER (PARTITION BY ip.security_token ORDER BY ip.date_time) AS previous_close,
    mi.ticker,
    row_number() OVER (PARTITION BY mi.symbol ORDER BY ip.date_time DESC) AS rn
FROM 
    strike.index_prices_1min AS ip
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
    AND ip.close != 0
QUALIFY rn = 1
ORDER BY 
    ip.date_time DESC
"""

incremental_stock_data_query = """
SELECT 
    ep.date_time AS created_at,
    ms.symbol,
    ep.close AS close_price,
    ms.security_code,
    lagInFrame(ep.close) OVER (PARTITION BY ep.security_token ORDER BY ep.date_time) AS previous_close,
    ms.ticker,
    row_number() OVER (PARTITION BY ms.symbol ORDER BY ep.date_time DESC) AS rn
FROM 
    strike.equity_prices_1min AS ep
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
QUALIFY rn = 1
ORDER BY ep.date_time DESC
"""

def batch_load_eod_stock_data(dd_con, current_time, batch_years=2):
    """Load EOD stock data in 2-year batches"""
    
    total_records_count = 0
    max_timestamp = None
    
    # Calculate total years to load (12 years)
    total_years = 12
    num_batches = total_years // batch_years
    
    # Process in 2-year batches
    end_date = current_time
    
    for i in range(num_batches):
        # Calculate start date (end_date - 2 years)
        start_date = end_date - timedelta(days=batch_years*365)
            
        logging.info(f"Loading batch {i+1}/{num_batches}: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Create query for this batch
        batch_query = batch_eod_stock_query_template.format(
            start_date=start_date.strftime("%Y-%m-%d %H:%M:%S"),
            end_date=end_date.strftime("%Y-%m-%d %H:%M:%S")
        )
        
        # Execute query
        batch_result = ClickHousePool.execute_query(batch_query)
        
        # Process results
        batch_df = pl.DataFrame(batch_result, schema=eod_stock_data_columns, orient="row")
        
        # Insert data into DuckDB
        if len(batch_df) > 0:
            # Convert to arrow for DuckDB
            df_arrow = batch_df.to_arrow()
            
            # Insert the new records
            insert_result = dd_con.execute("""
                INSERT INTO public.eod_stock_data 
                SELECT * FROM df_arrow
            """)
            
            inserted_count = insert_result.fetchone()[0]
            total_records_count += inserted_count
            
            # Update max timestamp if needed
            batch_max = batch_df['created_at'].max()
            if max_timestamp is None or batch_max > max_timestamp:
                max_timestamp = batch_max
                
            logging.info(f"Batch {i+1}/{num_batches} completed: {inserted_count} records loaded")
        else:
            logging.info(f"Batch {i+1}/{num_batches} completed: No records found")
        
        # Update end date for next batch
        end_date = start_date
    
    return total_records_count, max_timestamp


def add_current_day_stock_data(dd_con):
    """Add current day's stock data"""
    try:
        # Get current time for the queries
        current_time = datetime.now(INDIAN_TZ)
        current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Execute both incremental queries
        stock_query = incremental_stock_data_query.format(last_updated=current_time_str)
        index_query = incremental_index_data_query.format(last_updated=current_time_str)
        
        # Get stock data
        stock_result = ClickHousePool.execute_query(stock_query)
        stock_df = pl.DataFrame(stock_result, schema=["created_at", "symbol", "close_price", "security_code", "previous_close", "ticker", "rn"], orient="row")
        
        # Get index data
        index_result = ClickHousePool.execute_query(index_query)
        index_df = pl.DataFrame(index_result, schema=["created_at", "symbol", "close_price", "security_code", "previous_close", "ticker", "rn"], orient="row")
        
        current_data = pl.concat([stock_df, index_df])
        current_data = current_data.drop(["rn"])
        
        logging.info(f"Retrieved {len(current_data)} current stock price records for today from ClickHouse")
        
        if len(current_data) == 0:
            logging.info("No current day stock data found")
            return
        
        # Save original max timestamp before any date processing
        original_max_timestamp = current_data['created_at'].max()
        logging.info(f"Max timestamp from current data: {original_max_timestamp}")
        
        # Log the max created_at from the current stock data
        max_created_at = current_data['created_at'].max()
        logging.info(f"Max created_at from current stock data: {max_created_at}")
        
        # Remove duplicates in the current data
        current_data = current_data.unique(subset=["security_code", "created_at"])
        logging.info(f"After removing duplicates in current data: {len(current_data)} records")
        
        # Get all security codes for efficient deletion
        security_codes = current_data.select(pl.col('security_code')).unique().to_series().to_list()
        
        # Convert timestamps to Indian timezone and then to naive datetime
        current_data = current_data.with_columns(
            pl.col('created_at').dt.replace_time_zone('UTC').dt.convert_time_zone(INDIAN_TZ)
        )
        current_data = current_data.with_columns(
            pl.col('created_at').dt.replace_time_zone(None)
        )
        
        # Begin transaction for delete+insert operations
        dd_con.execute("BEGIN TRANSACTION")
        
        try:
            # Delete current day's entries for the securities we're updating
            logging.info(f"Deleting current day's entries for the securities we're updating: {len(security_codes)}")
            if security_codes:
                # First check if there are any records to delete
                check_query = f"""
                    SELECT COUNT(*) FROM public.eod_stock_data
                    WHERE created_at::DATE = CURRENT_DATE::DATE
                """
                check_result = dd_con.execute(check_query).fetchone()[0]
                logging.info(f"Found {check_result} existing records for today in eod_stock_data")
                
                # Use parameterized query for safety and correctness
                placeholders = ', '.join(['?' for _ in security_codes])
                delete_query = f"""
                    DELETE FROM public.eod_stock_data
                    WHERE created_at::DATE = CURRENT_DATE::DATE
                    AND security_code IN ({placeholders})
                """
                delete_result = dd_con.execute(delete_query, security_codes)
                deleted_count = delete_result.fetchone()[0]
                logging.info(f"Deleted {deleted_count} existing records for today from eod_stock_data")
            
            # Convert to arrow for duckdb
            df_arrow = current_data.to_arrow()
            
            # Insert new current day data
            insert_result = dd_con.execute("""
                INSERT INTO public.eod_stock_data
                SELECT * FROM df_arrow
            """)
            inserted_count = insert_result.fetchone()[0]
            logging.info(f"Inserted {inserted_count} new records into eod_stock_data")
            print(f"EOD data update: Inserted {inserted_count} new records")
            
            # Update the timestamp with the ORIGINAL full timestamp from current_stock_price
            dd_con.execute("""
                INSERT INTO public.synced_tables (table_name, updated_at, full_refresh_required)
                VALUES (?, ?, ?)
                ON CONFLICT (table_name) 
                DO UPDATE SET updated_at = EXCLUDED.updated_at
            """, ['eod_stock_data', original_max_timestamp, False])
            
            logging.info(f"Updated eod_stock_data timestamp to {original_max_timestamp} in synced_tables")
            
            # Commit the transaction
            dd_con.execute("COMMIT")
            logging.info(f"Successfully completed transaction for EOD data update")
            
        except Exception as e:
            # Rollback on error
            dd_con.execute("ROLLBACK")
            logging.error(f"Transaction rolled back due to error: {str(e)}")
            raise
        
    except Exception as e:
        logging.error(f"Error adding current day stock data: {str(e)}")


def handle_eod_stock_data(dd_con=None, pg_connection=None, force=False):
    """Handle incremental updates for EOD stock data"""
    try:
        if dd_con is None:
            dd_con = ensure_duckdb_schema()
            
        if pg_connection is None:
            pg_connection = get_pg_connection()
            
        # Check if table exists
        table_exists = dd_con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'eod_stock_data'").fetchone()[0]
        
        if not table_exists or force:
            # If table doesn't exist or force refresh, create table and load in batches
            logging.info("EOD stock data table doesn't exist in duckdb or force refresh requested, creating with batch load")
            
            # Create the table schema
            dd_con.execute(f"""
                CREATE TABLE IF NOT EXISTS public.eod_stock_data (
                    created_at TIMESTAMP WITH TIME ZONE,
                    symbol VARCHAR,
                    close_price DOUBLE,
                    security_code VARCHAR,
                    previous_close DOUBLE,
                    ticker VARCHAR
                )
            """)
            
            logging.info("Created eod_stock_data table")
            current_time = datetime.now(timezone.utc)
            
            # Load data in 2-year batches
            total_records, max_timestamp = batch_load_eod_stock_data(dd_con, current_time)
            
            # Add current day's data
            add_current_day_stock_data(dd_con)
            
            # Save the max timestamp if records were loaded
            if total_records > 0 and max_timestamp is not None:
                # Update the timestamp in DuckDB
                dd_con.execute("""
                    INSERT INTO public.synced_tables (table_name, updated_at, full_refresh_required)
                    VALUES (?, ?, ?)
                    ON CONFLICT (table_name) 
                    DO UPDATE SET updated_at = EXCLUDED.updated_at
                """, ['eod_stock_data', max_timestamp, False])
                
                logging.info(f"Successfully loaded {total_records} records in batches")
                print(f"Created initial table of eod_stock_data with {total_records} records")
            else:
                logging.info(f"No data found for the requested time period")
                print("Created initial table of eod_stock_data")
        else:
            # Add current day data
            add_current_day_stock_data(dd_con)
                    
        return True
    except Exception as e:
        logging.error(f"Error handling EOD stock data: {str(e)}")
        return False


def load_eod_data(force=False):
    """Main function to load EOD stock data"""
    try:
        dd_con = ensure_duckdb_schema()
        pg_connection = get_pg_connection()
        
        result = handle_eod_stock_data(dd_con, pg_connection, force)
        
        logging.info(f"EOD data load {'completed successfully' if result else 'failed'}")
        return result
    except Exception as e:
        logging.error(f"Error in load_eod_data: {str(e)}")
        return False
    finally:
        if 'dd_con' in locals():
            dd_con.close() 
