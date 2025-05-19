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

# Define columns for stock_prices table
stock_prices_columns = ["current_price", "created_at", "security_code", "ticker", "symbol", "previous_close"]

# Base ClickHouse query for stock data( loads 0 to 2 months data)
base_stock_data_query1 = """
SELECT 
    ep.close as current_price,
    ep.date_time as created_at,
    ms.security_code AS security_code,
    ms.ticker AS ticker,
    ms.symbol AS symbol,
    lagInFrame(ep.close) OVER (PARTITION BY ep.security_token ORDER BY ep.date_time) AS previous_close
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
    ep.date_time > '{current_time}' - INTERVAL '1 months' - INTERVAL '15 days'
    AND toMinute(ep.date_time) % 5 = 0
    AND ep.close != 0
ORDER BY ep.date_time DESC
"""

# Base ClickHouse query for stock data( loads 2 months to 4 months data)
base_stock_data_query2 = """
SELECT 
    ep.close as current_price,
    ep.date_time as created_at,
    ms.security_code AS security_code,
    ms.ticker AS ticker,
    ms.symbol AS symbol,
    lagInFrame(ep.close) OVER (PARTITION BY ep.security_token ORDER BY ep.date_time) AS previous_close
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
    ep.date_time > '{current_time}' - INTERVAL '3 months'
    AND ep.date_time <= '{current_time}' - INTERVAL '1 months' - INTERVAL '15 days'
    AND toMinute(ep.date_time) % 5 = 0
    AND ep.close != 0
ORDER BY ep.date_time DESC
"""


base_stock_data_query3 = """
SELECT 
    ep.close as current_price,
    ep.date_time as created_at,
    ms.security_code AS security_code,
    ms.ticker AS ticker,
    ms.symbol AS symbol,
    lagInFrame(ep.close) OVER (PARTITION BY ep.security_token ORDER BY ep.date_time) AS previous_close
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
    ep.date_time > '{current_time}' - INTERVAL '4 months' - INTERVAL '10 days'
    AND ep.date_time <= '{current_time}' - INTERVAL '3 months'
    AND toMinute(ep.date_time) % 5 = 0
    AND ep.close != 0
ORDER BY ep.date_time DESC
"""
# Incremental query for updates after initial load
incremental_stock_data_query = """
SELECT 
    ep.close as current_price,
    ep.date_time as created_at,
    ms.security_code AS security_code,
    ms.ticker AS ticker,
    ms.symbol AS symbol,
    lagInFrame(ep.close) OVER (PARTITION BY ep.security_token ORDER BY ep.date_time) AS previous_close
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
    AND toMinute(ep.date_time) % 5 = 0
ORDER BY ep.date_time DESC
"""

# Base ClickHouse query for index data
base_index_data_query = """
SELECT 
    ip.close AS current_price,
    ip.date_time AS created_at,
    mi.security_code AS security_code,
    mi.ticker AS ticker,
    mi.symbol AS symbol,
    lagInFrame(ip.close) OVER (PARTITION BY ip.security_token ORDER BY ip.date_time) AS previous_close
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
    ip.date_time > '{current_time}' - interval '4 months' - interval '10 days'
    AND toMinute(ip.date_time) % 5 = 0
    AND ip.close != 0
ORDER BY 
    ip.date_time DESC
"""

# Incremental query for index data updates
incremental_index_data_query = """
SELECT 
    ip.close AS current_price,
    ip.date_time AS created_at,
    mi.security_code AS security_code,
    mi.ticker AS ticker,
    mi.symbol AS symbol,
    lagInFrame(ip.close) OVER (PARTITION BY ip.security_token ORDER BY ip.date_time) AS previous_close
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

def handle_hourly_stock_data(dd_con=None, pg_connection=None, force=False):
    """Handle incremental updates and bulk loading for hourly stock and index data"""
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
            # If table doesn't exist or force refresh, load all 6 months of data in one go
            logging.info("Stock prices table doesn't exist or force refresh requested, loading full 6 months data")
            
            # Create the table schema
            dd_con.execute(f"""
                CREATE TABLE IF NOT EXISTS public.stock_prices (
                    current_price DOUBLE,
                    created_at TIMESTAMP WITH TIME ZONE,
                    security_code VARCHAR,
                    ticker VARCHAR,
                    symbol VARCHAR,
                    previous_close DOUBLE
                )
            """)
            
            logging.info("Created indexes on stock_prices table")
            current_time = datetime.now(timezone(timedelta(hours=5, minutes=30)))
            current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
            
            
            # Load stock data
            stock_query = base_stock_data_query1.format(current_time=current_time_str)
            stock_result = ClickHousePool.execute_query(stock_query)
                        # Convert to DataFrames - handle empty results gracefully
            stock_df = process_query_result(stock_result, stock_prices_columns)
            insert_data_into_duckdb(stock_df, dd_con)  
            original_max_timestamp = stock_df['created_at'].max()

            stock_query2 = base_stock_data_query2.format(current_time=current_time_str)
            stock_result = ClickHousePool.execute_query(stock_query2)
            stock_df = process_query_result(stock_result, stock_prices_columns)
            insert_data_into_duckdb(stock_df, dd_con)

            stock_query3 = base_stock_data_query3.format(current_time=current_time_str)
            stock_result = ClickHousePool.execute_query(stock_query3)
            stock_df = process_query_result(stock_result, stock_prices_columns)
            insert_data_into_duckdb(stock_df, dd_con)

            # Load index data
            index_query = base_index_data_query.format(current_time=current_time_str)
            index_result = ClickHousePool.execute_query(index_query)
            index_df = process_query_result(index_result, stock_prices_columns)
            insert_data_into_duckdb(index_df, dd_con)

                # Save the original max timestamp
                
            try:
                # Update the timestamp in DuckDB (keeping in Indian time)
                dd_con.execute("""
                    INSERT INTO public.synced_tables (table_name, updated_at, full_refresh_required)
                    VALUES (?, ?, ?)
                    ON CONFLICT (table_name) 
                    DO UPDATE SET updated_at = EXCLUDED.updated_at
                """, ['stock_prices', original_max_timestamp, False])
                
                logging.info(f"Successfully committed full 6 months data load")
            except Exception as e:
                logging.error(f"Error updating timestamp: {str(e)}")
                raise
            else:
                logging.info(f"No data found for the last 6 months")
                
        else:
            # Get the last update timestamp from DuckDB
            last_updated = get_last_sync_timestamp(dd_con, "stock_prices")
            
            if not last_updated:
                # If no timestamp, trigger a full reload
                logging.info("No timestamp found, performing full reload")
                return handle_hourly_stock_data(dd_con, pg_connection, force=True)
            
            # Format the timestamp for ClickHouse (keeping in Indian time)
            last_updated_str = last_updated.strftime("%Y-%m-%d %H:%M:%S")
            
            # Create incremental queries
            stock_incremental_query = incremental_stock_data_query.format(last_updated=last_updated_str)
            index_incremental_query = incremental_index_data_query.format(last_updated=last_updated_str)
            
            # Get new data from ClickHouse for both stocks and indices
            
            # Load stock data
            stock_result = ClickHousePool.execute_query(stock_incremental_query)
            
            
            # Load index data
            index_result = ClickHousePool.execute_query(index_incremental_query)
            
            
            # Convert to DataFrames - handle empty results gracefully
            stock_df = pl.DataFrame([] if not stock_result else stock_result, 
                                   schema=stock_prices_columns, 
                                   orient="row") if stock_result is not None else pl.DataFrame(schema=stock_prices_columns)
            
            index_df = pl.DataFrame([] if not index_result else index_result, 
                                   schema=stock_prices_columns, 
                                   orient="row") if index_result is not None else pl.DataFrame(schema=stock_prices_columns)
                        
            # Combine both DataFrames
            df = pl.concat([stock_df, index_df])
            
            if len(df) > 0:
                logging.info(f"Found {len(df)} new records since {last_updated} (stocks: {len(stock_df)}, indices: {len(index_df)})")
                
                # Save the original max timestamp
                original_max_timestamp = df['created_at'].max()
                logging.info(f"Max timestamp from fetched data: {original_max_timestamp}")
                
                # Begin transaction
                dd_con.execute("BEGIN TRANSACTION")
                
                try:
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
                    logging.info(f"Inserted {inserted_count} new records")
                    
                    # Update the timestamp in DuckDB (keeping in Indian time)
                    dd_con.execute("""
                        INSERT INTO public.synced_tables (table_name, updated_at, full_refresh_required)
                        VALUES (?, ?, ?)
                        ON CONFLICT (table_name) 
                        DO UPDATE SET updated_at = EXCLUDED.updated_at
                    """, ['stock_prices', original_max_timestamp, False])
                    
                    # Commit the transaction
                    dd_con.execute("COMMIT")
                    logging.info(f"Successfully added {len(df)} new records to stock_prices table")
                except Exception as e:
                    # Rollback on error
                    dd_con.execute("ROLLBACK")
                    logging.error(f"Transaction rolled back due to error: {str(e)}")
                    raise
                else:  # This else should align with try/except
                    logging.info("No new data found")
                    
                    # Update the timestamp to current time in Indian timezone
                    current_time = datetime.now(INDIAN_TZ)
                    update_sync_timestamp(dd_con, "stock_prices", current_time)
                    
        return True
    except Exception as e:
        logging.error(f"Error handling hourly stock and index data: {str(e)}")
        return False



def process_query_result(result, columns):
    """Process query result with proper type handling for decimal values
    
    Args:
        result: The query result from ClickHouse
        columns: The column schema to apply
        
    Returns:
        A polars DataFrame with properly handled types
    """
    if not result:
        return pl.DataFrame(schema={
            "current_price": pl.Float64,
            "created_at": pl.Datetime,
            "security_code": pl.Utf8,
            "ticker": pl.Utf8,
            "symbol": pl.Utf8,
            "previous_close": pl.Float64
        })
    
    # Create a proper schema dictionary
    schema = {
        "current_price": pl.Float64,
        "created_at": pl.Datetime,
        "security_code": pl.Utf8,
        "ticker": pl.Utf8,
        "symbol": pl.Utf8,
        "previous_close": pl.Float64
    }
    
    # Convert the result to a list of dictionaries
    rows = []
    for row in result:
        row_dict = {}
        for i, col in enumerate(columns):
            if i < len(row):
                # Handle decimal values by converting to float
                if hasattr(row[i], 'as_tuple') and callable(getattr(row[i], 'as_tuple')):
                    # This is likely a Decimal type
                    row_dict[col] = float(row[i])
                else:
                    row_dict[col] = row[i]
            else:
                # Handle missing values
                if col in ["current_price", "previous_close"]:
                    row_dict[col] = 0.0
                elif col == "created_at":
                    row_dict[col] = datetime.now(INDIAN_TZ)
                else:
                    row_dict[col] = ""
        rows.append(row_dict)
    
    # Create DataFrame with explicit schema
    df = pl.DataFrame(rows, schema=schema)
    
    # Convert timestamps to Indian timezone
    df = df.with_columns(
        pl.col("created_at").dt.replace_time_zone("UTC").dt.convert_time_zone("Asia/Kolkata")
    )
    
    return df


def load_hourly_data(force=False):
    """Main function to load hourly stock data"""
    try:
        dd_con = ensure_duckdb_schema()
        pg_connection = get_pg_connection()
        
        result = handle_hourly_stock_data(dd_con, pg_connection, force)
        
        logging.info(f"Hourly data load {'completed successfully' if result else 'failed'}")
        return result
    except Exception as e:
        logging.error(f"Error in load_hourly_data: {str(e)}")
        return False
