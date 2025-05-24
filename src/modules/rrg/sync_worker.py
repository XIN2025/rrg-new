import duckdb
import polars as pl
from datetime import datetime, timezone, timedelta
from src.db.clickhouse import pool as clickhouse_pool
from src.celery_app import celery_app
import json
import time
import random
import pytz
from src.utils.logger import get_logger
from src.utils.metrics import (
    classSyncWorkerTimer, CeleryTaskMetrics, SyncWorkerQueryMetrics,
    record_sync_worker_rows_loaded, record_sync_worker_rows_deleted,
    record_sync_worker_timestamp_lag, record_sync_worker_timestamp
)

logger = get_logger("sync_worker")

# Define timezone
INDIAN_TZ = 'Asia/Kolkata'


def get_stock_prices_timestamp(conn) -> datetime:
    """Get timestamp of last stock data update from DuckDB synced_tables"""
    with classSyncWorkerTimer("get_stock_prices_timestamp", "rrg_sync_worker"):
        try:
            result = conn.execute("""
                SELECT updated_at 
                FROM public.synced_tables 
                WHERE table_name = 'stock_prices'
            """).fetchone()
            
            if result and result[0]:
                # Return timestamp as-is without timezone conversion
                timestamp = result[0]
                return timestamp
            return None
        except Exception as e:
            logger.error(f"Error getting stock prices timestamp: {str(e)}")
            return None


def update_stock_prices_timestamp(conn, timestamp: datetime):
    """Update stock prices timestamp in DuckDB synced_tables"""
    with classSyncWorkerTimer("update_stock_prices_timestamp", "rrg_sync_worker"):
        try:
            # Use timestamp as-is without timezone conversion
            conn.execute("""
                INSERT INTO public.synced_tables (table_name, updated_at, full_refresh_required)
                VALUES (?, ?, ?)
                ON CONFLICT (table_name) 
                DO UPDATE SET updated_at = EXCLUDED.updated_at
            """, ['stock_prices', timestamp, False])
            
            # Record the timestamp in metrics
            record_sync_worker_timestamp('sync_stock_prices', 'stock_prices', timestamp)
            
            logger.info(f"Updated stock_prices timestamp to {timestamp} in DuckDB synced_tables")
        except Exception as e:
            logger.error(f"Error updating stock prices timestamp: {str(e)}")
      


def sync_stock_prices():
    """Sync new stock prices from ClickHouse to DuckDB stock_prices table"""
    # Define columns for stock_prices table
    stock_prices_columns = ["current_price", "created_at", "security_code", "ticker", "symbol", "previous_close"]
    
    with classSyncWorkerTimer("sync_stock_prices", "rrg_sync_worker"):
        dd_conn = None

        try:
            dd_conn = duckdb.connect("data/pydb.duckdb", read_only=False)
            
            last_sync = get_stock_prices_timestamp(dd_conn)
            
            if last_sync is None:
                logger.warning("No previous stock prices timestamp found. Starting from now.")
                last_sync = datetime.now() - timedelta(minutes=5)
            else:
                logger.info(f"Last stock prices update was at {last_sync} (timezone: {last_sync.tzinfo})")
                # Record timestamp lag
                record_sync_worker_timestamp_lag('sync_stock_prices', 'stock_prices', last_sync)
            
            # Format timestamp for ClickHouse query (without timezone info)
            last_sync_formatted = last_sync.strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"Using timestamp for query: {last_sync}")
            logger.info(f"Formatted last sync timestamp for query: {last_sync_formatted}")
            
            # Incremental stock data query from minute_data_loader.py
            stock_query = f"""
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
                ep.date_time >= '{last_sync_formatted}'
                AND ep.close != 0
                AND toMinute(ep.date_time) % 5 = 0
            ORDER BY ep.date_time DESC
            """
            
            # Incremental index data query from minute_data_loader.py
            index_query = f"""
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
                ip.date_time >= '{last_sync_formatted}'
                AND toMinute(ip.date_time) % 5 = 0
                AND ip.close != 0
            ORDER BY 
                ip.date_time DESC
            """
            
            # Use metrics to track query execution time
            with SyncWorkerQueryMetrics('sync_stock_prices', 'stock', 'clickhouse'):
                logger.info(f"Executing ClickHouse stock query...")
                stock_result = clickhouse_pool.execute_query(stock_query)
            
            with SyncWorkerQueryMetrics('sync_stock_prices', 'index', 'clickhouse'):
                logger.info(f"Executing ClickHouse index query...")
                index_result = clickhouse_pool.execute_query(index_query)
            
            # Convert to DataFrames
            stock_df = pl.DataFrame(stock_result, schema=stock_prices_columns, orient="row")
            index_df = pl.DataFrame(index_result, schema=stock_prices_columns, orient="row")
            
            # Combine both DataFrames
            df = pl.concat([stock_df, index_df])

            # Record data points count
            stock_count = len(stock_df)
            index_count = len(index_df)
            total_count = len(df)
            
            logger.info(f"Found {total_count} new records since {last_sync} (stocks: {stock_count}, indices: {index_count})")
            
            if total_count > 0:
                # Save the original max timestamp
                original_max_timestamp = df['created_at'].max()
                logger.info(f"Max timestamp from data: {original_max_timestamp}")
                
                # Begin transaction
                dd_conn.execute("BEGIN TRANSACTION")
                
                try:
                    # Convert to arrow for DuckDB
                    df_arrow = df.to_arrow()
                    
                    # Insert the new records
                    insert_result = dd_conn.execute("""
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
                    
                    # Record inserted row count in metrics
                    record_sync_worker_rows_loaded('sync_stock_prices', 'clickhouse', 'stock_prices', inserted_count)
                    
                    # Update the timestamp in DuckDB
                    update_stock_prices_timestamp(dd_conn, original_max_timestamp)
                    
                    # Commit the transaction
                    dd_conn.execute("COMMIT")
                    logger.info(f"Successfully committed {inserted_count} records to stock_prices table")
                    return True
                except Exception as e:
                    # Rollback on error
                    dd_conn.execute("ROLLBACK")
                    logger.error(f"Transaction rolled back: {str(e)}")
                    return False
            else:
                logger.info("No new records to sync")
                
                # Update the timestamp to current time
                current_time = datetime.now()
                update_stock_prices_timestamp(dd_conn, current_time)
                return True
        except Exception as e:
            logger.error(f"Error syncing stock prices: {str(e)}")
            return False
        finally:
            if dd_conn:
                dd_conn.close()
                logger.info("DuckDB connection closed")


def sync_stock_price_eod():
    """Sync stock price EOD data from ClickHouse to DuckDB eod_stock_data table"""
    # Define columns for eod_stock_data table including rn
    stock_prices_columns = ["created_at", "symbol", "close_price", "security_code", "previous_close", "ticker", "rn"]
    # Final columns after dropping rn
    eod_stock_data_columns = ["created_at", "symbol", "close_price", "security_code", "previous_close", "ticker"]
    
    with classSyncWorkerTimer("sync_stock_price_eod", "rrg_sync_worker"):
        dd_conn = None

        try:
            # Initialize DuckDB connection
            dd_conn = duckdb.connect("data/pydb.duckdb", read_only=False)
            
            # Get the last sync timestamp for EOD stock data
            last_sync_result = dd_conn.execute("""
                SELECT updated_at 
                FROM public.synced_tables 
                WHERE table_name = 'eod_stock_data'
            """).fetchone()
            
            if last_sync_result and last_sync_result[0]:
                last_sync = last_sync_result[0]
                # Use timestamp as-is without timezone conversion
                # Record timestamp lag
                record_sync_worker_timestamp_lag('sync_stock_price_eod', 'eod_stock_data', last_sync)
            else:
                # If no previous sync, use a timestamp from 24 hours ago
                last_sync = datetime.now(INDIAN_TZ) - timedelta(days=1)
                
            logger.info(f"Last EOD stock data update was at {last_sync} (timezone: {last_sync.tzinfo})")
            
            # Format timestamp for ClickHouse query
            last_sync_formatted = last_sync.strftime('%Y-%m-%d %H:%M:%S')
            
            # Incremental stock data query from eod_data_loader.py
            stock_query = f"""
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
                ep.date_time >= '{last_sync_formatted}'
                AND ep.close != 0
                AND toMinute(ep.date_time) % 5 = 0
            QUALIFY rn = 1
            ORDER BY ep.date_time DESC
            """
            
            # Incremental index data query from eod_data_loader.py
            index_query = f"""
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
                ip.date_time >= '{last_sync_formatted}'
                AND toMinute(ip.date_time) % 5 = 0
                AND ip.close != 0
            QUALIFY rn = 1
            ORDER BY 
                ip.date_time DESC
            """
            
            # Get stock data with metrics tracking
            with SyncWorkerQueryMetrics('sync_stock_price_eod', 'stock', 'clickhouse'):
                logger.info(f"Executing ClickHouse stock query...")
                stock_result = clickhouse_pool.execute_query(stock_query)
            stock_df = pl.DataFrame(stock_result, schema=stock_prices_columns, orient="row")
            
            # Get index data with metrics tracking
            with SyncWorkerQueryMetrics('sync_stock_price_eod', 'index', 'clickhouse'):
                logger.info(f"Executing ClickHouse index query...")
                index_result = clickhouse_pool.execute_query(index_query)
            index_df = pl.DataFrame(index_result, schema=stock_prices_columns, orient="row")
            
            # Combine both DataFrames
            current_data = pl.concat([stock_df, index_df])
            
            # Drop the rn column
            current_data = current_data.drop("rn")
            
            # Record data points count
            stock_count = len(stock_df)
            index_count = len(index_df)
            total_count = len(current_data)
            
            logger.info(f"Retrieved {total_count} current stock price records for today from ClickHouse (stocks: {stock_count}, indices: {index_count})")
            
            if total_count == 0:
                logger.info("No current day stock data found")
                return True
            
            # Save original max timestamp before any date processing
            original_max_timestamp = current_data['created_at'].max()
            logger.info(f"Max timestamp from current data: {original_max_timestamp}")
            
            # Remove duplicates in the current data
            current_data = current_data.unique(subset=["security_code", "created_at"])
            logger.info(f"After removing duplicates in current data: {len(current_data)} records")
            
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
            dd_conn.execute("BEGIN TRANSACTION")
            
            try:
                # Delete current day's entries for the securities we're updating
                logger.info(f"Deleting current day's entries for the securities we're updating: {len(security_codes)}")
                if security_codes:
                    # First check if there are any records to delete
                    check_query = f"""
                        SELECT COUNT(*) FROM public.eod_stock_data
                        WHERE created_at::DATE = CURRENT_DATE::DATE
                    """
                    check_result = dd_conn.execute(check_query).fetchone()[0]
                    logger.info(f"Found {check_result} existing records for today in eod_stock_data")
                    
                    # Use parameterized query for safety and correctness
                    placeholders = ', '.join(['?' for _ in security_codes])
                    delete_query = f"""
                        DELETE FROM public.eod_stock_data
                        WHERE created_at::DATE = CURRENT_DATE::DATE
                        AND security_code IN ({placeholders})
                    """
                    delete_result = dd_conn.execute(delete_query, security_codes)
                    deleted_count = delete_result.fetchone()[0]
                    logger.info(f"Deleted {deleted_count} existing records for today from eod_stock_data")
                    
                    # Record deleted row count in metrics
                    record_sync_worker_rows_deleted('sync_stock_price_eod', 'eod_stock_data', deleted_count)
                
                # Convert to arrow for duckdb
                df_arrow = current_data.to_arrow()
                
                # Insert new current day data
                insert_result = dd_conn.execute("""
                    INSERT INTO public.eod_stock_data
                    SELECT * FROM df_arrow
                """)
                inserted_count = insert_result.fetchone()[0]
                logger.info(f"Inserted {inserted_count} new records into eod_stock_data")
                print(f"EOD data update: Inserted {inserted_count} new records")
                
                # Record inserted row count in metrics
                record_sync_worker_rows_loaded('sync_stock_price_eod', 'clickhouse', 'eod_stock_data', inserted_count)
                
                # Update the timestamp with the ORIGINAL full timestamp from current_stock_price
                # Use timestamp as-is without timezone conversion
                dd_conn.execute("""
                    INSERT INTO public.synced_tables (table_name, updated_at, full_refresh_required)
                    VALUES (?, ?, ?)
                    ON CONFLICT (table_name) 
                    DO UPDATE SET updated_at = EXCLUDED.updated_at
                """, ['eod_stock_data', original_max_timestamp, False])
                
                # Record the timestamp in metrics
                record_sync_worker_timestamp('sync_stock_price_eod', 'eod_stock_data', original_max_timestamp)
                
                logger.info(f"Updated eod_stock_data timestamp to {original_max_timestamp} in synced_tables")
                
                # Commit the transaction
                dd_conn.execute("COMMIT")
                logger.info(f"Successfully completed transaction for EOD data update")
                return True
                
            except Exception as e:
                # Rollback on error
                dd_conn.execute("ROLLBACK")
                logger.error(f"Transaction rolled back due to error: {str(e)}")
                return False
                
        except Exception as e:
            logger.error(f"Error adding current day stock data: {str(e)}")
            return False
        finally:
            if dd_conn:
                dd_conn.close()
                logger.info("DuckDB connection closed")

@celery_app.task(name="run_stock_prices_sync")
def sync_stock_prices_task():
    """Celery task wrapper for sync_stock_prices with metrics"""
    with CeleryTaskMetrics('run_stock_prices_sync', 'data_processing'):
        success = sync_stock_prices()
        return success

@celery_app.task(name="sync_stock_prices_eod")
def sync_stock_price_eod_task():
    """Celery task wrapper for sync_stock_price_eod with metrics"""
    with CeleryTaskMetrics('sync_stock_prices_eod', 'data_processing'):
        success = sync_stock_price_eod()
        return success
