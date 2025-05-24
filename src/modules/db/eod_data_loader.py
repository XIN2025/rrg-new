import logging
import duckdb
import os
import time
import psutil
from src.utils.logger import get_logger
from src.utils.duck_pool import get_duckdb_connection
from src.utils.clickhouse_pool import pool as ClickHousePool
from src.modules.db.config import MAX_RECORDS

logger = get_logger("eod_data_loader")

def load_eod_data(days=30, force=False):
    """Load EOD data from ClickHouse to DuckDB"""
    try:
        start_time = time.time()
        
        # Validate days parameter
        if not isinstance(days, (int, float)) or days <= 0:
            logger.warning("Invalid days parameter, defaulting to 30 days")
            days = 30

        logger.info("Initializing ClickHouse connection...")
        logger.info(f"CHDB_HOST: {os.getenv('CHDB_HOST')}")
        logger.info(f"CHDB_PORT: {os.getenv('CHDB_PORT')}")
        logger.info(f"CHDB_USER: {os.getenv('CHDB_USER')}")
        logger.info(f"CHDB_NAME: {os.getenv('CHDB_NAME')}")

        # Create schema and table if they don't exist
        with get_duckdb_connection() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS public")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS public.eod_stock_data (
                    created_at TIMESTAMP,
                    symbol VARCHAR,
                    close_price DECIMAL(18,2),
                    security_code VARCHAR,
                    previous_close DECIMAL(18,2),
                    ticker VARCHAR
                )
            """)

        # Count total records to load
        count_query = f"""
            SELECT COUNT(*) as total
            FROM strike.equity_prices_1d e
            JOIN strike.mv_stocks s ON e.security_code = s.security_code
            WHERE e.date_time >= now() - INTERVAL {int(days)} DAY
        """
        result = ClickHousePool.execute_query(count_query)
        total_records = result[0][0]
        logger.info(f"Total records to load: {total_records}")

        # Load all data at once
        query = f"""
            WITH ordered_data AS (
                SELECT
                    e.date_time as created_at,
                    s.symbol,
                    e.close as close_price,
                    e.security_code,
                    lagInFrame(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time) as previous_close,
                    s.ticker
                FROM strike.equity_prices_1d e
                JOIN strike.mv_stocks s ON e.security_code = s.security_code
                WHERE e.date_time >= now() - INTERVAL {int(days)} DAY
                ORDER BY e.date_time DESC
            )
            SELECT * FROM ordered_data
        """

        logger.info(f"Loading all {total_records:,} records at once")
        result = ClickHousePool.execute_query(query)

        if not result:
            logger.warning("No data returned from query")
            return False

        # Bulk insert into DuckDB using a temporary table
        with get_duckdb_connection() as conn:
            conn.execute("""
                CREATE TEMPORARY TABLE temp_eod_data (
                    created_at TIMESTAMP,
                    symbol VARCHAR,
                    close_price DECIMAL(18,2),
                    security_code VARCHAR,
                    previous_close DECIMAL(18,2),
                    ticker VARCHAR
                )
            """)
            conn.executemany("""
                INSERT INTO temp_eod_data 
                (created_at, symbol, close_price, security_code, previous_close, ticker)
                VALUES (?, ?, ?, ?, ?, ?)
            """, result)
            conn.execute("""
                INSERT INTO public.eod_stock_data 
                SELECT * FROM temp_eod_data
            """)
            conn.execute("DROP TABLE temp_eod_data")

        # Log final statistics
        total_time = time.time() - start_time
        avg_speed = total_records / total_time if total_time > 0 else 0
        logger.info("EOD data load completed:")
        logger.info(f"Total records: {total_records}")
        logger.info(f"Total time: {total_time:.2f} seconds")
        logger.info(f"Average speed: {int(avg_speed)} records/sec")
        return True

    except Exception as e:
        logger.error(f"Error loading EOD data: {str(e)}")
        return False
