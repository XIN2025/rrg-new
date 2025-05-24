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

def load_eod_data(days=30, force=False, db_path=None):
    """Load EOD data from ClickHouse to DuckDB"""
    try:
        start_time = time.time()
        
        # For testing, use a small sample first
        test_mode = True
        if test_mode:
            days = 5  # Just 5 days of data
            chunk_size = 10  # Small chunks for testing

        logger.info("Initializing ClickHouse connection...")
        logger.info(f"CHDB_HOST: {os.getenv('CHDB_HOST')}")
        logger.info(f"CHDB_PORT: {os.getenv('CHDB_PORT')}")
        logger.info(f"CHDB_USER: {os.getenv('CHDB_USER')}")
        logger.info(f"CHDB_NAME: {os.getenv('CHDB_NAME')}")

        # Create schema and table if they don't exist
        with get_duckdb_connection(db_path) as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS public")
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

        # Count total records to load
        count_query = f"""
            SELECT COUNT(*) as total
            FROM strike.equity_prices_1d e
            JOIN strike.mv_stocks s ON e.security_code = s.security_code
            WHERE e.date_time >= now() - INTERVAL {int(days)} DAY
            AND e.close > 0
        """
        result = ClickHousePool.execute_query(count_query)
        total_records = result[0][0]
        logger.info(f"Total records to load: {total_records:,}")

        # Load data in chunks
        offset = 0
        total_loaded = 0

        while offset < total_records:
            # Query to get data with all required columns
            query = f"""
                WITH ordered_data AS (
                    SELECT
                        e.date_time as created_at,
                        e.close / NULLIF(lagInFrame(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time), 0) as ratio,
                        e.close / NULLIF(avg(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time ROWS BETWEEN 20 PRECEDING AND CURRENT ROW), 0) as momentum,
                        e.close as close_price,
                        (e.close - NULLIF(lagInFrame(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time), 0)) / NULLIF(lagInFrame(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time), 0) * 100 as change_percentage,
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
                    WHERE e.date_time >= now() - INTERVAL {int(days)} DAY
                    AND e.close > 0
                    ORDER BY e.date_time DESC
                    LIMIT {chunk_size} OFFSET {offset}
                )
                SELECT * FROM ordered_data
            """
            
            logger.info(f"Executing query for chunk {offset//chunk_size + 1}...")
            result = ClickHousePool.execute_query(query)
            
            if not result:
                logger.warning(f"No data returned for chunk {offset//chunk_size + 1}")
                break
                
            chunk_records = len(result)
            total_loaded += chunk_records
            
            # Print sample data for verification
            if offset == 0:
                logger.info("Sample data from first chunk:")
                for row in result[:2]:  # Show first 2 rows
                    logger.info(f"Row: {row}")
            
            # Calculate and log progress metrics
            elapsed_time = time.time() - start_time
            avg_speed = total_loaded / elapsed_time if elapsed_time > 0 else 0
            estimated_remaining = (total_records - total_loaded) / avg_speed if avg_speed > 0 else 0
            
            logger.info(f"Chunk {offset//chunk_size + 1} loaded: {chunk_records} records")
            logger.info(f"Total loaded: {total_loaded}/{total_records} records ({(total_loaded/total_records*100):.1f}%)")
            logger.info(f"Average speed: {int(avg_speed)} records/sec")
            logger.info(f"Estimated time remaining: {int(estimated_remaining)} seconds")
            
            # Bulk insert into DuckDB using a temporary table
            with get_duckdb_connection(db_path) as conn:
                logger.info(f"Inserting chunk {offset//chunk_size + 1} into DuckDB...")
                conn.execute("""
                    CREATE TEMPORARY TABLE temp_eod_data (
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
                conn.executemany("""
                    INSERT INTO temp_eod_data 
                    (created_at, ratio, momentum, close_price, change_percentage, metric_1, metric_2, metric_3, signal, security_code, previous_close, ticker)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, result)
                conn.execute("""
                    INSERT INTO public.eod_stock_data 
                    SELECT * FROM temp_eod_data
                """)
                conn.execute("DROP TABLE temp_eod_data")
                logger.info(f"Chunk {offset//chunk_size + 1} inserted successfully")
            
            offset += chunk_size
            
            # Log memory usage
            process = psutil.Process()
            memory_info = process.memory_info()
            logger.info(f"Memory usage: {memory_info.rss / 1024 / 1024:.1f} MB")

        # Log final statistics
        total_time = time.time() - start_time
        avg_speed = total_loaded / total_time if total_time > 0 else 0
        
        logger.info("EOD data load completed:")
        logger.info(f"Total records loaded: {total_loaded:,}")
        logger.info(f"Total time: {total_time:.2f} seconds")
        logger.info(f"Average speed: {int(avg_speed)} records/sec")
        
        # Verify the data in DuckDB
        with get_duckdb_connection(db_path) as conn:
            sample = conn.execute("""
                SELECT * FROM public.eod_stock_data 
                LIMIT 5
            """).fetchdf()
            logger.info("\nSample data from DuckDB:")
            logger.info(sample)
        
        return True
        
    except Exception as e:
        logger.error(f"Error loading EOD data: {str(e)}", exc_info=True)
        return False
