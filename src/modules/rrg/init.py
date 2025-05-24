"""
Database initialization for RRG module.
"""
import os
import sys
# Add the project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, project_root)

# Now import everything else
from pathlib import Path
import time
from typing import List, Dict, Any
import traceback
from src.utils.logger import get_logger
from src.utils.duck_pool import get_duckdb_connection
from src.utils.clickhouse_pool import pool as ClickHousePool
import logging

from src.modules.db.reference_data_loader import load_all_reference_data
from src.modules.db.eod_data_loader import load_eod_data
from src.modules.db.data_manager import load_hourly_data

logger = get_logger("rrg_init")
logger.setLevel(logging.INFO)

def log_and_print(msg, level="info"):
    print(msg)
    if level == "info":
        logger.info(msg)
    elif level == "warning":
        logger.warning(msg)
    elif level == "error":
        logger.error(msg)
    else:
        logger.debug(msg)

def ensure_data_dir() -> str:
    """Ensure data directory exists at the project root."""
    project_root = Path(__file__).resolve().parents[3]
    data_dir = os.path.join(project_root, "data")
    os.makedirs(data_dir, exist_ok=True)
    return data_dir

def is_table_empty(conn, table_name: str) -> bool:
    """Check if a table exists and has data."""
    try:
        result = conn.execute(f"SELECT COUNT(*) FROM public.{table_name}").fetchone()
        return result[0] == 0 if result else True
    except Exception as e:
        log_and_print(f"Error checking table {table_name}: {str(e)}", level="error")
        return True

def verify_table_data(conn, table_name: str) -> Dict[str, Any]:
    """Verify data integrity for a specific table."""
    try:
        # Check row count
        count = conn.execute(f"SELECT COUNT(*) FROM public.{table_name}").fetchone()[0]
        return {
            "table_name": table_name,
            "row_count": count
        }
    except Exception as e:
        log_and_print(f"Error verifying table {table_name}: {str(e)}", level="error")
        return {"table_name": table_name, "error": str(e)}

def init_database() -> bool:
    """Initialize the database with required tables."""
    try:
        logger.info("[INIT] Starting database initialization...")
        
        # Get DuckDB path
        db_path = os.path.join("data", "pydb.duckdb")
        logger.info(f"[INIT] DuckDB path: {os.path.abspath(db_path)}")
        
        # Initialize connection pool
        with get_duckdb_connection() as conn:
            # Create public schema if it doesn't exist
            conn.execute("CREATE SCHEMA IF NOT EXISTS public")
            
            # Create tables if they don't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS public.market_metadata (
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
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS public.eod_stock_data (
                    created_at TIMESTAMP,
                    security_code VARCHAR,
                    ticker VARCHAR,
                    close_price DOUBLE,
                    previous_close DOUBLE,
                    ratio DOUBLE,
                    momentum DOUBLE,
                    change_percentage DOUBLE,
                    metric_1 DOUBLE,
                    metric_2 DOUBLE,
                    metric_3 DOUBLE,
                    signal INTEGER
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS public.hourly_stock_data (
                    created_at TIMESTAMP,
                    ratio DOUBLE,
                    momentum DOUBLE,
                    close_price DOUBLE,
                    change_percentage DOUBLE,
                    metric_1 DOUBLE,
                    metric_2 DOUBLE,
                    metric_3 DOUBLE,
                    signal INTEGER,
                    security_code VARCHAR,
                    previous_close DOUBLE,
                    ticker VARCHAR
                )
            """)
            
            # Load market metadata from ClickHouse
            metadata_query = """
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
                WHERE symbol IS NOT NULL
            """
            
            metadata_data = ClickHousePool.execute_query(metadata_query)
            if metadata_data:
                conn.executemany("""
                    INSERT INTO public.market_metadata 
                    (security_token, security_code, company_name, symbol, alternate_symbol, ticker, is_fno, stocks_count, stocks, security_codes, security_tokens, series, category, exchange_group, security_type_code)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, metadata_data)
                logger.info(f"Loaded {len(metadata_data)} market metadata records")
            
            # Verify tables
            tables_to_verify = [
                "market_metadata",
                "eod_stock_data",
                "hourly_stock_data"
            ]
            
            tables_to_reload = []
            for table in tables_to_verify:
                try:
                    # Check if table exists and has data
                    result = conn.execute(f"SELECT COUNT(*) FROM public.{table}").fetchone()
                    if result[0] == 0:
                        logger.warning(f"[INIT] Table {table} is empty")
                        tables_to_reload.append({"table_name": table, "error": "Table is empty"})
                except Exception as e:
                    logger.error(f"Error verifying table {table}: {str(e)}")
                    tables_to_reload.append({"table_name": table, "error": str(e)})
            
            if tables_to_reload:
                logger.warning(f"[INIT] Tables need reloading: {tables_to_reload}")
                logger.info("[INIT] Starting data reload process...")
                
                # Load reference data first
                logger.info("[INIT] Loading reference data...")
                if not load_all_reference_data(force=True, db_path=db_path):
                    logger.error("[INIT] Failed to load reference data")
                    return False
                
                # Load market data
                logger.info("[INIT] Loading market data...")
                if not load_eod_data(days=365, force=True, db_path=db_path):
                    logger.error("[INIT] Failed to load market data")
                    return False
                
                logger.info("[INIT] Data reload completed successfully")
            else:
                logger.info("[INIT] All tables are up to date")
            
            return True
            
    except Exception as e:
        logger.error(f"[INIT] Error during database initialization: {str(e)}")
        return False

def verify_database() -> bool:
    """Verify database setup and data integrity."""
    try:
        log_and_print("[VERIFY] Starting database verification...")
        data_dir = ensure_data_dir()
        db_path = os.path.join(data_dir, "pydb.duckdb")
        with get_duckdb_connection(db_path) as conn:
            required_tables = [
                "market_metadata", "eod_stock_data", "hourly_stock_data", 
                "companies", "stocks", "indices", "indices_stocks"
            ]
            verification_results = {}
            for table in required_tables:
                result = verify_table_data(conn, table)
                verification_results[table] = result
                if result.get("error") or result["row_count"] == 0:
                    log_and_print(f"[VERIFY] Table {table} verification failed: {result}", level="error")
                    return False
                log_and_print(f"[VERIFY] Table {table} verified: {result}")
            log_and_print("[VERIFY] Database verification results:")
            for table, result in verification_results.items():
                log_and_print(f"[VERIFY] {table}: {result}")
            return True
    except Exception as e:
        log_and_print(f"[VERIFY] Error verifying database: {str(e)}", level="error")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    log_and_print("[MAIN] Database initialization script started.")
    result_init = init_database()
    if result_init:
        log_and_print("[MAIN] Database initialized successfully.")
        result_verify = verify_database()
        if result_verify:
            log_and_print("[MAIN] Database verification passed.")
        else:
            log_and_print("[MAIN] Database verification failed.", level="error")
    else:
        log_and_print("[MAIN] Database initialization failed.", level="error")
    log_and_print("[MAIN] Script finished.") 
