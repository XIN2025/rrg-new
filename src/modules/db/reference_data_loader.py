import logging
import duckdb
import polars as pl
import time
from typing import Optional, Dict, Any, List
from src.modules.db.db_common import (
    handle_table_data,
    ensure_duckdb_schema,
    ensure_synced_table_exists,
    should_refresh_table,
    get_last_sync_timestamp,
    update_sync_timestamp
)
from src.modules.db.slug_utils import generate_slug
from datetime import datetime, timezone
from src.utils.duck_pool import get_duckdb_connection
from src.utils.clickhouse_pool import pool
from src.utils.logger import get_logger
from src.modules.db.config import MAX_RECORDS
from src.modules.db.metadata_store import RRGMetadataStore
import os
from pathlib import Path

# Initialize logger
logger = logging.getLogger("reference_data_loader")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Define column lists for DuckDB tables
companies_columns = ["name"]
stocks_columns = ["company_name", "security_code"]
indices_columns = ["security_code", "name", "slug", "symbol"]
indices_stocks_columns = ["security_code"]
market_metadata_columns = ["company_name", "security_code","ticker", "symbol", "security_type_code", 
                           "company_code", "meaningful_name"]

# Define queries for reference data with limits to avoid timeouts
company_query = """SELECT company_name AS name FROM strike.dion_company_master"""

stock_query = """SELECT company_name, security_code 
                FROM strike.mv_stocks"""

indices_query = """SELECT security_code, company_name AS name, company_name AS slug, symbol 
             FROM strike.mv_indices"""

indices_stocks_query = """SELECT DISTINCT sc AS security_code
            FROM (
                SELECT arrayJoin(security_codes) AS sc
                FROM strike.mv_indices
                WHERE security_codes IS NOT NULL AND length(security_codes) > 0
            )
            WHERE sc IS NOT NULL
"""

metadata_query = """
SELECT
    cm.company_name as company_name,
    mi.security_code AS security_code,
    sm.ticker,
    stock_symbol as symbol,
    sm.security_type_code as security_type_code,
    sm.company_code as company_code,
    cm.company_name as meaningful_name
FROM (
    SELECT
        arrayJoin(arrayZip(stocks, security_codes)) AS zipped,
        zipped.1 AS stock_symbol,
        zipped.2 AS security_code
    FROM strike.mv_indices
) AS mi
LEFT JOIN (
    SELECT
        security_code,
        argMax(ticker, modified_date) AS ticker,
        argMax(company_code, modified_date) AS company_code,
        argMax(security_type_code, modified_date) AS security_type_code
    FROM strike.dion_security_master
    GROUP BY security_code
) AS sm
    ON mi.security_code = sm.security_code
LEFT JOIN strike.dion_company_master AS cm
    ON sm.company_code = cm.company_code
"""
metadata_query2 = """
SELECT 
    i.company_name AS name,
    s.security_code AS security_code,
    i.mv_ticker AS ticker,
    i.symbol AS symbol,
    s.security_type_code AS security_type_code,
    s.company_code AS company_code,
    i.company_name as meaningful_name
FROM (
    SELECT
        trim(ticker) AS mv_ticker,
        symbol,
        company_name
    FROM strike.mv_indices
    WHERE ticker IS NOT NULL
) AS i
LEFT JOIN (
    SELECT
        ticker,
        argMax(security_code, modified_date) AS security_code,
        argMax(security_type_code, modified_date) AS security_type_code,
        argMax(company_code, modified_date) AS company_code
    FROM strike.dion_security_master
    GROUP BY ticker
) AS s
    ON i.mv_ticker = s.ticker
WHERE s.security_type_code IN (5, 26)
  AND s.ticker IS NOT NULL
"""

def verify_table_data(conn, table_name: str, expected_columns: List[str]) -> Dict[str, Any]:
    """Verify data integrity for a specific table."""
    try:
        # Check if table exists
        table_exists = conn.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
        """).fetchone()[0]
        
        if not table_exists:
            return {"error": f"Table {table_name} does not exist"}
        
        # Check if all expected columns exist
        columns = conn.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
        """).fetchall()
        
        existing_columns = [col[0] for col in columns]
        missing_columns = [col for col in expected_columns if col not in existing_columns]
        
        if missing_columns:
            return {"error": f"Missing columns in {table_name}: {missing_columns}"}
        
        # Check row count
        count = conn.execute(f"SELECT COUNT(*) FROM public.{table_name}").fetchone()[0]
        
        # Check for null values in critical columns
        null_check = {}
        for col in expected_columns:
            null_count = conn.execute(f"""
                SELECT COUNT(*) 
                FROM public.{table_name} 
                WHERE {col} IS NULL
            """).fetchone()[0]
            if null_count > 0:
                null_check[col] = null_count
        
        return {
            "table_name": table_name,
            "row_count": count,
            "null_values": null_check,
            "columns": existing_columns
        }
    except Exception as e:
        logger.error(f"Error verifying table {table_name}: {str(e)}")
        return {"error": str(e)}

def load_with_retry(func, *args, max_retries: int = 3, **kwargs) -> bool:
    """Load data with retry mechanism."""
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                logger.info(f"Retry attempt {attempt + 1} of {max_retries}")
                time.sleep(2 ** attempt)  # Exponential backoff
            
            result = func(*args, **kwargs)
            if result:
                return True
                
        except Exception as e:
            logger.error(f"Error in attempt {attempt + 1}: {str(e)}")
            if attempt == max_retries - 1:
                return False
    
    return False

def add_slug_column_to_table(dd_con, table_name, name_column="name"):
    """
    Add a slug column to an existing table after it has been loaded
    
    Args:
        dd_con: DuckDB connection
        table_name: Name of the table to add the slug to
        name_column: Column to use as the source for generating the slug (default: 'name')
    
    Returns:
        bool: Success status
    """
    try:
        # Check if the table exists
        table_exists = dd_con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table_name}'").fetchone()[0]
        
        if not table_exists:
            logger.warning(f"Cannot add slug to {table_name}: table does not exist")
            return False
            
        # Check if the name column exists
        result = dd_con.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table_name}' AND column_name = '{name_column}'").fetchone()[0]
        
        if not result:
            logger.warning(f"Cannot add slug to {table_name}: column '{name_column}' does not exist")
            return False
            
        # Check if slug column already exists
        slug_exists = dd_con.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table_name}' AND column_name = 'slug'").fetchone()[0]
        
        if slug_exists:
            # If slug column exists, drop it first to rebuild
            logger.info(f"Slug column already exists in {table_name}, recreating it")
            dd_con.execute(f"ALTER TABLE public.{table_name} DROP COLUMN IF EXISTS slug")
        
        # Add the slug column to the table
        dd_con.execute(f"""
            ALTER TABLE public.{table_name} ADD COLUMN slug VARCHAR;
            UPDATE public.{table_name} SET slug = generate_slug({name_column});
        """)
        
        logger.info(f"Successfully added slug column to {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error adding slug column to {table_name}: {str(e)}")
        return False
        

def load_companies(force: bool = False, db_path: Optional[str] = None, dd_con: Optional[duckdb.DuckDBPyConnection] = None) -> bool:
    """Load company data into DuckDB with verification."""
    logger = get_logger("reference_data_loader")
    try:
        if dd_con is None:
            with get_duckdb_connection(db_path) as dd_con:
                result = handle_table_data(
                    table_name="companies", 
                    query=company_query, 
                    force=force,
                    expected_columns=companies_columns
                )
        else:
            result = handle_table_data(
                table_name="companies", 
                query=company_query, 
                force=force,
                expected_columns=companies_columns
            )
        
        if result:
            # Verify the loaded data
            verification = verify_table_data(dd_con, "companies", companies_columns)
            if verification.get("error"):
                logger.error(f"Data verification failed: {verification}")
                return False
            
            logger.info(f"Companies data load completed: {verification}")
            return True
            
        logger.error("Companies data load failed")
        return False
        
    except Exception as e:
        logger.error(f"Error loading companies data: {str(e)}")
        return False


def load_stocks(force: bool = False, db_path: Optional[str] = None, dd_con: Optional[duckdb.DuckDBPyConnection] = None) -> bool:
    """Load stocks data into DuckDB with verification."""
    logger = get_logger("reference_data_loader")
    try:
        if dd_con is None:
            with get_duckdb_connection(db_path) as dd_con:
                result = handle_table_data(
                    table_name="stocks", 
                    query=stock_query, 
                    force=force,
                    expected_columns=stocks_columns
                )
        else:
            result = handle_table_data(
                table_name="stocks", 
                query=stock_query, 
                force=force,
                expected_columns=stocks_columns
            )
        
        if result:
            # Verify the loaded data
            verification = verify_table_data(dd_con, "stocks", stocks_columns)
            if verification.get("error"):
                logger.error(f"Data verification failed: {verification}")
                return False
            
            logger.info(f"Stocks data load completed: {verification}")
            return True
            
        logger.error("Stocks data load failed")
        return False
        
    except Exception as e:
        logger.error(f"Error loading stocks data: {str(e)}")
        return False


def load_indices(force: bool = False, db_path: Optional[str] = None, dd_con: Optional[duckdb.DuckDBPyConnection] = None) -> bool:
    """Load indices data into DuckDB with verification."""
    logger = get_logger("reference_data_loader")
    try:
        if dd_con is None:
            with get_duckdb_connection(db_path) as dd_con:
                result = handle_table_data(
                    table_name="indices", 
                    query=indices_query, 
                    force=force,
                    expected_columns=indices_columns
                )
        else:
            result = handle_table_data(
                table_name="indices", 
                query=indices_query, 
                force=force,
                expected_columns=indices_columns
            )
        
        if result:
            # Verify the loaded data
            verification = verify_table_data(dd_con, "indices", indices_columns)
            if verification.get("error"):
                logger.error(f"Data verification failed: {verification}")
                return False
            
            logger.info(f"Indices data load completed: {verification}")
            return True
            
        logger.error("Indices data load failed")
        return False
        
    except Exception as e:
        logger.error(f"Error loading indices data: {str(e)}")
        return False


def load_indices_stocks(force: bool = False, db_path: Optional[str] = None, dd_con: Optional[duckdb.DuckDBPyConnection] = None) -> bool:
    """Load indices-stocks data into DuckDB with verification."""
    logger = get_logger("reference_data_loader")
    try:
        if dd_con is None:
            with get_duckdb_connection(db_path) as dd_con:
                result = handle_table_data(
                    table_name="indices_stocks", 
                    query=indices_stocks_query, 
                    force=force,
                    expected_columns=indices_stocks_columns
                )
        else:
            result = handle_table_data(
                table_name="indices_stocks", 
                query=indices_stocks_query, 
                force=force,
                expected_columns=indices_stocks_columns
            )
        
        if result:
            # Verify the loaded data
            verification = verify_table_data(dd_con, "indices_stocks", indices_stocks_columns)
            if verification.get("error"):
                logger.error(f"Data verification failed: {verification}")
                return False
            
            logger.info(f"Indices stocks data load completed: {verification}")
            return True
            
        logger.error("Indices stocks data load failed")
        return False
        
    except Exception as e:
        logger.error(f"Error loading indices stocks data: {str(e)}")
        return False

def load_market_metadata(force: bool = False, db_path: Optional[str] = None, dd_con: Optional[duckdb.DuckDBPyConnection] = None) -> bool:
    """Load market metadata from ClickHouse into DuckDB with verification."""
    logger = get_logger("reference_data_loader")
    try:
        if dd_con is None:
            with get_duckdb_connection(db_path) as dd_con:
                result = handle_table_data(
                    table_name="market_metadata", 
                    query=metadata_query, 
                    force=force,
                    expected_columns=market_metadata_columns
                )
        else:
            result = handle_table_data(
                table_name="market_metadata", 
                query=metadata_query, 
                force=force,
                expected_columns=market_metadata_columns
            )
        
        if result:
            # Verify the loaded data
            verification = verify_table_data(dd_con, "market_metadata", market_metadata_columns)
            if verification.get("error"):
                logger.error(f"Data verification failed: {verification}")
                return False
            
            logger.info(f"Market metadata load completed: {verification}")
            return True
            
        logger.error("Market metadata load failed")
        return False
        
    except Exception as e:
        logger.error(f"Error loading market metadata: {str(e)}")
        return False

def add_indices_names_to_companies(dd_con):
    """
    Get unique names and slugs from indices table and append them to companies table
    
    Args:
        dd_con: DuckDB connection
    
    Returns:
        bool: Success status
    """
    try:
            
        # Get unique names and slugs from indices that don't exist in companies
        unique_indices = dd_con.execute("""
            SELECT DISTINCT i.name, i.slug 
            FROM public.indices i
            WHERE NOT EXISTS (
                SELECT 1 FROM public.companies c
                WHERE c.name = i.name
            )
        """).fetchall()
        
        if unique_indices:
            # Insert the unique indices names into companies one by one
            for row in unique_indices:
                dd_con.execute("""
                    INSERT INTO public.companies (name, slug)
                    VALUES (?, ?)
                """, row)
            
            logger.info(f"Added {len(unique_indices)} unique indices names to companies table")
        else:
            logger.info("No new unique indices names to add to companies table")
        
        return True
    except Exception as e:
        logger.error(f"Error adding indices names to companies: {str(e)}")
        return False

def add_slug_columns_to_tables(dd_con):
    """Add slug columns to all relevant tables after they've been loaded"""
    try:
        # Log all tables in public schema
        tables = dd_con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'").fetchall()
        logger.info(f"Tables in public schema: {[t[0] for t in tables]}")
        # Create the UDF only if it doesn't already exist
        try:
            dd_con.execute("SELECT generate_slug('test')")
        except:
            # Only create the function if it doesn't exist
            dd_con.create_function("generate_slug", generate_slug, ["VARCHAR"], "VARCHAR")
        # Add slug to companies table
        companies_result = add_slug_column_to_table(dd_con, "companies", "name")
        # Add slug to indices table 
        indices_result = add_slug_column_to_table(dd_con, "indices", "name")
        # Add slug to market_metadata table using company_name
        metadata_result = add_slug_column_to_table(dd_con, "market_metadata", "company_name")
        all_success = all([companies_result, indices_result, metadata_result])
        logger.info(f"Adding slug columns to tables {'completed successfully' if all_success else 'had some failures'}")
        return all_success
    except Exception as e:
        logger.error(f"Error adding slug columns to tables: {str(e)}")
        return False


def load_all_reference_data(force: bool = False, db_path: Optional[str] = None) -> bool:
    """Load all reference data into DuckDB using a single direct connection with retries and verification."""
    import duckdb
    logger = get_logger("reference_data_loader")
    try:
        logger.info("Loading all reference data with a single direct connection...")
        if db_path is None:
            project_root = Path(__file__).resolve().parents[3]
            db_path = os.path.join(project_root, "data", "pydb.duckdb")
        
        conn = duckdb.connect(db_path)
        logger.info(f"Using direct DuckDB connection id: {id(conn)} and path: {db_path}")
        
        # Load each table with retry mechanism
        load_functions = [
            (load_companies, "companies"),
            (load_stocks, "stocks"),
            (load_indices, "indices"),
            (load_indices_stocks, "indices_stocks"),
            (load_market_metadata, "market_metadata")
        ]
        
        for load_func, table_name in load_functions:
            logger.info(f"Loading {table_name}...")
            if not load_with_retry(load_func, force, db_path, conn):
                logger.error(f"Failed to load {table_name} after retries")
                conn.close()
                return False
        
        logger.info("Committing all changes before adding slug columns...")
        conn.commit()
        
        if not add_slug_columns_to_tables(conn):
            logger.error("Failed to add slug columns to tables")
            conn.close()
            return False
        
        logger.info("All reference data loaded successfully")
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error loading reference data: {str(e)}")
        return False
