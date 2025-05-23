import logging
import duckdb
import polars as pl
from src.modules.db.db_common import (
    handle_table_data,
    get_pg_connection,
    ensure_duckdb_schema,
    ensure_synced_table_exists,
    should_refresh_table,
    get_last_sync_timestamp,
    update_sync_timestamp
)
from src.modules.db.slug_utils import generate_slug
from datetime import datetime, timezone
from src.db.clickhouse import pool as ClickHousePool
from src.utils.logger import get_logger

logger = get_logger("reference_data_loader")

# Define column lists for DuckDB tables
companies_columns = ["name"]
stocks_columns = ["company_name", "security_code"]
indices_columns = ["security_code", "name", "slug", "symbol"]
indices_stocks_columns = ["security_code"]
market_metadata_columns = ["name", "security_code","ticker", "symbol", "security_type_code", 
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

# Simplified column names in the query - removed trailing semicolons
metadata_query = """
SELECT
    cm.company_name as name,
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
            logging.warning(f"Cannot add slug to {table_name}: table does not exist")
            return False
            
        # Check if the name column exists
        result = dd_con.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table_name}' AND column_name = '{name_column}'").fetchone()[0]
        
        if not result:
            logging.warning(f"Cannot add slug to {table_name}: column '{name_column}' does not exist")
            return False
            
        # Check if slug column already exists
        slug_exists = dd_con.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table_name}' AND column_name = 'slug'").fetchone()[0]
        
        if slug_exists:
            # If slug column exists, drop it first to rebuild
            logging.info(f"Slug column already exists in {table_name}, recreating it")
            dd_con.execute(f"ALTER TABLE public.{table_name} DROP COLUMN IF EXISTS slug")
        
        # Add the slug column to the table
        dd_con.execute(f"""
            ALTER TABLE public.{table_name} ADD COLUMN slug VARCHAR;
            UPDATE public.{table_name} SET slug = generate_slug({name_column});
        """)
        
        logging.info(f"Successfully added slug column to {table_name}")
        return True
        
    except Exception as e:
        logging.error(f"Error adding slug column to {table_name}: {str(e)}")
        return False
        

def load_companies(dd_con=None, force=False):
    """Load company data into DuckDB"""
    try:
        if dd_con is None:
            dd_con = ensure_duckdb_schema()
            
        pg_connection = get_pg_connection()
        
        result = handle_table_data(
            dd_con, 
            pg_connection, 
            "companies", 
            company_query, 
            force=force, 
            column_list=companies_columns
        )
        
        logging.info(f"Companies data load {'completed successfully' if result else 'failed'}")
        return result
    except Exception as e:
        logging.error(f"Error loading companies data: {str(e)}")
        return False


def load_stocks(dd_con=None, force=False):
    """Load stocks data into DuckDB"""
    try:
        if dd_con is None:
            dd_con = ensure_duckdb_schema()
            
        pg_connection = get_pg_connection()
        
        result = handle_table_data(
            dd_con, 
            pg_connection, 
            "stocks", 
            stock_query, 
            force=force, 
            column_list=stocks_columns
        )
        
        logging.info(f"Stocks data load {'completed successfully' if result else 'failed'}")
        return result
    except Exception as e:
        logging.error(f"Error loading stocks data: {str(e)}")
        return False


def load_indices(dd_con=None, force=False):
    """Load indices data into DuckDB"""
    try:
        if dd_con is None:
            dd_con = ensure_duckdb_schema()
            
        pg_connection = get_pg_connection()
        
        result = handle_table_data(
            dd_con, 
            pg_connection, 
            "indices", 
            indices_query, 
            force=force, 
            column_list=indices_columns
        )
        
        logging.info(f"Indices data load {'completed successfully' if result else 'failed'}")
        return result
    except Exception as e:
        logging.error(f"Error loading indices data: {str(e)}")
        return False


def load_indices_stocks(dd_con=None, force=False):
    """Load indices-stocks data into DuckDB"""
    try:
        if dd_con is None:
            dd_con = ensure_duckdb_schema()
            
        pg_connection = get_pg_connection()
        
        result = handle_table_data(
            dd_con, 
            pg_connection, 
            "indices_stocks", 
            indices_stocks_query, 
            force=force, 
            column_list=indices_stocks_columns
        )
        
        logging.info(f"Indices stocks data load {'completed successfully' if result else 'failed'}")
        return result
    except Exception as e:
        logging.error(f"Error loading indices stocks data: {str(e)}")
        return False

def load_market_metadata(dd_con=None, force=False):
    """Load market metadata from ClickHouse into DuckDB"""
    try:
        if dd_con is None:
            dd_con = ensure_duckdb_schema()
            
        # Check if table exists and force refresh if needed
        table_exists = dd_con.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'market_metadata'
        """).fetchone()[0]
        
        if table_exists and not force:
            logging.info("Market metadata table exists and force=False, skipping")
            return True
            
        # Drop table if it exists
        dd_con.execute("DROP TABLE IF EXISTS public.market_metadata")
        
        # Create table with correct schema
        dd_con.execute("""
            CREATE TABLE public.market_metadata (
                security_token VARCHAR,
                security_code VARCHAR,
                company_name VARCHAR,
                symbol VARCHAR,
                alternate_symbol VARCHAR,
                nse_index_name VARCHAR,
                ticker VARCHAR,
                is_fno BOOLEAN,
                stocks_count INTEGER,
                stocks VARCHAR[],
                security_codes VARCHAR[],
                security_tokens VARCHAR[],
                series VARCHAR,
                category VARCHAR,
                exchange_group VARCHAR,
                security_type_code INTEGER,
                slug VARCHAR
            )
        """)
        
        # Load indices data
        indices_query = """
        SELECT 
            security_token,
            security_code,
            company_name,
            symbol,
            alternate_symbol,
            nse_index_name,
            ticker,
            is_fno,
            stocks_count,
            stocks,
            security_codes,
            security_tokens,
            NULL as series,
            NULL as category,
            NULL as exchange_group,
            5 as security_type_code
        FROM strike.mv_indices
        """
        
        indices_result = ClickHousePool.execute_query(indices_query)
        if indices_result:
            for row in indices_result:
                dd_con.execute("""
                    INSERT INTO public.market_metadata (
                        security_token, security_code, company_name, symbol, alternate_symbol, 
                        nse_index_name, ticker, is_fno, stocks_count, stocks, security_codes, 
                        security_tokens, series, category, exchange_group, security_type_code
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    row.get('security_token'),
                    row.get('security_code'),
                    row.get('company_name'),
                    row.get('symbol'),
                    row.get('alternate_symbol'),
                    row.get('nse_index_name'),
                    row.get('ticker'),
                    row.get('is_fno'),
                    row.get('stocks_count'),
                    row.get('stocks'),
                    row.get('security_codes'),
                    row.get('security_tokens'),
                    None, None, None, 5
                ])
            logging.info(f"Loaded {len(indices_result)} indices records")
        
        # Load stocks data
        stocks_query = """
        SELECT 
            security_token,
            security_code,
            company_name,
            symbol,
            alternate_symbol,
            NULL as nse_index_name,
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
        """
        
        stocks_result = ClickHousePool.execute_query(stocks_query)
        if stocks_result:
            for row in stocks_result:
                dd_con.execute("""
                    INSERT INTO public.market_metadata (
                        security_token, security_code, company_name, symbol, alternate_symbol, 
                        nse_index_name, ticker, is_fno, stocks_count, stocks, security_codes, 
                        security_tokens, series, category, exchange_group, security_type_code
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    row.get('security_token'),
                    row.get('security_code'),
                    row.get('company_name'),
                    row.get('symbol'),
                    row.get('alternate_symbol'),
                    None,
                    row.get('ticker'),
                    row.get('is_fno'),
                    None,
                    None,
                    None,
                    None,
                    row.get('series'),
                    row.get('category'),
                    row.get('exchange_group'),
                    26
                ])
            logging.info(f"Loaded {len(stocks_result)} stocks records")
        
        # Add slug column
        dd_con.execute("""
            UPDATE public.market_metadata 
            SET slug = regexp_replace(lower(company_name), '[^a-z0-9]+', '-', 'g')
        """)
        
        return True
    except Exception as e:
        logging.error(f"Error loading market metadata: {str(e)}")
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
            
            logging.info(f"Added {len(unique_indices)} unique indices names to companies table")
        else:
            logging.info("No new unique indices names to add to companies table")
        
        return True
    except Exception as e:
        logging.error(f"Error adding indices names to companies: {str(e)}")
        return False

def add_slug_columns_to_tables(dd_con):
    """Add slug columns to relevant tables"""
    try:
        # Add slug column to market_metadata if it doesn't exist
        dd_con.execute("""
            ALTER TABLE public.market_metadata 
            ADD COLUMN IF NOT EXISTS slug VARCHAR
        """)
        
        # Update slug values
        dd_con.execute("""
            UPDATE public.market_metadata
            SET slug = LOWER(REGEXP_REPLACE(company_name, '[^a-zA-Z0-9]', '-'))
            WHERE slug IS NULL
        """)
        
        return True
    except Exception as e:
        logger.error(f"Error adding slug columns: {str(e)}")
        return False


def load_all_reference_data(dd_con, force=False):
    """Load all reference data into DuckDB"""
    try:
        # Load market metadata
        load_market_metadata(dd_con, force)
        
        # Load other reference data
        load_companies(dd_con, force)
        load_stocks(dd_con, force)
        load_indices(dd_con, force)
        load_indices_stocks(dd_con, force)
        
        # Add slug columns
        add_slug_columns_to_tables(dd_con)
        
        return True
    except Exception as e:
        logger.error(f"Error loading reference data: {str(e)}")
        return False

def add_slug_columns_to_tables(dd_con):
    """Add slug columns to all relevant tables after they've been loaded"""
    try:
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
        
        # Add slug to market_metadata table
        metadata_result = add_slug_column_to_table(dd_con, "market_metadata", "name")
        
        all_success = all([companies_result, indices_result, metadata_result])
        logging.info(f"Adding slug columns to tables {'completed successfully' if all_success else 'had some failures'}")
        
        return all_success
    except Exception as e:
        logging.error(f"Error adding slug columns to tables: {str(e)}")
        return False
    finally:
        if 'dd_con' in locals():
            dd_con.close() 
