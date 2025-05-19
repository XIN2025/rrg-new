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
    """Load market metadata into DuckDB"""
    try:
        if dd_con is None:
            dd_con = ensure_duckdb_schema()
            
        # Check if the market_metadata table exists
        table_exists = dd_con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'market_metadata'").fetchone()[0]
        
        # Check if we need to refresh the table
        refresh_needed = force or not table_exists or should_refresh_table(dd_con, "market_metadata", force)
        
        if not refresh_needed:
            # Table exists and doesn't need refresh
            logging.info(f"Table market_metadata exists and is up to date. Skipping...")
            return True
            
        # Create a fresh table
        dd_con.execute("DROP TABLE IF EXISTS public.market_metadata")
        dd_con.execute("""
            CREATE TABLE public.market_metadata (
                name VARCHAR,
                security_code VARCHAR,
                ticker VARCHAR,
                symbol VARCHAR,
                security_type_code VARCHAR,
                company_code VARCHAR,
                meaningful_name VARCHAR,
                slug VARCHAR
            )
        """)
        
        # Execute both queries and combine the results
        ch_pool = ClickHousePool
        
        # First query
        result_rows1 = ch_pool.execute_query(metadata_query)
        if result_rows1:
            # We know what the columns should be based on the query
            column_names = ["name", "security_code", "ticker", "symbol", "security_type_code", "company_code", "meaningful_name"]
            
            # Convert to DataFrame
            df1 = pl.DataFrame(result_rows1, schema=column_names, orient="row")
            
            # Add slugs
            df1 = df1.with_columns(
                pl.col('name').str.to_lowercase().str.replace(" ", "-").alias('slug')
            )
        else:
            df1 = pl.DataFrame(schema=market_metadata_columns + ["slug"])
        
        # Second query
        result_rows2 = ch_pool.execute_query(metadata_query2)
        if result_rows2:
            # These are the same column names
            column_names = ["name", "security_code", "ticker", "symbol", "security_type_code", "company_code", "meaningful_name"]
            
            # Convert to DataFrame
            df2 = pl.DataFrame(result_rows2, schema=column_names, orient="row")
            
            # Add slugs
            df2 = df2.with_columns(
                pl.col('name').str.to_lowercase().str.replace(" ", "-").alias('slug')
            )
        else:
            df2 = pl.DataFrame(schema=market_metadata_columns + ["slug"])
        
        # Combine results
        df = pl.concat([df1, df2])
        
        # Remove duplicates by security_code, prioritizing non-empty values
        if len(df) > 0:
            # First remove obvious duplicates
            df = df.unique(subset=["security_code"])
            
            # Convert to Arrow for DuckDB
            df_arrow = df.to_arrow()
            
            # Insert into DuckDB
            insert_query = """
                INSERT INTO public.market_metadata
                SELECT * FROM df_arrow
            """
            dd_con.execute(insert_query)
            
            # Store timestamp for this table
            current_time = datetime.now(timezone.utc)
            update_sync_timestamp(dd_con, "market_metadata", current_time, full_refresh_required=True)
            
            logging.info(f"Market metadata loaded with {len(df)} records")
        else:
            logging.warning("No market metadata data found")
        
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


def load_all_reference_data(force=False):
    """Load all reference data tables in one operation"""
    try:
        dd_con = ensure_duckdb_schema()
        
        # Create the UDF only once for the entire process
        try:
            dd_con.execute("SELECT generate_slug('test')")
        except:
            # Only create the function if it doesn't exist
            dd_con.create_function("generate_slug", generate_slug, ["VARCHAR"], "VARCHAR")
        
        # First load indices
        indices_result = load_indices(dd_con, force)
        logging.info(f"Indices data load {'completed successfully' if indices_result else 'had some failures'}")
        
        # Add slug columns to indices table - don't create the function again
        indices_slug_result = add_slug_column_to_table(dd_con, "indices", "name")
        
        # Then load companies
        companies_result = load_companies(dd_con, force)
        logging.info(f"Companies data load {'completed successfully' if companies_result else 'had some failures'}")
        
        # Add slug columns to companies table
        companies_slug_result = add_slug_column_to_table(dd_con, "companies", "name")
        
        # Append unique indices names to companies
        indices_append_result = add_indices_names_to_companies(dd_con)
        logging.info(f"Adding indices names to companies {'completed successfully' if indices_append_result else 'had some failures'}")
        
        # Load remaining reference data tables
        results = [
            indices_result,
            companies_result,
            load_stocks(dd_con, force),
            load_indices_stocks(dd_con, force),
            load_market_metadata(dd_con, force)
        ]
        
        # Check if all operations were successful
        all_success = all(results)
        logging.info(f"All reference data load {'completed successfully' if all_success else 'had some failures'}")
        
        # Add slug columns to other tables
        slug_success = add_slug_columns_to_tables(dd_con)
        logging.info(f"Slug columns addition {'completed successfully' if slug_success else 'had some failures'}")
        
        return all_success and slug_success
    except Exception as e:
        logging.error(f"Error loading all reference data: {str(e)}")
        return False
    finally:
        if 'dd_con' in locals():
            dd_con.close() 
