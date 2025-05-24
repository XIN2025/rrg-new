import polars as pl
import duckdb
import os
import logging
from datetime import datetime, timezone
from src.db.clickhouse import pool as ClickHousePool
import time
from typing import Optional
import pandas as pd
from src.utils.logger import get_logger
from src.utils.duck_pool import get_duckdb_connection

# Initialize logger
logger = get_logger("db_common")


def ensure_synced_table_exists(conn):
    """Ensure the synced_tables table exists in DuckDB to track last sync times"""
    try:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS public.synced_tables (
            table_name VARCHAR PRIMARY KEY,
            updated_at TIMESTAMP WITH TIME ZONE,
            full_refresh_required BOOLEAN
        )
        """)
        logging.info("Ensured synced_tables table exists")
        return True
    except Exception as e:
        logging.error(f"Error creating synced_tables: {str(e)}")
        return False


def get_last_sync_timestamp(conn, table_name):
    """Get the last sync timestamp for a table from DuckDB"""
    try:
        # Check if table exists in the synced_tables
        result = conn.execute("""
            SELECT updated_at 
            FROM public.synced_tables 
            WHERE table_name = ?
        """, [table_name]).fetchone()
        
        if result and result[0]:
            return result[0]
        return None
    except Exception as e:
        logging.error(f"Error getting last sync timestamp for {table_name}: {str(e)}")
        return None


def update_sync_timestamp(conn, table_name, timestamp, full_refresh_required=False):
    """Update the sync timestamp for a table in DuckDB"""
    try:
        # Ensure timestamp is in UTC
        if isinstance(timestamp, datetime):
            # Convert to UTC if it has timezone info, otherwise add UTC timezone
            timestamp_utc = timestamp.astimezone(timezone.utc) if timestamp.tzinfo is not None else timestamp.replace(tzinfo=timezone.utc)
        else:
            # If it's a string or something else, try to convert to UTC datetime
            logging.warning(f"Non-datetime timestamp for {table_name}: {timestamp}. Converting to UTC datetime.")
            try:
                # Assume it's a string timestamp
                timestamp_utc = datetime.fromisoformat(str(timestamp).replace('Z', '+00:00')).astimezone(timezone.utc)
            except Exception as e:
                logging.error(f"Cannot convert timestamp {timestamp} to datetime: {str(e)}")
                timestamp_utc = datetime.now(timezone.utc)
            
        # Insert or update the timestamp
        conn.execute("""
            INSERT INTO public.synced_tables (table_name, updated_at, full_refresh_required)
            VALUES (?, ?, ?)
            ON CONFLICT (table_name) 
            DO UPDATE SET updated_at = EXCLUDED.updated_at, full_refresh_required = EXCLUDED.full_refresh_required
        """, [table_name, timestamp_utc, full_refresh_required])
        
        logging.info(f"Updated sync timestamp for {table_name} to {timestamp_utc}")
        return True
    except Exception as e:
        logging.error(f"Error updating sync timestamp for {table_name}: {str(e)}")
        return False


def create_duckdb_table(query: str, table_name: str, conn: duckdb.DuckDBPyConnection, expected_columns: list = None) -> bool:
    """
    Create a DuckDB table from a ClickHouse query.
    
    Args:
        query: Query to execute
        table_name: Name of the table to create
        conn: DuckDB connection
        expected_columns: List of expected column names (optional)
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Create public schema if it doesn't exist
        conn.execute("CREATE SCHEMA IF NOT EXISTS public")
        
        # Drop existing table if it exists
        conn.execute(f"DROP TABLE IF EXISTS public.{table_name}")
        
        # Get data from ClickHouse
        data = ClickHousePool.execute_query(query)
        if not data:
            logger.warning(f"⚠️ No data returned for {table_name}")
            return False
            
        # Create DataFrame and set columns if needed
        df = pd.DataFrame(data)
        if df.empty:
            logger.warning(f"⚠️ Empty DataFrame for {table_name}")
            return False
        if expected_columns and len(df.columns) == len(expected_columns):
            df.columns = expected_columns
            
        # Create table from DataFrame in public schema
        conn.execute(f"CREATE TABLE public.{table_name} AS SELECT * FROM df")
        logger.info(f"✅ Created table public.{table_name} with {len(df)} rows")
        return True
        
    except Exception as e:
        logger.error(f"🚨 Error creating table public.{table_name}: {str(e)}")
        return False


def store_table_timestamp(conn, table_name, df):
    """Store the latest timestamp from the dataframe in DuckDB synced_tables"""
    try:
        # Default values
        max_timestamp = None
        full_refresh_required = False
        
        # Check for timestamp columns in order of preference
        if 'updated_at' in df.columns and 'created_at' in df.columns:
            # If both present, use the min of the max of each
            max_created = df.select(pl.col('created_at')).max().item()
            max_updated = df.select(pl.col('updated_at')).max().item()
            
            # Ensure both are datetime objects with UTC timezone
            if isinstance(max_created, str):
                max_created = datetime.fromisoformat(max_created.replace('Z', '+00:00')).astimezone(timezone.utc)
            elif isinstance(max_created, datetime):
                max_created = max_created.astimezone(timezone.utc) if max_created.tzinfo is not None else max_created.replace(tzinfo=timezone.utc)
                
            if isinstance(max_updated, str):
                max_updated = datetime.fromisoformat(max_updated.replace('Z', '+00:00')).astimezone(timezone.utc)
            elif isinstance(max_updated, datetime):
                max_updated = max_updated.astimezone(timezone.utc) if max_updated.tzinfo is not None else max_updated.replace(tzinfo=timezone.utc)
                
            # Get the min of the two max timestamps
            max_timestamp = min(max_created, max_updated)
            
        elif 'created_at' in df.columns:
            # Just use created_at
            max_timestamp = df.select(pl.col('created_at')).max().item()
            # Ensure it's a UTC datetime
            if isinstance(max_timestamp, str):
                max_timestamp = datetime.fromisoformat(max_timestamp.replace('Z', '+00:00')).astimezone(timezone.utc)
            elif isinstance(max_timestamp, datetime):
                max_timestamp = max_timestamp.astimezone(timezone.utc) if max_timestamp.tzinfo is not None else max_timestamp.replace(tzinfo=timezone.utc)
            
        elif 'updated_at' in df.columns:
            # Just use updated_at
            max_timestamp = df.select(pl.col('updated_at')).max().item()
            # Ensure it's a UTC datetime
            if isinstance(max_timestamp, str):
                max_timestamp = datetime.fromisoformat(max_timestamp.replace('Z', '+00:00')).astimezone(timezone.utc)
            elif isinstance(max_timestamp, datetime):
                max_timestamp = max_timestamp.astimezone(timezone.utc) if max_timestamp.tzinfo is not None else max_timestamp.replace(tzinfo=timezone.utc)
        else:
            # No timestamp columns, use current time in UTC
            max_timestamp = datetime.now(timezone.utc)
            full_refresh_required = True

        logging.info("+++++++++++++++++++++++++++++++++++")
        logging.info(f"max_timestamp for {table_name}: {max_timestamp}")
        logging.info("+++++++++++++++++++++++++++++++++++")
        
        # Update in DuckDB
        update_sync_timestamp(conn, table_name, max_timestamp, full_refresh_required)
        
    except Exception as e:
        logging.error(f"Error storing timestamp for {table_name}: {str(e)}")


def should_refresh_table(conn, table_name, force=False):
    """Check if a table should be refreshed based on DuckDB timestamp"""
    try:
        if force:
            logging.info(f"Force refresh requested for table {table_name}")
            return True
            
        # Get last updated timestamp from DuckDB
        last_updated = get_last_sync_timestamp(conn, table_name)
        
        # If no timestamp exists, refresh the table
        if not last_updated:
            logging.info(f"No timestamp found for {table_name}, will refresh")
            return True
            
        # Check if full refresh is required for this table
        result = conn.execute("""
            SELECT full_refresh_required 
            FROM public.synced_tables 
            WHERE table_name = ?
        """, [table_name]).fetchone()
        
        if result and result[0]:
            logging.info(f"Table {table_name} requires full refresh (no timestamp columns)")
            return True
        
        logging.info(f"Table {table_name} was updated at {last_updated}, checking for new data")
        return False
    except Exception as e:
        logging.error(f"Error checking refresh for {table_name}: {str(e)}")
        # Default to refreshing the table if there's an error
        return True


def handle_table_data(table_name: str, query: str, force: bool = False, timestamp_column: Optional[str] = None, expected_columns: Optional[list] = None) -> bool:
    """
    Handle table data loading with proper connection management.
    
    Args:
        table_name: Name of the table to handle
        query: Query to execute
        force: Whether to force a full refresh
        timestamp_column: Optional timestamp column for incremental updates
        expected_columns: List of expected column names (optional)
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Check if table exists using information_schema
        with get_duckdb_connection() as conn:
            table_exists = conn.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table_name}'").fetchone()[0] > 0
            if not table_exists or force:
                logger.info(f"🔄 {'Creating' if not table_exists else 'Refreshing'} table public.{table_name}")
                return create_duckdb_table(query, table_name, conn, expected_columns)
            if timestamp_column:
                # Get latest timestamp from DuckDB
                latest_ts = conn.execute(f"SELECT MAX({timestamp_column}) FROM public.{table_name}").fetchone()[0]
                if latest_ts:
                    # Add timestamp filter to query
                    query = f"{query} WHERE {timestamp_column} > '{latest_ts}'"
                    logger.info(f"📅 Incremental update for public.{table_name} from {latest_ts}")
                else:
                    logger.warning(f"⚠️ No timestamp found for public.{table_name}, performing full refresh")
                    return create_duckdb_table(query, table_name, conn, expected_columns)
            else:
                logger.info(f"ℹ️ No timestamp column specified for public.{table_name}, skipping incremental update")
                return True
            # Execute incremental update
            try:
                data = ClickHousePool.execute_query(query)
                if data:
                    df = pl.DataFrame(data, schema=column_list, orient="row")
                    if not df.empty:
                        conn.execute(f"INSERT INTO public.{table_name} SELECT * FROM df")
                        logger.info(f"✅ Incremental update completed for public.{table_name}")
                        return True
                    else:
                        logger.info(f"ℹ️ No new data for public.{table_name}")
                        return True
                else:
                    logger.info(f"ℹ️ No data returned for public.{table_name}")
                    return True
            except Exception as e:
                logger.error(f"🚨 Error during incremental update: {str(e)}")
                return False
                
    except Exception as e:
        logger.error(f"🚨 Error handling table public.{table_name}: {str(e)}")
        return False


def drop_table(table_name: str, conn):
    """Drop a table if it exists"""
    conn.execute(f"DROP TABLE IF EXISTS public.{table_name};")
    print(f"dropped {table_name}")
    return


def get_pg_connection():
    """Get a PostgreSQL connection string"""
    return f'postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}'


def ensure_duckdb_schema(db_path="data/pydb.duckdb"):
    """Ensure DuckDB schema exists and connection is ready"""
    dd_con = duckdb.connect(db_path)
    dd_con.execute("create schema if not exists public;")
    ensure_synced_table_exists(dd_con)
    return dd_con


def remove_old_data():
    """Remove old DuckDB files"""
    try:
        os.remove("data/pydb.duckdb")
        os.remove("data/pydb.duckdb.wal")
    except Exception as e:
        print(e.args)
        pass
    return 
