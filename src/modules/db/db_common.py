import polars as pl
import duckdb
import os
import logging
from datetime import datetime, timezone
from src.db.clickhouse import pool as ClickHousePool
import time



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


def create_duckdb_table(query: str, table_name: str, conn, connection, column_list=None):
    try:
        # Drop the table if it exists
        logging.info(f"Attempting to drop table public.{table_name} if it exists")
        conn.execute(f"DROP TABLE IF EXISTS public.{table_name};")
        
        # Read data from ClickHouse
        logging.info(f"Reading data from ClickHouse for table {table_name}")
        
        # Add retry mechanism for ClickHouse queries
        max_retries = 3
        retry_delay = 5
        current_retry = 0
        
        while current_retry < max_retries:
            try:
                logging.info(f"Attempt {current_retry + 1} of {max_retries} to query ClickHouse for {table_name}")

                result = ClickHousePool.execute_query(query)
                
                # Create DataFrame with explicit orientation to avoid warning
                if column_list:
                    df = pl.DataFrame(result, schema=column_list, orient="row")
                else:
                    # Get column names from the query result
                    # Execute a LIMIT 0 query to get just the column names
                    sample_query = f"SELECT * FROM ({query}) AS sample LIMIT 0"
                    sample_result = ClickHousePool.execute_query(sample_query)

                    df = pl.DataFrame(result, schema=column_list, orient="row")
                
                # If we get here, the query succeeded
                break
            except Exception as e:
                current_retry += 1
                if "timed out" in str(e).lower() and current_retry < max_retries:
                    logging.warning(f"ClickHouse query timed out. Retry attempt {current_retry} of {max_retries} in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    # Increase retry delay for subsequent attempts
                    retry_delay *= 2
                else:
                    if current_retry == max_retries:
                        logging.error(f"Failed to query data after {max_retries} attempts")
                    raise
            
        logging.info(f"Retrieved {len(df)} rows from ClickHouse")
        df_arrow = df.to_arrow()
        
        # Use explicit column list if provided, otherwise get from DataFrame
        if column_list:
            logging.info(f"Using provided column list for table {table_name}: {column_list}")
            column_names = column_list
        else:
            # Get column names from the DataFrame
            column_names = df.columns
            logging.info(f"Using DataFrame columns for table {table_name}: {column_names}")
        
        # Create table with explicit column names
        logging.info(f"Creating table public.{table_name}")
        conn.execute(
            f"create table public.{table_name} as select {', '.join(column_names)} from df_arrow;"
        )
        
        # Verify table was created
        table_exists = conn.execute(f"SELECT COUNT(*) FROM public.{table_name}").fetchone()[0]
        logging.info(f"Successfully created/updated table: {table_name} with {table_exists} rows")
        
        # Store the latest timestamp in DuckDB synced_tables
        store_table_timestamp(conn, table_name, df)
            
        return True
    except Exception as e:
        logging.error(f"Error creating table {table_name}: {str(e)}")
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


def handle_table_data(dd_con, connection, table_name, query, force=False, column_list=None):
    """Generic handler for table data with incremental updates where possible"""
    try:
        # Check if table exists
        table_exists = dd_con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table_name}'").fetchone()[0]
        
        if not table_exists:
            # If table doesn't exist, create it with full data load
            logging.info(f"{table_name} table doesn't exist, creating with full load")
            create_duckdb_table(query, table_name, dd_con, connection, column_list=column_list)
            return True
            
        # Check if we need to do a full refresh
        if should_refresh_table(dd_con, table_name, force):
            # Check if this table requires full refresh (has no timestamp columns)
            result = dd_con.execute("""
                SELECT full_refresh_required 
                FROM public.synced_tables 
                WHERE table_name = ?
            """, [table_name]).fetchone()
            
            if result and result[0]:
                # This table has no timestamp columns, do a full refresh
                logging.info(f"{table_name} has no timestamp columns, performing full refresh")
                create_duckdb_table(query, table_name, dd_con, connection, column_list=column_list)
                return True
                
            # Get last updated timestamp from DuckDB
            last_updated = get_last_sync_timestamp(dd_con, table_name)

            if not last_updated:
                # No timestamp recorded, do a full refresh
                logging.info(f"No timestamp found for {table_name}, performing full refresh")
                create_duckdb_table(query, table_name, dd_con, connection, column_list=column_list)
                return True
                
            # Sample the table to check for timestamp columns
            # Use ClickHouse connection for sample
            sample_query = f"SELECT * FROM ({query}) AS sample LIMIT 1"
            sample_result = ClickHousePool.execute_query(sample_query)
            
            sample_df = pl.DataFrame(sample_result, schema=column_list, orient="row")
            
            # Check for timestamp columns
            has_created_at = 'created_at' in sample_df.columns
            has_updated_at = 'updated_at' in sample_df.columns
            
            if not (has_created_at or has_updated_at):
                # No timestamp columns, do a full refresh
                logging.info(f"{table_name} has no timestamp columns, performing full refresh")
                create_duckdb_table(query, table_name, dd_con, connection, column_list=column_list)
                # Mark this table as requiring full refresh in the future
                update_sync_timestamp(dd_con, table_name, datetime.now(timezone.utc), True)
                return True
                
            # Create incremental update query based on available timestamp columns
            # Ensure last_updated is in UTC and format appropriately for the query
            last_updated_utc = last_updated.astimezone(timezone.utc)
            
            # Format timestamp for database query (without timezone info)
            last_updated_str = last_updated_utc.strftime("%Y-%m-%d %H:%M:%S")
            
            # ClickHouse SQL syntax for incremental updates
            if has_created_at and has_updated_at:
                # Use either created_at or updated_at, whichever is more recent
                incremental_query = f"""
                    SELECT * FROM ({query}) AS source
                    WHERE created_at > toDateTime('{last_updated_str}')
                    OR updated_at > toDateTime('{last_updated_str}')
                """
            elif has_created_at:
                incremental_query = f"""
                    SELECT * FROM ({query}) AS source
                    WHERE created_at > toDateTime('{last_updated_str}')
                """
            else:  # has_updated_at
                incremental_query = f"""
                    SELECT * FROM ({query}) AS source
                    WHERE updated_at > toDateTime('{last_updated_str}')
                """
            
            # Get new data from ClickHouse
            result = ClickHousePool.execute_query(incremental_query)
            
            df = pl.DataFrame(result, schema=column_list, orient="row")
            
            if len(df) > 0:
                logging.info(f"Found {len(df)} new records for {table_name} since {last_updated}")
                
                # Convert to arrow for duckdb
                df_arrow = df.to_arrow()
                
                # Get primary key columns to use for the NOT EXISTS clause
                # For simplicity, using a common set for known tables or falling back to all columns
                primary_key_mapping = {
                    "companies": ["id"],
                    "stocks": ["company_id", "security_code"],
                    "indices": ["id"],
                    "indices_stocks": ["security_code"],
                    "market_metadata": ["security_code"]
                }
                
                pk_columns = primary_key_mapping.get(table_name, df.columns[:1])  # Default to first column if not in mapping
                
                # Build the NOT EXISTS condition
                not_exists_conditions = " AND ".join([f"t.{col} = df_arrow.{col}" for col in pk_columns])
                
                # Insert the new records
                dd_con.execute(f"""
                    INSERT INTO public.{table_name} 
                    SELECT * FROM df_arrow
                    WHERE NOT EXISTS (
                        SELECT 1 FROM public.{table_name} t
                        WHERE {not_exists_conditions}
                    )
                """)
                
                # Update the timestamp in DuckDB
                store_table_timestamp(dd_con, table_name, df)
                
                logging.info(f"Successfully added {len(df)} new records to {table_name} table")
            else:
                logging.info(f"No new data found for {table_name}")
                
                # Update timestamp to current time in UTC
                current_time = datetime.now(timezone.utc)
                update_sync_timestamp(dd_con, table_name, current_time)
        
        return True
    except Exception as e:
        logging.error(f"Error handling {table_name} data: {str(e)}")
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