from src.utils.duck_pool import get_duckdb_connection

def check_duckdb_tables():
    """Check all tables and their structure in DuckDB."""
    try:
        with get_duckdb_connection() as conn:
            # List all schemas
            print("\n=== Schemas ===")
            schemas = conn.execute("""
                SELECT schema_name 
                FROM information_schema.schemata
            """).fetchdf()
            print(schemas)
            
            # List all tables
            print("\n=== Tables ===")
            tables = conn.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            """).fetchdf()
            print(tables)
            
            # Check market_metadata table structure
            print("\n=== market_metadata Structure ===")
            try:
                metadata_columns = conn.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = 'market_metadata'
                """).fetchdf()
                print(metadata_columns)
                
                # Sample data
                print("\n=== market_metadata Sample Data ===")
                sample = conn.execute("""
                    SELECT * FROM public.market_metadata LIMIT 5
                """).fetchdf()
                print(sample)
            except Exception as e:
                print(f"Error checking market_metadata: {str(e)}")
            
            # Check eod_stock_data table structure
            print("\n=== eod_stock_data Structure ===")
            try:
                stock_columns = conn.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = 'eod_stock_data'
                """).fetchdf()
                print(stock_columns)
                
                # Sample data
                print("\n=== eod_stock_data Sample Data ===")
                sample = conn.execute("""
                    SELECT * FROM public.eod_stock_data LIMIT 5
                """).fetchdf()
                print(sample)
            except Exception as e:
                print(f"Error checking eod_stock_data: {str(e)}")
            
    except Exception as e:
        print(f"Error checking DuckDB: {str(e)}")

if __name__ == "__main__":
    check_duckdb_tables() 
