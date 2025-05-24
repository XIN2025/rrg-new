from src.utils.duck_pool import get_duckdb_connection

print("Checking DuckDB schema...")

with get_duckdb_connection() as conn:
    # List all tables
    tables = conn.execute("SELECT * FROM sqlite_master WHERE type='table'").fetchdf()
    print("\nTables in DuckDB:")
    print(tables)
    
    # Try to access the market_metadata table directly
    try:
        print("\nTrying to access market_metadata table:")
        result = conn.execute("SELECT * FROM market_metadata LIMIT 5").fetchdf()
        print(result)
    except Exception as e:
        print(f"Error accessing market_metadata: {e}")
    
    # Try to access the public.market_metadata table
    try:
        print("\nTrying to access public.market_metadata table:")
        result = conn.execute("SELECT * FROM public.market_metadata LIMIT 5").fetchdf()
        print(result)
    except Exception as e:
        print(f"Error accessing public.market_metadata: {e}")
