from src.utils.duck_pool import get_duckdb_connection
import pandas as pd

print("Checking DuckDB tables...")

with get_duckdb_connection() as conn:
    # List all tables
    tables = conn.execute("SELECT * FROM sqlite_master WHERE type='table'").fetchdf()
    print("Tables in DuckDB:")
    print(tables)

    # Check and print public.market_metadata
    if 'market_metadata' in tables['name'].values:
        print("\nFirst 100 rows from public.market_metadata:")
        df = conn.execute("SELECT * FROM public.market_metadata LIMIT 100").fetchdf()
        print(df)
        print("\nTotal rows in market_metadata:")
        count = conn.execute("SELECT COUNT(*) as count FROM public.market_metadata").fetchdf()
        print(count)
    else:
        print("\nTable public.market_metadata does not exist.")

    # Check and print public.stock_prices
    if 'stock_prices' in tables['name'].values:
        print("\nFirst 100 rows from public.stock_prices:")
        df = conn.execute("SELECT * FROM public.stock_prices LIMIT 100").fetchdf()
        print(df)
        print("\nTotal rows in stock_prices:")
        count = conn.execute("SELECT COUNT(*) as count FROM public.stock_prices").fetchdf()
        print(count)
    else:
        print("\nTable public.stock_prices does not exist.")

    # Check and print public.eod_stock_data
    if 'eod_stock_data' in tables['name'].values:
        print("\nFirst 100 rows from public.eod_stock_data:")
        df = conn.execute("SELECT * FROM public.eod_stock_data LIMIT 100").fetchdf()
        print(df)
        print("\nTotal rows in eod_stock_data:")
        count = conn.execute("SELECT COUNT(*) as count FROM public.eod_stock_data").fetchdf()
        print(count)
    else:
        print("\nTable public.eod_stock_data does not exist.")
