import os
import sys

# Add the project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.append(project_root)

from src.utils.duck_pool import get_duckdb_connection

if __name__ == "__main__":
    with get_duckdb_connection() as conn:
        print("First 10 rows:")
        rows = conn.execute("SELECT * FROM public.market_metadata LIMIT 10").fetchall()
        for row in rows:
            print(row)
        print("\nDistinct security_type_code values:")
        codes = conn.execute("SELECT DISTINCT security_type_code FROM public.market_metadata").fetchall()
        print([c[0] for c in codes]) 
