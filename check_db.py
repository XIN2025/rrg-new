import duckdb
import pandas as pd
from pathlib import Path

def check_database():
    db_path = Path("data/pydb.duckdb")
    if not db_path.exists():
        print("Database file does not exist!")
        return

    try:
        # Connect to the database
        conn = duckdb.connect(str(db_path))
        
        # Get list of tables
        print("\nTables in database:")
        tables_df = conn.execute("SHOW TABLES").fetchdf()
        print(tables_df)
        
        if not tables_df.empty:
            # Check market_metadata table
            print("\nSample from market_metadata:")
            metadata_df = conn.execute("""
                SELECT created_at, ticker, close_price 
                FROM market_metadata 
                LIMIT 5
            """).fetchdf()
            print(metadata_df)
            
            # Check for indices
            print("\nIndices in market_metadata:")
            indices_df = conn.execute("""
                SELECT DISTINCT ticker 
                FROM market_metadata 
                WHERE ticker LIKE '%NIFTY%' OR ticker LIKE '%BANK%'
            """).fetchdf()
            print(indices_df)
            
            # Check eod_stock_data
            print("\nSample from eod_stock_data:")
            eod_df = conn.execute("""
                SELECT * FROM eod_stock_data 
                LIMIT 5
            """).fetchdf()
            print(eod_df)
        
        conn.close()
        
    except Exception as e:
        print(f"Error checking database: {str(e)}")

if __name__ == "__main__":
    check_database() 
