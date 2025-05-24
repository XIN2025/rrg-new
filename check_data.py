from src.utils.duck_pool import get_duckdb_connection
import pandas as pd

def check_duckdb_data():
    """Check data in DuckDB."""
    try:
        with get_duckdb_connection() as conn:
            # Check market metadata
            print("\nChecking market metadata:")
            metadata_query = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT security_code) as unique_securities,
                    COUNT(DISTINCT CASE WHEN security_type_code = 5 THEN symbol END) as indices_count,
                    COUNT(DISTINCT CASE WHEN security_type_code = 1 THEN symbol END) as stocks_count
                FROM public.market_metadata
            """
            metadata_stats = conn.execute(metadata_query).fetchdf()
            print("\nMarket Metadata Statistics:")
            print(metadata_stats)
            
            # Sample of indices
            indices_query = """
                SELECT symbol, ticker, security_code
                FROM public.market_metadata
                WHERE security_type_code = 5
                LIMIT 5
            """
            indices_sample = conn.execute(indices_query).fetchdf()
            print("\nSample Indices:")
            print(indices_sample)
            
            # Check if CNX500 exists
            cnx500_query = """
                SELECT *
                FROM public.market_metadata
                WHERE symbol = 'CNX500'
            """
            cnx500_data = conn.execute(cnx500_query).fetchdf()
            print("\nCNX500 Data:")
            print(cnx500_data)
            
            # Check price data
            print("\nChecking price data:")
            price_query = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT ticker) as unique_tickers,
                    MIN(created_at) as earliest_date,
                    MAX(created_at) as latest_date
                FROM public.eod_stock_data
            """
            price_stats = conn.execute(price_query).fetchdf()
            print("\nPrice Data Statistics:")
            print(price_stats)
            
            # Sample price data for CNX500
            cnx500_price_query = """
                SELECT created_at, close_price, ratio, momentum, change_percentage
                FROM public.eod_stock_data
                WHERE ticker = 'CNX500'
                ORDER BY created_at DESC
                LIMIT 5
            """
            cnx500_prices = conn.execute(cnx500_price_query).fetchdf()
            print("\nCNX500 Price Data Sample:")
            print(cnx500_prices)
            
    except Exception as e:
        print(f"Error checking data: {str(e)}")

if __name__ == "__main__":
    check_duckdb_data() 
