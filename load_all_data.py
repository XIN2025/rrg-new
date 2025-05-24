from src.utils.duck_pool import get_duckdb_connection
from src.utils.clickhouse_pool import pool
from src.modules.rrg.reference_data_loader import load_market_metadata
import polars as pl
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def verify_market_metadata():
    """Verify market metadata is properly loaded."""
    with get_duckdb_connection() as conn:
        # Check indices
        indices = conn.execute("""
            SELECT COUNT(*) as count, 
                   COUNT(CASE WHEN symbol = 'CNX500' THEN 1 END) as has_cnx500
            FROM public.market_metadata 
            WHERE security_type_code = 5
        """).fetchdf()
        print("\nIndices in market_metadata:")
        print(indices)
        
        # Check sample indices
        sample_indices = conn.execute("""
            SELECT symbol, ticker, security_code, security_type_code
            FROM public.market_metadata 
            WHERE security_type_code = 5
            LIMIT 5
        """).fetchdf()
        print("\nSample indices:")
        print(sample_indices)

def verify_price_data():
    """Verify price data is properly loaded."""
    with get_duckdb_connection() as conn:
        # Check price data
        price_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT ticker) as unique_tickers,
                MIN(created_at) as earliest_date,
                MAX(created_at) as latest_date
            FROM public.eod_stock_data
        """).fetchdf()
        print("\nPrice data statistics:")
        print(price_stats)
        
        # Check sample price data
        sample_prices = conn.execute("""
            SELECT 
                created_at,
                ticker,
                close_price,
                ratio,
                momentum,
                change_percentage,
                signal
            FROM public.eod_stock_data
            WHERE ticker = 'CNX500'
            ORDER BY created_at DESC
            LIMIT 5
        """).fetchdf()
        print("\nSample CNX500 price data:")
        print(sample_prices)

def verify_technical_indicators():
    """Verify technical indicators are properly calculated."""
    with get_duckdb_connection() as conn:
        # Check if all required indicators exist
        indicators = conn.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN ratio IS NOT NULL THEN 1 END) as has_ratio,
                COUNT(CASE WHEN momentum IS NOT NULL THEN 1 END) as has_momentum,
                COUNT(CASE WHEN change_percentage IS NOT NULL THEN 1 END) as has_change,
                COUNT(CASE WHEN signal IS NOT NULL THEN 1 END) as has_signal
            FROM public.eod_stock_data
        """).fetchdf()
        print("\nTechnical indicators statistics:")
        print(indicators)

def verify_data_completeness():
    """Verify data completeness for RRG response."""
    with get_duckdb_connection() as conn:
        # Check if we have all required data for the last 3 months
        three_months_ago = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        completeness = conn.execute(f"""
            WITH required_data AS (
                SELECT 
                    ticker,
                    COUNT(*) as days_count,
                    COUNT(CASE WHEN ratio IS NOT NULL THEN 1 END) as ratio_count,
                    COUNT(CASE WHEN momentum IS NOT NULL THEN 1 END) as momentum_count,
                    COUNT(CASE WHEN change_percentage IS NOT NULL THEN 1 END) as change_count,
                    COUNT(CASE WHEN signal IS NOT NULL THEN 1 END) as signal_count
                FROM public.eod_stock_data
                WHERE created_at >= '{three_months_ago}'
                GROUP BY ticker
            )
            SELECT 
                COUNT(*) as total_tickers,
                AVG(days_count) as avg_days_per_ticker,
                MIN(days_count) as min_days,
                MAX(days_count) as max_days
            FROM required_data
        """).fetchdf()
        print("\nData completeness for last 3 months:")
        print(completeness)

def main():
    """Main function to load and verify all data."""
    print("Starting comprehensive data load and verification...")
    
    # Load market metadata
    print("\nLoading market metadata...")
    load_market_metadata(force=True)
    
    # Verify all components
    print("\nVerifying market metadata...")
    verify_market_metadata()
    
    print("\nVerifying price data...")
    verify_price_data()
    
    print("\nVerifying technical indicators...")
    verify_technical_indicators()
    
    print("\nVerifying data completeness...")
    verify_data_completeness()
    
    print("\nData load and verification complete!")

if __name__ == "__main__":
    main() 
