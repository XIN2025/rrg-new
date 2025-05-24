from src.utils.duck_pool import get_duckdb_connection
import sys

def test_metadata():
    try:
        with get_duckdb_connection('data/pydb.duckdb') as conn:
            # Check if tables exist
            tables = conn.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """).fetchdf()
            print('Available tables:')
            print(tables)
            
            # Test indices table
            indices = conn.execute("""
                SELECT * FROM public.indices LIMIT 5
            """).fetchdf()
            print('\nSample indices:')
            print(indices)
            
            # Test stocks table
            stocks = conn.execute("""
                SELECT * FROM public.stocks LIMIT 5
            """).fetchdf()
            print('\nSample stocks:')
            print(stocks)
            
            # Test indices_stocks table
            index_stocks = conn.execute("""
                SELECT * FROM public.indices_stocks LIMIT 5
            """).fetchdf()
            print('\nSample index_stocks:')
            print(index_stocks)
            
            return True
    except Exception as e:
        print(f"Error testing metadata: {str(e)}")
        return False

def test_data():
    with get_duckdb_connection('data/pydb.duckdb') as conn:
        # Test index stocks
        index_count = conn.execute("""
            SELECT COUNT(*) 
            FROM public.indices_stocks 
            WHERE index_symbol = 'NIFTY 50'
        """).fetchone()[0]
        print(f'Index stocks count: {index_count}')
        
        # Test sample stocks
        sample_stocks = conn.execute("""
            SELECT DISTINCT s.security_code, s.ticker
            FROM public.indices_stocks is
            JOIN public.stocks s ON is.security_code = s.security_code
            WHERE is.index_symbol = 'NIFTY 50'
            LIMIT 5
        """).fetchdf()
        print('\nSample stocks:')
        print(sample_stocks)
        
        # Test EOD data
        eod_count = conn.execute("""
            SELECT COUNT(*) 
            FROM public.eod_stock_data
        """).fetchone()[0]
        print(f'\nEOD data count: {eod_count}')
        
        # Test sample EOD data
        sample_eod = conn.execute("""
            SELECT created_at, ticker, close_price
            FROM public.eod_stock_data
            LIMIT 5
        """).fetchdf()
        print('\nSample EOD data:')
        print(sample_eod)

if __name__ == '__main__':
    print("Testing metadata tables first...")
    if test_metadata():
        print("\nMetadata tables look good!")
        print("\nWould you like to test the full data? (y/n)")
        response = input().lower()
        if response == 'y':
            print("\nTesting full data (this might take a while)...")
            test_data()
    else:
        print("\nMetadata test failed. Please check the database setup.")
        sys.exit(1)
