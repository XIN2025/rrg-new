print('>>> test_generate.py is running')

from src.modules.rrg.generate import get_ticker_data, format_rrg_data
import traceback

def test_rrg_generation():
    """Test RRG generation functionality."""
    print("\n=== Starting RRG Generation Test ===\n")
    
    # Test data - using TCS as ticker and CNX500 as index
    test_tickers = ["TCS"]
    test_index = "CNX500"  # Using CNX500 as the index
    test_date_range = "3 months"
    
    try:
        print("Test 1: Checking data retrieval...")
        data_df, metadata_df = get_ticker_data(test_tickers + [test_index], test_date_range, test_index)
        
        # Force conversion to pandas DataFrame if needed
        if hasattr(data_df, 'to_pandas'):
            data_df = data_df.to_pandas()
        if hasattr(metadata_df, 'to_pandas'):
            metadata_df = metadata_df.to_pandas()
        
        if data_df.empty or metadata_df.empty:
            raise Exception("No data retrieved")
            
        print(f"✓ Successfully retrieved data for {len(data_df)} rows and {len(metadata_df)} tickers")
        print("Data DataFrame columns:", data_df.columns.tolist())
        print("Data DataFrame dtypes:\n", data_df.dtypes)
        print("Data DataFrame head:")
        print(data_df.head(10))
        print("Metadata DataFrame columns:", metadata_df.columns.tolist())
        print("Metadata DataFrame dtypes:\n", metadata_df.dtypes)
        print("Metadata DataFrame head:")
        print(metadata_df.head(10))
        print(f"Unique symbols in data_df: {data_df['symbol'].unique()}")
        print(f"Unique tickers in metadata_df: {metadata_df['ticker'].unique() if 'ticker' in metadata_df.columns else 'N/A'}")
        print(f"Date range in data_df: {data_df['created_at'].min()} to {data_df['created_at'].max()}")
        
        print("\nTest 2: Checking data formatting...")
        formatted_data = format_rrg_data(data_df, metadata_df, test_index)
        
        if not formatted_data or "data" not in formatted_data:
            raise Exception("Data formatting failed")
            
        print("✓ Successfully formatted data for RRG visualization")
        print("\nFormatted data structure:")
        print(f"- Benchmark: {formatted_data['data']['benchmark']}")
        print(f"- Number of datalists: {len(formatted_data['data']['datalists'])}")
        print(f"- Number of change data points: {len(formatted_data['change_data'])}")
        
        print("\n=== All Tests Passed Successfully ===\n")
        return True
        
    except Exception as e:
        print(f"\nTest failed with error: {str(e)}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_rrg_generation() 
 