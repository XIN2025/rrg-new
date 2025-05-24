from src.modules.rrg.metadata_store import RRGMetadataStore
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_metadata_loading():
    try:
        # Create metadata store instance
        store = RRGMetadataStore()
        
        # Force refresh metadata
        success = store.refresh_metadata()
        if not success:
            logger.error("Failed to refresh metadata")
            return False
            
        # Get all dataframes
        indices_df = store.get_indices()
        stocks_df = store.get_stocks()
        market_metadata_df = store.get_market_metadata()
        
        # Print sample data from each dataframe
        logger.info("\nSample from indices_df:")
        if indices_df is not None and not indices_df.is_empty():
            logger.info(indices_df.head())
            logger.info(f"Columns: {indices_df.columns}")
            logger.info(f"Total rows: {len(indices_df)}")
        else:
            logger.error("No indices data loaded")
            
        logger.info("\nSample from stocks_df:")
        if stocks_df is not None and not stocks_df.is_empty():
            logger.info(stocks_df.head())
            logger.info(f"Columns: {stocks_df.columns}")
            logger.info(f"Total rows: {len(stocks_df)}")
        else:
            logger.error("No stocks data loaded")
            
        logger.info("\nSample from market_metadata_df:")
        if market_metadata_df is not None and not market_metadata_df.is_empty():
            logger.info(market_metadata_df.head())
            logger.info(f"Columns: {market_metadata_df.columns}")
            logger.info(f"Total rows: {len(market_metadata_df)}")
        else:
            logger.error("No market metadata loaded")
            
        return True
        
    except Exception as e:
        logger.error(f"Error testing metadata loading: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    success = test_metadata_loading()
    if success:
        print("✅ Metadata loading test completed successfully!")
    else:
        print("❌ Metadata loading test failed!") 
