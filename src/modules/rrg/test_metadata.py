from src.modules.rrg.metadata_store import get_metadata_store
from src.utils.logger import get_logger

logger = get_logger("test_metadata")

def main():
    try:
        # Get metadata store instance
        store = get_metadata_store()
        
        # Run the test
        logger.info("Starting metadata loading test...")
        success = store.test_metadata_loading()
        
        if success:
            logger.info("Metadata loading test completed successfully")
        else:
            logger.error("Metadata loading test failed")
            
    except Exception as e:
        logger.error(f"Error running metadata test: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main() 
