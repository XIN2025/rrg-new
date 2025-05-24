import os
import sys

# Add the project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.append(project_root)

from src.modules.rrg.metadata_store import get_metadata_store
from src.utils.logger import get_logger
import logging

logger = get_logger("test_metadata")
logger.setLevel(logging.INFO)

def main():
    try:
        # Get metadata store instance
        store = get_metadata_store()
        
        # Run the test
        logger.info("Starting metadata loading test...")
        success = store.test_metadata_loading()
        
        if success:
            logger.info("Metadata loading test completed successfully")
            
            # Print detailed status
            metadata_status = store.get_metadata_status()
            logger.info("Metadata Status:")
            for key, value in metadata_status.items():
                logger.info(f"  {key}: {value}")
            
            # Print price data status
            price_status = store.get_price_data_status()
            logger.info("Price Data Status:")
            for key, value in price_status.items():
                logger.info(f"  {key}: {value}")
        else:
            logger.error("Metadata loading test failed")
            
    except Exception as e:
        logger.error(f"Error running metadata test: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main() 
