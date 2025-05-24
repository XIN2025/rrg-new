from src.utils.duck_pool import get_duckdb_connection
from src.modules.db.reference_data_loader import load_all_reference_data
import logging
import os
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def cleanup_duckdb():
    """Clean up any existing DuckDB connections and locks"""
    try:
        # Wait a bit to ensure any existing connections are closed
        time.sleep(1)
        
        # Try to remove any existing lock files
        db_path = "data/pydb.duckdb"
        lock_path = f"{db_path}.lock"
        
        if os.path.exists(lock_path):
            try:
                os.remove(lock_path)
                logger.info("Removed existing lock file")
            except Exception as e:
                logger.warning(f"Could not remove lock file: {e}")
                
        return True
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        return False

def test_database():
    try:
        # Clean up any existing connections
        cleanup_duckdb()
        
        # First try to load reference data
        logger.info("Attempting to load reference data...")
        success = load_all_reference_data(force=True)
        
        if not success:
            logger.error("Failed to load reference data")
            return False
            
        # Test the connection and query
        with get_duckdb_connection() as conn:
            # Test companies table
            companies_count = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
            logger.info(f"Found {companies_count} companies")
            
            # Test market_metadata table
            metadata_count = conn.execute("SELECT COUNT(*) FROM market_metadata").fetchone()[0]
            logger.info(f"Found {metadata_count} market metadata entries")
            
            # Verify slug column exists
            columns = conn.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'market_metadata' 
                AND column_name = 'slug'
            """).fetchall()
            
            if columns:
                logger.info("✅ Slug column exists in market_metadata table")
            else:
                logger.error("❌ Slug column not found in market_metadata table")
                
        return True
        
    except Exception as e:
        logger.error(f"Error testing database: {str(e)}")
        return False
    finally:
        # Ensure cleanup happens even if there's an error
        cleanup_duckdb()

if __name__ == "__main__":
    test_database() 
