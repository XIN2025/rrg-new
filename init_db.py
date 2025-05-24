from src.modules.rrg.service import RRGService
import asyncio
from src.utils.duck_pool import get_duckdb_connection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def init_database():
    """Initialize the database with required tables and data"""
    try:
        # Create tables if they don't exist
        with get_duckdb_connection() as conn:
            # Create public schema
            conn.execute("CREATE SCHEMA IF NOT EXISTS public")
            logger.info("Created public schema")
            
            # Create market metadata table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS public.market_metadata (
                    security_code VARCHAR,
                    symbol VARCHAR,
                    ticker VARCHAR,
                    security_type_code INTEGER,
                    PRIMARY KEY (security_code)
                )
            """)
            
            # Create EOD stock data table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS public.eod_stock_data (
                    created_at TIMESTAMP,
                    ratio DOUBLE,
                    momentum DOUBLE,
                    close_price DOUBLE,
                    change_percentage DOUBLE,
                    metric_1 DOUBLE,
                    metric_2 DOUBLE,
                    metric_3 DOUBLE,
                    signal INTEGER,
                    security_code VARCHAR,
                    previous_close DOUBLE,
                    ticker VARCHAR,
                    PRIMARY KEY (created_at, security_code)
                )
            """)
            
            logger.info("Database tables created successfully")
        
        # Initialize RRG service and load data
        service = RRGService()
        logger.info("Loading EOD data...")
        success = await service.load_data()
        
        if success:
            logger.info("Database initialized successfully!")
            return True
        else:
            logger.error("Failed to initialize database")
            return False
            
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    asyncio.run(init_database()) 
