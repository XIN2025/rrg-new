import asyncio
import logging
import traceback
from datetime import datetime, timedelta
from src.modules.rrg.service import RRGService
from src.modules.rrg.schemas import RrgRequest
from src.utils.duck_pool import get_duckdb_connection
from src.utils.clickhouse_pool import pool as ClickHousePool

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def print_available_symbols():
    try:
        with get_duckdb_connection() as conn:
            df = conn.execute('SELECT DISTINCT symbol FROM public.market_metadata LIMIT 10').fetchdf()
            logger.info(f"Sample available symbols in public.market_metadata: {df['symbol'].tolist()}")
    except Exception as e:
        logger.error(f"Error fetching symbols: {e}")
        logger.error(traceback.format_exc())

async def test_database_connection():
    """Test connection to both DuckDB and ClickHouse"""
    logger.info("Testing database connections...")
    
    # Test DuckDB connection
    try:
        with get_duckdb_connection() as conn:
            result = conn.execute("SELECT 1").fetchone()
            logger.info("✅ DuckDB connection successful")
    except Exception as e:
        logger.error(f"❌ DuckDB connection failed: {str(e)}")
        logger.error(traceback.format_exc())
        return False
    
    # Test ClickHouse connection with timeout using asyncio
    async def clickhouse_test():
        return ClickHousePool.execute_query("SELECT 1")
    try:
        logger.info("Attempting ClickHouse connection...")
        await asyncio.wait_for(clickhouse_test(), timeout=10)
        logger.info("✅ ClickHouse connection successful")
    except asyncio.TimeoutError:
        logger.error("❌ ClickHouse connection timed out after 10 seconds")
        return False
    except Exception as e:
        logger.error(f"❌ ClickHouse connection failed: {str(e)}")
        logger.error(traceback.format_exc())
        return False
    
    return True

async def test_data_loading():
    """Test loading data from ClickHouse to DuckDB"""
    logger.info("\nTesting data loading...")
    
    service = RRGService()
    
    # Set date range for last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    async def load_data_call():
        return await service.load_data(
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d')
        )
    try:
        logger.info(f"Loading data from {start_date} to {end_date}")
        success = await asyncio.wait_for(load_data_call(), timeout=30)
        
        if success:
            logger.info("✅ Data loading successful")
            
            # Verify data was loaded
            try:
                with get_duckdb_connection() as conn:
                    logger.info("Checking loaded data in DuckDB...")
                    count = conn.execute("SELECT COUNT(*) FROM public.eod_stock_data").fetchone()[0]
                    logger.info(f"Loaded {count:,} records into DuckDB")
                    
                    # Check for any null values
                    null_check = conn.execute("""
                        SELECT COUNT(*) as null_count 
                        FROM public.eod_stock_data 
                        WHERE close_price IS NULL 
                        OR ratio IS NULL 
                        OR momentum IS NULL
                    """).fetchone()[0]
                    
                    if null_check > 0:
                        logger.warning(f"Found {null_check} records with null values")
                    
                    # Check data distribution
                    symbol_count = conn.execute("""
                        SELECT COUNT(DISTINCT security_code) as symbol_count 
                        FROM public.eod_stock_data
                    """).fetchone()[0]
                    logger.info(f"Data contains {symbol_count} unique symbols")
                    
                return True
            except Exception as e:
                logger.error(f"❌ Error verifying loaded data: {str(e)}")
                logger.error(traceback.format_exc())
                return False
        else:
            logger.error("❌ Data loading failed")
            return False
            
    except asyncio.TimeoutError:
        logger.error("❌ Data loading timed out after 30 seconds")
        return False
    except Exception as e:
        logger.error(f"❌ Error during data loading: {str(e)}")
        logger.error(traceback.format_exc())
        return False

async def test_rrg_generation():
    """Test RRG data generation for a specific index"""
    logger.info("\nTesting RRG data generation...")
    
    service = RRGService()
    
    # Test with NIFTY 50 index
    request = RrgRequest(
        index_symbol="NIFTY 50",
        timeframe="daily",
        date_range="30"
    )
    
    async def rrg_call():
        return await service.get_rrg_data(request)
    try:
        logger.info(f"Generating RRG data for {request.index_symbol}")
        response = await asyncio.wait_for(rrg_call(), timeout=30)
        
        if response.status == "success":
            logger.info("✅ RRG data generation successful")
            logger.info(f"Generated filename: {response.filename}")
            
            # Verify data structure
            if "datalists" in response.data and "indexdata" in response.data:
                logger.info(f"Found {len(response.data['datalists'])} stocks in RRG data")
                logger.info(f"Found {len(response.data['indexdata'])} index data points")
                
                # Additional data quality checks
                if len(response.data['datalists']) == 0:
                    logger.warning("No stocks found in RRG data")
                if len(response.data['indexdata']) == 0:
                    logger.warning("No index data points found")
                    
                # Check first stock's data points
                if len(response.data['datalists']) > 0:
                    first_stock = response.data['datalists'][0]
                    logger.info(f"Sample stock data points for {first_stock['symbol']}: {len(first_stock['data'])}")
            return True
        else:
            logger.error(f"❌ RRG data generation failed: {response.error}")
            return False
            
    except asyncio.TimeoutError:
        logger.error("❌ RRG generation timed out after 30 seconds")
        return False
    except Exception as e:
        logger.error(f"❌ Error during RRG generation: {str(e)}")
        logger.error(traceback.format_exc())
        return False

async def main():
    """Run all tests in sequence"""
    logger.info("Starting RRG pipeline tests...")
    print_available_symbols()
    
    try:
        # Test database connections
        if not await test_database_connection():
            logger.error("Database connection tests failed. Aborting further tests.")
            return
        
        # Test data loading
        if not await test_data_loading():
            logger.error("Data loading test failed. Aborting further tests.")
            return
        
        # Test RRG generation
        if not await test_rrg_generation():
            logger.error("RRG generation test failed.")
            return
        
        logger.info("\n✅ All tests completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Unexpected error in main: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(main()) 
