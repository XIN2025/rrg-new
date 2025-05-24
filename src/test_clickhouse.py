from db.clickhouse import ClickHousePool
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_clickhouse_connection():
    try:
        # Get ClickHouse instance
        pool = ClickHousePool.get_instance()
        
        # Get distinct security_tokens from index_prices_1min
        query = """
        SELECT DISTINCT security_token
        FROM strike.index_prices_1min
        LIMIT 10
        """
        result = pool.execute_query(query)
        logger.info(f"Distinct security_tokens from index_prices_1min: {result}")
        
        # Get distinct security_codes from dion_index_master
        query = """
        SELECT DISTINCT security_code, index_name
        FROM strike.dion_index_master
        LIMIT 10
        """
        result = pool.execute_query(query)
        logger.info(f"Distinct security_codes from dion_index_master: {result}")
        
        # Try a join with a specific security_token
        query = """
        SELECT 
            i.date_time,
            i.security_token,
            m.index_name,
            i.open,
            i.high,
            i.low,
            i.close
        FROM strike.index_prices_1min i
        LEFT JOIN strike.dion_index_master m ON CAST(i.security_token AS String) = m.security_code
        WHERE i.security_token = 6961
        LIMIT 5
        """
        result = pool.execute_query(query)
        logger.info(f"Joined data for security_token 6961: {result}")
        
        return True
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_clickhouse_connection()
    if success:
        print("✅ ClickHouse connection test successful!")
    else:
        print("❌ ClickHouse connection test failed!") 
