import logging
from clickhouse_connect import get_client
from clickhouse_connect.driver.exceptions import DatabaseError
from src.config import CHDB_HOST, CHDB_PORT, CHDB_USER, CHDB_PASSWORD, CHDB_NAME, CHDB_SECURE

logger = logging.getLogger("clickhouse")
logger.setLevel(logging.INFO)

# Add console handler if not already present
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

class ClickHousePool:
    _instance = None
    _client = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ClickHousePool, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if self._client is None:
            self._initialize_client()

    def _initialize_client(self):
        try:
            logger.info("Initializing ClickHouse connection...")
            logger.info(f"CHDB_HOST: {CHDB_HOST}")
            logger.info(f"CHDB_PORT: {CHDB_PORT}")
            logger.info(f"CHDB_USER: {CHDB_USER}")
            logger.info(f"CHDB_PASSWORD: {'*' * len(CHDB_PASSWORD)}")
            logger.info(f"CHDB_NAME: {CHDB_NAME}")
            logger.info(f"CHDB_SECURE: {CHDB_SECURE}")

            self._client = get_client(
                host=CHDB_HOST,
                port=CHDB_PORT,
                username=CHDB_USER,
                password=CHDB_PASSWORD,
                database=CHDB_NAME,
                secure=CHDB_SECURE
            )
            logger.info("ClickHouse connection initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize ClickHouse connection: {str(e)}")
            raise

    def execute_query(self, query, parameters=None):
        try:
            logger.info(f"Executing ClickHouse query: {query}")
            if parameters:
                logger.info(f"Query parameters: {parameters}")
            
            result = self._client.query(query, parameters=parameters)
            logger.info(f"Query executed successfully, returned {len(result.result_rows)} rows")
            return result.result_rows
        except DatabaseError as e:
            logger.error(f"ClickHouse query execution failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error executing ClickHouse query: {str(e)}")
            raise

    def close(self):
        if self._client:
            try:
                self._client.close()
                logger.info("ClickHouse connection closed")
            except Exception as e:
                logger.error(f"Error closing ClickHouse connection: {str(e)}")
            finally:
                self._client = None
                self._instance = None

# Create a singleton instance
pool = ClickHousePool() 
