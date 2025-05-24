import os
import logging
from dotenv import load_dotenv
import clickhouse_connect
import polars as pl
from typing import Optional
from clickhouse_connect.driver.exceptions import OperationalError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("clickhouse")

# Load environment variables
load_dotenv()

class ClickHousePool:
    _instance = None

    def __init__(self):
        try:
            host = os.getenv('CHDB_HOST', 'localhost')
            port = int(os.getenv('CHDB_PORT', '8123'))
            database = os.getenv('CHDB_NAME', 'strike')
            username = os.getenv('CHDB_USER', 'default')
            password = os.getenv('CHDB_PASSWORD', '')
            secure = os.getenv('CHDB_SECURE', 'False').lower() == 'true'

            logger.info(f"CHDB_HOST: {host}")
            logger.info(f"CHDB_PORT: {port}")
            logger.info(f"CHDB_USER: {username}")
            logger.info(f"CHDB_PASSWORD: {'*' * len(password) if password else ''}")
            logger.info(f"CHDB_NAME: {database}")
            logger.info(f"CHDB_SECURE: {secure}")

            self._client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database,
                secure=secure
            )
        except Exception as e:
            logger.error(f"Error initializing ClickHouse client: {str(e)}")
            self._client = None

    @classmethod
    def get_instance(cls) -> 'ClickHousePool':
        if cls._instance is None:
            cls._instance = ClickHousePool()
        return cls._instance

    def get_client(self):
        if self._client is None:
            raise OperationalError("ClickHouse client not initialized")
        return self._client

    def execute(self, query: str, params: Optional[dict] = None):
        try:
            if self._client is None:
                logger.warning("ClickHouse client not initialized, returning empty result")
                return []
            return self._client.command(query, parameters=params)
        except Exception as e:
            logger.error(f"Error executing ClickHouse query: {str(e)}")
            return []

    def execute_query(self, query, parameters=None):
        try:
            logger.info(f"Executing ClickHouse query: {query[:100]}")
            result = self._client.query(query, parameters=parameters)
            return result.result_rows
        except Exception as e:
            logger.error(f"ClickHouse query execution failed: {e}")
            raise

    def execute_polars_query(self, query):
        """
        Execute a query using Polars and return a Polars DataFrame.
        """
        try:
            conn_str = self.get_connection_string()
            logger.info(f"Executing Polars query: {query[:100]}")
            df = pl.read_database_uri(query, conn_str)
            logger.info(f"Polars query returned {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Polars ClickHouse query failed: {e}")
            raise

    def get_connection_string(self):
        user = os.getenv("CHDB_USER", "default")
        pwd = os.getenv("CHDB_PASSWORD", "")
        host = os.getenv("CHDB_HOST", "localhost")
        port = os.getenv("CHDB_PORT", "8123")
        db = os.getenv("CHDB_NAME", "strike")
        return f"clickhouse://{user}:{pwd}@{host}:{port}/{db}"


if __name__ == "__main__":
    pool = ClickHousePool.get_instance()
    query = "SELECT * FROM system.tables"
    df = pool.execute_query(query)
    print(df)

pool = ClickHousePool.get_instance()
