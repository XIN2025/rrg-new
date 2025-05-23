from clickhouse_driver import Client
import os
import logging

logger = logging.getLogger(__name__)

def get_clickhouse_connection():
    """Get a connection to ClickHouse."""
    try:
        host = os.getenv('CLICKHOUSE_HOST', 'localhost')
        port = int(os.getenv('CLICKHOUSE_PORT', 9000))
        user = os.getenv('CLICKHOUSE_USER', 'default')
        password = os.getenv('CLICKHOUSE_PASSWORD', '')
        database = os.getenv('CLICKHOUSE_DATABASE', 'indiacharts')
        
        client = Client(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        
        return client
    except Exception as e:
        logger.error(f"Error connecting to ClickHouse: {str(e)}", exc_info=True)
        raise 
