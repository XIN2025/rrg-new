from clickhouse_connect import get_client
import os
import logging

logger = logging.getLogger(__name__)

def get_clickhouse_connection():
    """Get a connection to ClickHouse using HTTP protocol."""
    try:
        host = os.getenv('CHDB_HOST', 'localhost')
        port = int(os.getenv('CHDB_PORT', 8123))  # HTTP default is 8123
        user = os.getenv('CHDB_USER', 'default')
        password = os.getenv('CHDB_PASSWORD', '')
        database = os.getenv('CHDB_NAME', 'default')
        
        logger.info(f"Connecting to ClickHouse HTTP at {host}:{port} with user {user} and database {database}")
        
        client = get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
            secure=False  
        )
        return client
    except Exception as e:
        logger.error(f"Error connecting to ClickHouse: {str(e)}", exc_info=True)
        raise 
