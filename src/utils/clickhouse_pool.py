from clickhouse_driver import Client
from typing import List, Dict, Any
import os
from src.utils.logger import get_logger
import requests
import json

logger = get_logger("clickhouse_pool")

class ClickHousePool:
    _instance = None
    _client = None
    _session = None

    @classmethod
    def get_session(cls):
        if cls._session is None:
            cls._session = requests.Session()
            cls._session.verify = False  # Skip SSL verification for development
        return cls._session

    @classmethod
    def execute_query(cls, query: str) -> List[Dict[str, Any]]:
        """Execute a query using HTTP interface and return results as a list of dictionaries"""
        try:
            host = os.getenv('CLICKHOUSE_HOST', 'api-uat-v2.strike.money')
            port = os.getenv('CLICKHOUSE_PORT', '18123')
            user = os.getenv('CLICKHOUSE_USER', 'open_gig')
            password = os.getenv('CLICKHOUSE_PASSWORD', 'open_gig_at_strike_IC')
            database = os.getenv('CLICKHOUSE_DATABASE', 'strike')

            url = f"http://{host}:{port}"
            logger.info(f"Executing ClickHouse query via HTTP at {url}")
            
            session = cls.get_session()
            response = session.post(
                url,
                params={
                    'database': database,
                    'query': query,
                    'user': user,
                    'password': password,
                    'default_format': 'JSONEachRow'
                },
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code != 200:
                raise Exception(f"ClickHouse query failed with status {response.status_code}: {response.text}")
            
            # Parse the response
            result = []
            for line in response.text.strip().split('\n'):
                if line:
                    result.append(json.loads(line))
            
            logger.debug(f"Query returned {len(result)} rows")
            return result
            
        except Exception as e:
            logger.error(f"Error executing ClickHouse query: {str(e)}")
            raise 
