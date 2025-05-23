import duckdb
import threading
import time
from typing import Optional
import logging
from contextlib import contextmanager
from src.utils.metrics import TimerMetric
from src.utils.logger import get_logger

# Get logger for this module
logger = logging.getLogger("duck_pool")

class DuckDBPool:
    def __init__(self, db_path: str, max_connections: int = 5, timeout: int = 30):
        self.db_path = db_path
        self.max_connections = max_connections
        self.timeout = timeout
        self.connections = []
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        
    def get_connection(self) -> Optional[duckdb.DuckDBPyConnection]:
        with self.condition:
            start_time = time.time()
            while len(self.connections) >= self.max_connections:
                if time.time() - start_time > self.timeout:
                    logger.error("🚨 Connection acquisition failed: Timeout waiting for available connection")
                    return None
                logger.debug("Waiting for available connection...")
                self.condition.wait(timeout=1)
            
            try:
                conn = duckdb.connect(self.db_path)
                self.connections.append(conn)
                logger.info(f"🔗 Connection acquired (ID: {id(conn)})")
                return conn
            except Exception as e:
                logger.error(f"🚨 Connection acquisition failed: {str(e)}")
                return None
    
    def release_connection(self, conn: duckdb.DuckDBPyConnection):
        with self.condition:
            if conn in self.connections:
                try:
                    conn.close()
                    self.connections.remove(conn)
                    logger.debug(f"Connection released (ID: {id(conn)})")
                except Exception as e:
                    logger.error(f"Error closing connection: {str(e)}")
            self.condition.notify()
    
    def close_all(self):
        with self.condition:
            for conn in self.connections:
                try:
                    conn.close()
                except Exception as e:
                    logger.error(f"Error closing connection: {str(e)}")
            self.connections.clear()
            logger.debug("All connections closed")

# Global pool instance
_pool = None

def init_pool(db_path: str = "data/pydb.duckdb", max_connections: int = 5, timeout: int = 30):
    global _pool
    if _pool is None:
        _pool = DuckDBPool(db_path, max_connections, timeout)
        logger.info(f"Initialized DuckDB pool with {max_connections} max connections")

@contextmanager
def get_duckdb_connection():
    global _pool
    if _pool is None:
        init_pool()
    
    conn = None
    try:
        conn = _pool.get_connection()
        if conn is None:
            raise Exception("Failed to acquire database connection")
        yield conn
    finally:
        if conn is not None:
            _pool.release_connection(conn)

def close_pool():
    global _pool
    if _pool is not None:
        _pool.close_all()
        _pool = None
        logger.debug("DuckDB connection pool closed")
