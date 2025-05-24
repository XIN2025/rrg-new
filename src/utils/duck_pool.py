import duckdb
import threading
import time
import os
from typing import Optional
import logging
from contextlib import contextmanager
from src.utils.metrics import TimerMetric
from src.utils.logger import get_logger
import queue

# Get logger for this module
logger = logging.getLogger("duck_pool")

# Get absolute path to data directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, 'data', 'pydb.duckdb')

class DuckDBPool:
    def __init__(self, db_path=DEFAULT_DB_PATH, max_connections=5, timeout=300):
        self.db_path = db_path
        self.pool = queue.Queue(maxsize=max_connections)
        self.max_connections = max_connections
        self.timeout = timeout
        self.active_connections = set()
        self._lock = threading.Lock()
        
        # Initialize pool with connections
        for _ in range(max_connections):
            try:
                conn = self._create_connection()
                self.pool.put(conn)
            except Exception as e:
                logger.error(f"Error creating initial connection: {str(e)}")
                
    def _create_connection(self):
        """Create a new DuckDB connection"""
        try:
            conn = duckdb.connect(self.db_path)
            return conn
        except Exception as e:
            logger.error(f"Error creating DuckDB connection: {str(e)}")
            raise
            
    def get_connection(self):
        """Get a connection from the pool"""
        try:
            with self._lock:
                if not self.pool.empty():
                    conn = self.pool.get(timeout=self.timeout)
                    self.active_connections.add(conn)
                    logger.info(f"🔗 Connection acquired (ID: {id(conn)})")
                    return conn
                    
            # If no connection available, create new one if under max
            with self._lock:
                if len(self.active_connections) < self.max_connections:
                    conn = self._create_connection()
                    self.active_connections.add(conn)
                    logger.info(f"🔗 New connection created (ID: {id(conn)})")
                    return conn
                    
            # Wait for a connection to become available
            conn = self.pool.get(timeout=self.timeout)
            with self._lock:
                self.active_connections.add(conn)
            logger.info(f"🔗 Connection acquired after wait (ID: {id(conn)})")
            return conn
            
        except queue.Empty:
            raise Exception("Timeout waiting for available connection")
            
    def release_connection(self, conn):
        """Return a connection to the pool"""
        try:
            with self._lock:
                if conn in self.active_connections:
                    self.active_connections.remove(conn)
                    self.pool.put(conn)
                    logger.debug(f"Connection released (ID: {id(conn)})")
        except Exception as e:
            logger.error(f"Error releasing connection: {str(e)}")
            
    def close_all(self):
        """Close all connections in the pool"""
        with self._lock:
            while not self.pool.empty():
                try:
                    conn = self.pool.get_nowait()
                    conn.close()
                except:
                    pass
            self.active_connections.clear()

# Global pool instance
_pool = None
_pool_db_path = None

def init_pool(db_path: str = "data/pydb.duckdb", max_connections: int = 5, timeout: int = 30):
    global _pool, _pool_db_path
    if _pool is None or _pool_db_path != db_path:
        _pool = DuckDBPool(db_path, max_connections, timeout)
        _pool_db_path = db_path
        logger.info(f"Initialized DuckDB pool with {max_connections} max connections at {db_path}")

@contextmanager
def get_duckdb_connection(db_path: Optional[str] = None):
    global _pool, _pool_db_path
    if _pool is None or (db_path is not None and _pool_db_path != db_path):
        if db_path is None:
            db_path = "data/pydb.duckdb"
        init_pool(db_path)
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
