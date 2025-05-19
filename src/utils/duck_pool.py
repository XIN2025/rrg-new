import duckdb
import threading
from typing import Dict, Optional, List, Tuple
from contextlib import contextmanager
from src.utils.metrics import TimerMetric
from src.utils.logger import get_logger

# Get logger for this module
logger = get_logger("duck_pool")

class DuckDBConnectionPool:
    """
    A thread-safe connection pool for DuckDB connections.
    
    This class manages a pool of DuckDB connections with:
    - Thread safety for concurrent requests
    - Maximum connection limits
    - Connection validation and reuse
    - Proper error handling
    """
    
    def __init__(self, max_connections: int = 10, connection_timeout: int = 30):
        self._connections: Dict[str, List[Tuple[duckdb.DuckDBPyConnection, float]]] = {}
        self._lock = threading.RLock()  # Reentrant lock for thread safety
        self._max_connections = max_connections
        self._connection_timeout = connection_timeout  # seconds
    
    @contextmanager
    def connection(self, db_path: str = "data/pydb.duckdb", read_only: bool = True):
        """
        Context manager for getting and automatically returning a connection.
        
        Args:
            db_path: Path to the DuckDB database
            read_only: Whether to open the connection in read-only mode
            
        Yields:
            DuckDB connection object
        """
        conn = self.get_connection(db_path, read_only)
        try:
            yield conn
        finally:
            self.return_connection(db_path, conn)
    
    def get_connection(self, db_path: str = "data/pydb.duckdb", read_only: bool = True) -> duckdb.DuckDBPyConnection:
        """
        Get a DuckDB connection from the pool.
        
        Args:
            db_path: Path to the DuckDB database
            read_only: Whether to open the connection in read-only mode
            
        Returns:
            DuckDB connection object
        """
        with TimerMetric("get_duckdb_connection", "duck_pool"):
            with self._lock:
                # Initialize the connection list for this db_path if it doesn't exist
                if db_path not in self._connections:
                    self._connections[db_path] = []
                
                # Try to reuse an existing connection
                now = __import__('time').time()
                for i, (conn, _) in enumerate(self._connections[db_path]):
                    try:
                        conn.execute("SELECT 1")
                        # Remove this connection from the available pool
                        self._connections[db_path].pop(i)
                        logger.debug(f"Reusing existing DuckDB connection for {db_path}")
                        return conn
                    except Exception:
                        logger.debug(f"Existing connection for {db_path} is invalid, removing it")
                        try:
                            conn.close()
                        except:
                            pass
                        self._connections[db_path].pop(i)
                
                conn_count = sum(len(conns) for conns in self._connections.values())
                if conn_count >= self._max_connections:
                    self._cleanup_expired_connections()
                    
                    if sum(len(conns) for conns in self._connections.values()) >= self._max_connections:
                        logger.warning(f"Connection pool limit reached ({self._max_connections})")
                        raise Exception(f"Connection pool exhausted (max={self._max_connections})")
                
                try:
                    conn = duckdb.connect(db_path, read_only=read_only)
                    logger.debug(f"Created new DuckDB connection for {db_path}")
                    return conn
                except Exception as e:
                    logger.error(f"Error connecting to DuckDB database {db_path}: {e}")
                    raise
    
    def return_connection(self, db_path: str, conn: duckdb.DuckDBPyConnection):
        """
        Return a connection to the pool for reuse.
        
        Args:
            db_path: Path to the DuckDB database
            conn: The connection to return
        """
        with self._lock:
            try:
                conn.execute("SELECT 1")
                now = __import__('time').time()
                self._connections[db_path].append((conn, now))
                logger.debug(f"Returned connection to pool for {db_path}")
            except Exception:
                # If the connection is invalid, close it
                logger.debug(f"Returned connection for {db_path} is invalid, closing it")
                try:
                    conn.close()
                except:
                    pass
    
    def _cleanup_expired_connections(self):
        """Remove and close expired connections from the pool."""
        now = __import__('time').time()
        with self._lock:
            for db_path, connections in list(self._connections.items()):
                unexpired = []
                for conn, timestamp in connections:
                    if now - timestamp > self._connection_timeout:
                        try:
                            conn.close()
                            logger.debug(f"Closed expired DuckDB connection for {db_path}")
                        except Exception as e:
                            logger.warning(f"Error closing expired DuckDB connection for {db_path}: {e}")
                    else:
                        unexpired.append((conn, timestamp))
                self._connections[db_path] = unexpired
    
    def close_all(self):
        """Close all connections in the pool."""
        with self._lock:
            for db_path, connections in self._connections.items():
                for conn, _ in connections:
                    if conn is not None:
                        try:
                            conn.close()
                            logger.debug(f"Closed DuckDB connection for {db_path}")
                        except Exception as e:
                            logger.warning(f"Error closing DuckDB connection for {db_path}: {e}")
            self._connections = {}
    
    def close_connection(self, db_path: str):
        """Close all connections for a specific database in the pool."""
        with self._lock:
            if db_path in self._connections:
                for conn, _ in self._connections[db_path]:
                    try:
                        conn.close()
                        logger.debug(f"Closed DuckDB connection for {db_path}")
                    except Exception as e:
                        logger.warning(f"Error closing DuckDB connection for {db_path}: {e}")
                self._connections[db_path] = []

# Create a singleton instance
_pool = DuckDBConnectionPool()

def get_duckdb_connection(db_path: str = "data/pydb.duckdb", read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """
    Get a DuckDB connection from the pool.
    
    Args:
        db_path: Path to the DuckDB database
        read_only: Whether to open the connection in read-only mode
        
    Returns:
        DuckDB connection object
    """
    return _pool.get_connection(db_path, read_only)

@contextmanager
def duckdb_connection(db_path: str = "data/pydb.duckdb", read_only: bool = True):
    """
    Context manager for safely using a DuckDB connection.
    
    Args:
        db_path: Path to the DuckDB database
        read_only: Whether to open the connection in read-only mode
        
    Yields:
        DuckDB connection object
    """
    with _pool.connection(db_path, read_only) as conn:
        yield conn

def close_all_connections():
    """Close all connections in the pool."""
    _pool.close_all()

def close_connection(db_path: str):
    """Close a specific connection in the pool."""
    _pool.close_connection(db_path)

def get_duckdb_connection():
    """Get a connection from pool with connection logging"""
    logger.debug("🛢️ Attempting to acquire DuckDB connection")
    try:
        conn = duckdb.connect("data/pydb.duckdb")
        logger.info(f"🔗 Connection acquired (ID: {id(conn)})")
        conn.execute("PRAGMA show_tables;")  # Keep connection active
        return conn
    except Exception as e:
        logger.error(f"🚨 Connection acquisition failed: {str(e)}")
        raise