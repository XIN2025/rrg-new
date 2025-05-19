#!/usr/bin/env python
"""
Simple test script to verify the DuckDB connection pool functionality.
"""

import time
from src.utils.duck_pool import get_duckdb_connection, close_all_connections
from src.utils.logger import get_logger

# Get logger
logger = get_logger("test_duck_pool")

def test_connection_reuse():
    """Test that connections are reused from the pool."""
    logger.info("Testing connection reuse...")
    
    # Get first connection
    start = time.time()
    conn1 = get_duckdb_connection()
    time1 = time.time() - start
    
    # Get second connection to same DB - should be reused
    start = time.time()
    conn2 = get_duckdb_connection()
    time2 = time.time() - start
    
    # Test connection equality
    are_same = conn1 is conn2
    
    logger.info(f"First connection took {time1:.4f}s")
    logger.info(f"Second connection took {time2:.4f}s")
    logger.info(f"Connections are the same object: {are_same}")
    
    # Test query execution
    result = conn1.sql("SELECT 1 as test").fetchone()[0]
    logger.info(f"Query result: {result}")
    
    # Compare connection times (second should be faster)
    logger.info(f"Connection time difference: {time1 - time2:.4f}s")
    logger.info(f"Second connection was {'faster' if time2 < time1 else 'slower'}")
    
    return are_same and result == 1

def test_multiple_dbs():
    """Test connection pooling with multiple DBs."""
    logger.info("Testing multiple DB connections...")
    
    # Note: second path is just for testing, won't actually connect
    db_paths = ["data/pydb.duckdb", "data/test.duckdb"]
    
    connections = {}
    for path in db_paths:
        try:
            connections[path] = get_duckdb_connection(path)
            logger.info(f"Connected to {path}")
        except Exception as e:
            logger.error(f"Failed to connect to {path}: {e}")
    
    # Verify connections are different objects
    if len(connections) > 1:
        paths = list(connections.keys())
        are_different = connections[paths[0]] is not connections[paths[1]]
        logger.info(f"Connections to different DBs are different objects: {are_different}")
    
    # Close all connections
    close_all_connections()
    
    return len(connections) > 0

if __name__ == "__main__":
    logger.info("Starting DuckDB connection pool tests...")
    
    reuse_test = test_connection_reuse()
    logger.info(f"Connection reuse test {'passed' if reuse_test else 'failed'}")
    
    multi_db_test = test_multiple_dbs()
    logger.info(f"Multiple DB test {'passed' if multi_db_test else 'failed'}")
    
    # Test reconnection after closing
    logger.info("Testing reconnection after closing all connections...")
    conn1 = get_duckdb_connection()
    close_all_connections()
    conn2 = get_duckdb_connection()
    different_after_close = conn1 is not conn2
    logger.info(f"Connection objects are different after closing: {different_after_close}")
    
    # Final cleanup
    close_all_connections()
    
    logger.info("Tests completed.") 