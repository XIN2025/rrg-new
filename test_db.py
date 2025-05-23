import os
import sys
import logging

# Add the current directory and src to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from src.modules.db.data_manager import test_database_setup, load_data
from src.utils.clickhouse_pool import ClickHousePool

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def print_clickhouse_table_schema(table_name):
    query = f"DESCRIBE TABLE {table_name}"
    try:
        result = ClickHousePool.execute_query(query)
        logging.info(f"Schema for {table_name}:")
        for row in result:
            logging.info(row)
    except Exception as e:
        logging.error(f"Error describing {table_name}: {e}")

def main():
    logging.info("Starting database test...")
    if not test_database_setup():
        logging.error("Database setup test failed")
        return
    if not load_data(force=True):
        logging.error("Data loading failed")
        return
    logging.info("All tests completed successfully!")
    # Optionally, print schemas for debugging
    # print_clickhouse_table_schema("strike.mv_indices")
    # print_clickhouse_table_schema("strike.mv_stocks")

if __name__ == "__main__":
    main() 
