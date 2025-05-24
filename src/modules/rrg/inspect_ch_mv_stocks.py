import os
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, project_root)
from src.utils.clickhouse_pool import pool

if __name__ == "__main__":
    # Query to get column names
    columns = pool.execute_query("DESCRIBE TABLE strike.mv_stocks")
    print("strike.mv_stocks columns:")
    for col in columns:
        print(col[0]) 
