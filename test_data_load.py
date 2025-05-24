import duckdb
from src.utils.clickhouse_pool import pool as ClickHousePool
from src.utils.duck_pool import get_duckdb_connection

def test_data_load():
    # First, create a fresh DuckDB connection
    conn = duckdb.connect('data/pydb.duckdb')
    
    # Create schema first
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    
    # Create the table with all required columns
    conn.execute("""
        CREATE TABLE IF NOT EXISTS public.eod_stock_data (
            created_at TIMESTAMP,
            ratio DECIMAL(18,6),
            momentum DECIMAL(18,6),
            close_price DECIMAL(18,2),
            change_percentage DECIMAL(18,6),
            metric_1 DECIMAL(18,6),
            metric_2 DECIMAL(18,6),
            metric_3 DECIMAL(18,6),
            signal INTEGER,
            security_code VARCHAR,
            previous_close DECIMAL(18,2),
            ticker VARCHAR
        )
    """)
    
    # Query ClickHouse for a small sample
    query = """
        WITH ordered_data AS (
            SELECT
                e.date_time as created_at,
                e.close / NULLIF(lagInFrame(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time), 0) as ratio,
                e.close / NULLIF(avg(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time ROWS BETWEEN 20 PRECEDING AND CURRENT ROW), 0) as momentum,
                e.close as close_price,
                (e.close - NULLIF(lagInFrame(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time), 0)) / NULLIF(lagInFrame(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time), 0) * 100 as change_percentage,
                e.close / NULLIF(avg(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), 0) as metric_1,
                e.close / NULLIF(avg(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time ROWS BETWEEN 10 PRECEDING AND CURRENT ROW), 0) as metric_2,
                e.close / NULLIF(avg(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time ROWS BETWEEN 15 PRECEDING AND CURRENT ROW), 0) as metric_3,
                CASE 
                    WHEN e.close > lagInFrame(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time) THEN 1
                    WHEN e.close < lagInFrame(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time) THEN 2
                    ELSE 0
                END as signal,
                s.security_code,
                lagInFrame(e.close) OVER (PARTITION BY s.security_code ORDER BY e.date_time) as previous_close,
                s.ticker
            FROM strike.equity_prices_1d e
            JOIN strike.mv_stocks s ON e.security_code = s.security_code
            WHERE e.date_time >= now() - INTERVAL 5 DAY
            AND e.close > 0
            ORDER BY e.date_time DESC
            LIMIT 10
        )
        SELECT * FROM ordered_data
    """
    
    print("Fetching data from ClickHouse...")
    result = ClickHousePool.execute_query(query)
    
    if not result:
        print("No data returned from ClickHouse")
        return
    
    print("\nSample data from ClickHouse:")
    for row in result[:2]:
        print(row)
    
    # Insert into DuckDB
    print("\nInserting into DuckDB...")
    conn.executemany("""
        INSERT INTO public.eod_stock_data 
        (created_at, ratio, momentum, close_price, change_percentage, metric_1, metric_2, metric_3, signal, security_code, previous_close, ticker)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, result)
    
    # Verify the data in DuckDB
    print("\nVerifying data in DuckDB:")
    sample = conn.execute("""
        SELECT * FROM public.eod_stock_data 
        LIMIT 5
    """).fetchdf()
    print(sample)
    
    # Print column names and types
    print("\nTable schema:")
    schema = conn.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'eod_stock_data'
    """).fetchdf()
    print(schema)

if __name__ == "__main__":
    test_data_load() 
