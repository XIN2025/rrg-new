from src.utils.duck_pool import get_duckdb_connection

with get_duckdb_connection() as conn:
    df = conn.execute("SELECT * FROM public.market_metadata WHERE symbol = 'CNX500'").fetchdf()
    if not df.empty:
        print('CNX500 FOUND:', df)
    else:
        print('CNX500 NOT FOUND') 
