import subprocess
import sys
import os

# Add the project root directory to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

def write_output(message):
    subprocess.run(['powershell', '-Command', f'Write-Host "{message}"'])

write_output('DEBUG SCRIPT STARTED')
try:
    from src.utils.duck_pool import get_duckdb_connection
    from src.utils.clickhouse_pool import pool
    import pandas as pd

    # Sample tickers from a typical RRG request
    SAMPLE_TICKERS = [
        "CNX Bank", "CNX 100", "CNX 200", "CNX500", "CNX Auto", "CNX Commo", "CNX Consum", "CNX Energy", "CNX FMCG",
        "CNX Infra", "CNX IT", "CNX Media", "CNX Metal", "CNX Midcap", "CNX MNC", "CNX Pharma", "CNX PSE", "CNX PSU Bank",
        "CNX Realty", "CNX Smallcap", "CPSE", "CNX Fin", "Nifty MCap50", "NIFMCSELECT", "NIFINDIAMANU", "NiftyMCap150",
        "NIFMICCAP250", "Nifty MSC400", "NiftySCap250", "Nifty SCap50", "NIFTOTALMAR", "Nifty CD", "Nifty Health",
        "Nifty LMC250", "NSEMID", "NIFTY OILGAS", "Nifty PBI", "NSE Index"
    ]

    def check_data():
        write_output("\nSample tickers from RRG request:")
        write_output(str(SAMPLE_TICKERS))
        write_output("\nChecking ClickHouse data first:")
        clickhouse_query = """
            SELECT 
                e.date_time as created_at,
                e.close as close_price,
                s.security_code,
                s.ticker,
                s.symbol
            FROM strike.equity_prices_1d e
            JOIN strike.mv_stocks s ON e.security_code = s.security_code
            WHERE s.symbol = 'CNX500'
            AND e.date_time >= now() - interval '3 months'
            LIMIT 5
        """
        try:
            write_output("Running ClickHouse query...")
            result = pool.execute_query(clickhouse_query)
            if not result:
                write_output("ClickHouse query returned no results.")
            else:
                write_output("\nClickHouse data sample:")
                write_output(str(pd.DataFrame(result)))
        except Exception as e:
            write_output(f"Error querying ClickHouse: {str(e)}")

        write_output("\nNow checking DuckDB data:")
        try:
            with get_duckdb_connection() as conn:
                write_output("\nChecking eod_stock_data:")
                eod_count = conn.execute("SELECT COUNT(*) FROM public.eod_stock_data").fetchone()[0]
                write_output(f"Total records: {eod_count}")
                write_output("Unique tickers in eod_stock_data:")
                tickers = conn.execute("SELECT DISTINCT ticker FROM public.eod_stock_data").fetchdf()
                write_output(str(tickers))
                write_output("Unique security_codes in eod_stock_data:")
                codes = conn.execute("SELECT DISTINCT security_code FROM public.eod_stock_data").fetchdf()
                write_output(str(codes))

                write_output("\nSample of eod_stock_data:")
                eod_sample = conn.execute("SELECT * FROM public.eod_stock_data LIMIT 5").fetchdf()
                write_output(str(eod_sample))

                write_output("\nChecking market_metadata:")
                meta_count = conn.execute("SELECT COUNT(*) FROM public.market_metadata").fetchone()[0]
                write_output(f"Total records: {meta_count}")
                write_output("Unique tickers in market_metadata:")
                meta_tickers = conn.execute("SELECT DISTINCT ticker FROM public.market_metadata").fetchdf()
                write_output(str(meta_tickers))
                write_output("Unique symbols in market_metadata:")
                meta_symbols = conn.execute("SELECT DISTINCT symbol FROM public.market_metadata").fetchdf()
                write_output(str(meta_symbols))

                write_output("\nSample of market_metadata:")
                meta_sample = conn.execute("SELECT * FROM public.market_metadata LIMIT 5").fetchdf()
                write_output(str(meta_sample))

                write_output("\nChecking CNX500 data in eod_stock_data:")
                cnx500_data = conn.execute("""
                    SELECT e.*, s.ticker, s.symbol 
                    FROM public.eod_stock_data e
                    JOIN public.market_metadata s ON e.security_code = s.security_code
                    WHERE s.symbol = 'CNX500'
                    LIMIT 5
                """).fetchdf()
                if cnx500_data.empty:
                    write_output("No CNX500 data found in eod_stock_data.")
                else:
                    write_output(str(cnx500_data))

                # Compare sample tickers to unique tickers in DB
                write_output("\nComparing sample tickers to unique tickers in eod_stock_data:")
                db_tickers = set(tickers['ticker'].dropna())
                for t in SAMPLE_TICKERS:
                    if t in db_tickers:
                        write_output(f"FOUND: {t}")
                    else:
                        write_output(f"MISSING: {t}")
        except Exception as e:
            write_output(f"Error querying DuckDB: {str(e)}")

    if __name__ == "__main__":
        check_data()
except Exception as e:
    write_output(f"FATAL ERROR: {e}") 
