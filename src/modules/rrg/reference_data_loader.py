import polars as pl
from datetime import datetime, timezone
from src.utils.metrics import TimerMetric, DuckDBQueryTimer
from src.utils.logger import get_logger
from src.utils.duck_pool import get_duckdb_connection
from src.utils.clickhouse import get_clickhouse_connection
import time as t
import logging

# Configure logger to only show errors
logger = get_logger("rrg_reference_data")
logger.setLevel(logging.ERROR)

class RRGReferenceDataLoader:
    def __init__(self):
        self._market_metadata_df = None
        self._metadata_loaded = False
        self._last_refresh_time = None
        self.ensure_metadata_loaded()

    def _load_metadata(self):
        """
        Internal function to load market metadata from DuckDB.
        """
        try:
            with get_duckdb_connection() as conn:
                with DuckDBQueryTimer("market_metadata_query"):
                    self._market_metadata_df = conn.sql(
                        """SELECT 
                            symbol, 
                            name, 
                            slug, 
                            ticker, 
                            security_code, 
                            security_type_code 
                        FROM public.market_metadata 
                        WHERE security_type_code IN (5, 26)"""
                    ).pl()
            
            return True
        except Exception as e:
            logger.error(f"[RRG Reference Data] Error loading metadata: {str(e)}", exc_info=True)
            return False

    def ensure_metadata_loaded(self):
        """
        Ensures that metadata is loaded if it hasn't been already.
        """
        with TimerMetric("ensure_metadata_loaded", "rrg_reference_data"):
            if self._metadata_loaded:
                return True
            
            success = self._load_metadata()
            
            if success:
                self._metadata_loaded = True
                self._last_refresh_time = datetime.now(timezone.utc)
                return True
            else:
                logger.error("[RRG Reference Data] Failed to load metadata")
                return False

    def refresh_metadata(self):
        """
        Forces a refresh of market metadata.
        """
        with TimerMetric("refresh_metadata", "rrg_reference_data"):
            success = self._load_metadata()
            
            if success:
                self._metadata_loaded = True
                self._last_refresh_time = datetime.now(timezone.utc)
                return True
            else:
                logger.error("[RRG Reference Data] Failed to refresh metadata")
                return False

    def get_metadata_status(self):
        """
        Returns the current status of metadata loading.
        """
        return {
            "loaded": self._metadata_loaded,
            "last_refresh": self._last_refresh_time.isoformat() if self._last_refresh_time else None
        }

def fetch_and_log_columns(ch_client):
    """Fetch and log the column names for strike.mv_indices and strike.mv_stocks."""
    try:
        indices_columns = ch_client.query("SELECT * FROM strike.mv_indices LIMIT 0").column_names
        stocks_columns = ch_client.query("SELECT * FROM strike.mv_stocks LIMIT 0").column_names
        logger.info("Columns in strike.mv_indices:")
        for col in indices_columns:
            logger.info(f"  {col}")
        logger.info("Columns in strike.mv_stocks:")
        for col in stocks_columns:
            logger.info(f"  {col}")
    except Exception as e:
        logger.error(f"Error fetching column names: {str(e)}", exc_info=True)

def load_market_metadata(dd_con=None, force=False):
    """Load market metadata from ClickHouse into DuckDB."""
    with get_duckdb_connection() as dd_con:
        # Drop and create table to ensure schema matches
        dd_con.execute("DROP TABLE IF EXISTS public.market_metadata;")
        dd_con.execute("""
            CREATE TABLE public.market_metadata (
                security_token VARCHAR,
                security_code VARCHAR,
                company_name VARCHAR,
                symbol VARCHAR,
                alternate_symbol VARCHAR,
                ticker VARCHAR,
                is_fno BOOLEAN,
                stocks_count INTEGER,
                stocks VARCHAR,
                security_codes VARCHAR,
                security_tokens VARCHAR,
                series VARCHAR,
                category VARCHAR,
                exchange_group VARCHAR,
                security_type_code INTEGER
            )
        """)

        with get_clickhouse_connection() as ch_client:
            # Fetch indices metadata
            indices_query = """
            SELECT 
                security_token,
                security_code,
                company_name,
                symbol,
                alternate_symbol,
                ticker,
                is_fno,
                stocks_count,
                stocks,
                security_codes,
                security_tokens,
                NULL as series,
                NULL as category,
                NULL as exchange_group,
                5 as security_type_code
            FROM strike.mv_indices
            """
            logger.info("Executing indices metadata query...")
            indices_result = ch_client.query(indices_query).result_rows
            logger.info(f"Fetched {len(indices_result)} indices records")

            # Fetch stocks metadata
            stocks_query = """
            SELECT 
                security_token,
                security_code,
                company_name,
                symbol,
                alternate_symbol,
                ticker,
                is_fno,
                NULL as stocks_count,
                NULL as stocks,
                NULL as security_codes,
                NULL as security_tokens,
                series,
                category,
                exchange_group,
                26 as security_type_code
            FROM strike.mv_stocks
            """
            logger.info("Executing stocks metadata query...")
            stocks_result = ch_client.query(stocks_query).result_rows
            logger.info(f"Fetched {len(stocks_result)} stocks records")

            # Combine results
            all_metadata = indices_result + stocks_result
            logger.info(f"Total records to insert: {len(all_metadata)}")

            if all_metadata:
                # Convert to DataFrame
                df = pl.DataFrame(all_metadata, schema=[
                    "security_token", "security_code", "company_name", "symbol", "alternate_symbol",
                    "ticker", "is_fno", "stocks_count", "stocks", "security_codes", "security_tokens",
                    "series", "category", "exchange_group", "security_type_code"
                ])

                # Insert into DuckDB
                dd_con.execute("BEGIN TRANSACTION")
                try:
                    dd_con.execute("INSERT INTO public.market_metadata SELECT * FROM df")
                    dd_con.execute("COMMIT")
                    logger.info("Successfully loaded market metadata into DuckDB")
                    return True
                except Exception as e:
                    dd_con.execute("ROLLBACK")
                    logger.error(f"Error loading market metadata: {str(e)}")
                    return False
            else:
                logger.warning("No market metadata records found to insert")
                return False

def load_stock_prices(dd_con=None, force=False):
    """Load stock prices from ClickHouse into DuckDB."""
    with get_duckdb_connection() as dd_con:
        # Drop and create table to ensure schema matches
        dd_con.execute("DROP TABLE IF EXISTS public.stock_prices;")
        dd_con.execute("""
            CREATE TABLE public.stock_prices (
                symbol VARCHAR,
                created_at TIMESTAMP,
                current_price DOUBLE,
                previous_close DOUBLE,
                security_code VARCHAR
            )
        """)

        with get_clickhouse_connection() as ch_client:
            # Fetch current stock prices (1min)
            current_prices_query = """
            SELECT 
                symbol,
                date_time as created_at,
                ltp as current_price,
                close as previous_close,
                security_code
            FROM strike.equity_prices_1min_last_traded
            WHERE ltp > 0
            """
            logger.info("Executing current stock prices query...")
            current_prices_result = ch_client.query(current_prices_query).result_rows
            logger.info(f"Fetched {len(current_prices_result)} current price records")

            # Fetch historical stock prices (1d)
            historical_prices_query = """
            SELECT 
                security_code,
                date_time as created_at,
                close as current_price,
                close as previous_close,
                NULL as symbol
            FROM strike.equity_prices_1d
            WHERE close > 0
            """
            logger.info("Executing historical stock prices query...")
            historical_prices_result = ch_client.query(historical_prices_query).result_rows
            logger.info(f"Fetched {len(historical_prices_result)} historical price records")

            # Combine results
            all_prices = current_prices_result + historical_prices_result
            logger.info(f"Total price records to insert: {len(all_prices)}")

            if all_prices:
                # Convert to DataFrame
                df = pl.DataFrame(all_prices, schema=[
                    "symbol", "created_at", "current_price", "previous_close", "security_code"
                ])

                # Insert into DuckDB
                dd_con.execute("BEGIN TRANSACTION")
                try:
                    dd_con.execute("INSERT INTO public.stock_prices SELECT * FROM df")
                    dd_con.execute("COMMIT")
                    logger.info("Successfully loaded stock prices into DuckDB")
                    return True
                except Exception as e:
                    dd_con.execute("ROLLBACK")
                    logger.error(f"Error loading stock prices: {str(e)}")
                    return False
            else:
                logger.warning("No stock prices data to load")
                return False

# ... existing code ... 
