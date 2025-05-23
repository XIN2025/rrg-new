"""
RRG Metadata Store

This module loads and maintains in-memory copies of metadata tables
from DuckDB that are used by the RRG module. It provides functions
to access clones of the data to ensure the original is not modified.
"""
import polars as pl
from datetime import datetime, timezone, timedelta
from src.utils.metrics import TimerMetric, DuckDBQueryTimer
from src.utils.logger import get_logger
from src.utils.duck_pool import get_duckdb_connection
from src.modules.rrg.time_utils import return_filter_days
import time as t
import logging

INDIAN_TZ = "Asia/Kolkata"

# Configure logger to only show errors
logger = get_logger("rrg_metadata")
logger.setLevel(logging.ERROR)

# Create a singleton instance
_metadata_store = None

def get_metadata_store():
    """Get or create the singleton instance of RRGMetadataStore"""
    global _metadata_store
    if _metadata_store is None:
        _metadata_store = RRGMetadataStore()
    return _metadata_store

# Expose all required functions at module level
def ensure_metadata_loaded():
    """Ensures that metadata is loaded if it hasn't been already"""
    return get_metadata_store().ensure_metadata_loaded()

def get_metadata_status():
    """Get the current status of metadata loading"""
    return get_metadata_store().get_metadata_status()

def refresh_metadata():
    """Forces a refresh of all metadata tables"""
    return get_metadata_store().refresh_metadata()

def ensure_price_data_loaded(days=None):
    """Ensures that price data is loaded if it hasn't been already"""
    return get_metadata_store().ensure_price_data_loaded(days)

def get_price_data_status():
    """Get the current status of price data loading"""
    return get_metadata_store().get_price_data_status()

class RRGMetadataStore:
    def __init__(self):
        self._indices_df = None
        self._indices_stocks_df = None
        self._companies_df = None
        self._stocks_df = None
        self._market_metadata_df = None
        self._metadata_loaded = False
        self._last_refresh_time = None
        self._stock_prices_df = None
        self._eod_stock_data_df = None
        self._price_data_loaded = False
        self._last_price_refresh_time = None
        self._price_data_days = 3650  # Increased to 10 years to ensure complete historical data
        self.ensure_metadata_loaded()

    def _load_metadata(self):
        """
        Internal function to load all metadata from DuckDB.
        This is called by ensure_metadata_loaded and refresh_metadata.
        """
        conn = get_duckdb_connection()
        
        try:
            with DuckDBQueryTimer("indices_query"):
                self._indices_df = conn.sql("SELECT security_code, name, slug, symbol FROM public.indices").pl()
            
            with DuckDBQueryTimer("indices_stocks_query"):
                self._indices_stocks_df = conn.sql("SELECT security_code FROM public.indices_stocks").pl()
            
            with DuckDBQueryTimer("companies_query"):
                self._companies_df = conn.sql("SELECT slug, name FROM public.companies").pl()
            
            with DuckDBQueryTimer("stocks_query"):
                self._stocks_df = conn.sql("SELECT company_name, security_code FROM public.stocks").pl()
            
            with DuckDBQueryTimer("market_metadata_query"):
                self._market_metadata_df = conn.sql("SELECT symbol, name, slug, ticker, security_code, security_type_code FROM public.market_metadata").pl()
            
            return True
        except Exception as e:
            logger.error(f"[RRG Metadata] Error loading metadata: {str(e)}", exc_info=True)
            return False

    def _load_price_data(self, days=None):
        """
        Internal function to load stock price and EOD data from DuckDB.
        
        Args:
            days: Number of days of historical data to load (defaults to _price_data_days)
        """
        if days is not None:
            self._price_data_days = days
        
        conn = get_duckdb_connection()
        
        try:
            load_start = t.time()
            
            # First get the list of index symbols from market_metadata
            index_symbols = conn.sql(
                """SELECT DISTINCT symbol 
                   FROM public.market_metadata 
                   WHERE security_type_code IN (5, 26)"""
            ).pl()["symbol"].to_list()
            
            if not index_symbols:
                raise ValueError("No index symbols found in market_metadata")
            
            # Load index data first
            with DuckDBQueryTimer("index_prices_query"):
                self._stock_prices_df = conn.sql(
                    f"""SELECT 
                        created_at,
                        symbol,
                        current_price as close_price,
                        security_code,
                        previous_close,
                        ticker
                    FROM public.stock_prices
                    WHERE created_at >= CURRENT_DATE - INTERVAL '{self._price_data_days} days'
                    AND symbol IN ({','.join([f"'{s}'" for s in index_symbols])})
                    AND current_price > 0  -- Ensure positive prices
                    ORDER BY created_at ASC"""
                ).pl()
            
            # Load EOD data for indices
            with DuckDBQueryTimer("eod_index_data_query"):
                self._eod_stock_data_df = conn.sql(
                    f"""SELECT 
                        created_at,
                        symbol,
                        close_price,
                        security_code,
                        previous_close,
                        ticker
                    FROM public.eod_stock_data 
                    WHERE created_at >= CURRENT_DATE - INTERVAL '{self._price_data_days} days'
                    AND symbol IN ({','.join([f"'{s}'" for s in index_symbols])})
                    AND close_price > 0  -- Ensure positive prices
                    ORDER BY created_at ASC"""
                ).pl()
            
            # Join with market_metadata to get additional fields
            self._stock_prices_df = self._stock_prices_df.join(
                conn.sql("SELECT symbol, name, slug, security_type_code FROM public.market_metadata").pl(),
                on="symbol",
                how="left"
            )
            
            self._eod_stock_data_df = self._eod_stock_data_df.join(
                conn.sql("SELECT symbol, name, slug, security_type_code FROM public.market_metadata").pl(),
                on="symbol",
                how="left"
            )
            
            # Handle timezone conversions
            self._stock_prices_df = self._stock_prices_df.with_columns(
                pl.col("created_at").dt.replace_time_zone("UTC").dt.convert_time_zone("Asia/Kolkata").alias("created_at")
            )
            self._eod_stock_data_df = self._eod_stock_data_df.with_columns(
                pl.col("created_at").dt.replace_time_zone("UTC").dt.convert_time_zone(INDIAN_TZ).alias("created_at")
            )
            
            # Remove timezone info from stock prices
            self._stock_prices_df = self._stock_prices_df.with_columns(
                pl.col("created_at").dt.replace_time_zone(None).alias("created_at")
            )
            
            # Process EOD data dates
            self._eod_stock_data_df = self._eod_stock_data_df.with_columns(
                pl.col("created_at").dt.replace_time_zone("UTC").dt.convert_time_zone(INDIAN_TZ)
            )
            self._eod_stock_data_df = self._eod_stock_data_df.with_columns(
                pl.col("created_at").dt.date().cast(pl.Datetime).alias("created_at")
            )
            
            # Add one day to EOD dates
            self._eod_stock_data_df = self._eod_stock_data_df.with_columns(
                (pl.col("created_at") + pl.duration(days=1)).alias("created_at")
            )
            
            # Format slugs properly
            for df in [self._stock_prices_df, self._eod_stock_data_df]:
                if "slug" in df.columns:
                    df = df.with_columns([
                        pl.col("slug").str.replace(" ", "").str.replace("-", "").str.replace("&", "").str.to_lowercase().alias("slug")
                    ])
            
            # Validate data
            # Check for required columns
            required_columns = ["created_at", "symbol", "close_price", "security_code", "ticker", "name", "slug", "security_type_code"]
            for df_name, df in [("stock_prices", self._stock_prices_df), ("eod_stock_data", self._eod_stock_data_df)]:
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    raise ValueError(f"Missing required columns in {df_name}: {missing_columns}")
            
            # Check for valid security type codes
            for df_name, df in [("stock_prices", self._stock_prices_df), ("eod_stock_data", self._eod_stock_data_df)]:
                invalid_types = df.filter(~pl.col("security_type_code").is_in([5, 26]))["symbol"].unique()
                if len(invalid_types) > 0:
                    raise ValueError(f"Invalid security type codes in {df_name} for symbols: {invalid_types}")
            
            # Check for valid prices
            for df_name, df in [("stock_prices", self._stock_prices_df), ("eod_stock_data", self._eod_stock_data_df)]:
                invalid_prices = df.filter(pl.col("close_price") <= 0)
                if len(invalid_prices) > 0:
                    raise ValueError(f"Found non-positive prices in {df_name}")
            
            return True
        except Exception as e:
            logger.error(f"[RRG Price Data] Error loading price data: {str(e)}", exc_info=True)
            return False

    def ensure_metadata_loaded(self):
        """
        Ensures that metadata is loaded if it hasn't been already.
        """
        with TimerMetric("ensure_metadata_loaded", "rrg_metadata"):
            if self._metadata_loaded:
                return True
            
            success = self._load_metadata()
            
            if success:
                self._metadata_loaded = True
                self._last_refresh_time = datetime.now(timezone.utc)
                return True
            else:
                logger.error("[RRG Metadata] Failed to load metadata")
                return False

    def ensure_price_data_loaded(self, days=None):
        """
        Ensures that price data is loaded if it hasn't been already.
        
        Args:
            days: Optional number of days of historical data to load
        """
        with TimerMetric("ensure_price_data_loaded", "rrg_metadata"):
            if self._price_data_loaded:
                return True
            
            success = self._load_price_data(days)
            
            if success:
                self._price_data_loaded = True
                self._last_price_refresh_time = datetime.now(timezone.utc)
                return True
            else:
                logger.error("[RRG Price Data] Failed to load price data")
                return False

    def refresh_metadata(self):
        """
        Forces a refresh of all metadata tables.
        """
        with TimerMetric("refresh_metadata", "rrg_metadata"):
            success = self._load_metadata()
            
            if success:
                self._metadata_loaded = True
                self._last_refresh_time = datetime.now(timezone.utc)
                return True
            else:
                logger.error("[RRG Metadata] Failed to refresh metadata")
                return False

    def refresh_price_data(self, days=None):
        """
        Forces a refresh of price data.
        
        Args:
            days: Optional number of days of historical data to load
        """
        with TimerMetric("refresh_price_data", "rrg_metadata"):
            success = self._load_price_data(days)
            
            if success:
                self._price_data_loaded = True
                self._last_price_refresh_time = datetime.now(timezone.utc)
                return True
            else:
                logger.error("[RRG Price Data] Failed to refresh price data")
                return False

    def get_metadata_status(self):
        """
        Returns the current status of metadata loading.
        """
        return {
            "loaded": self._metadata_loaded,
            "last_refresh": self._last_refresh_time.isoformat() if self._last_refresh_time else None
        }

    def get_price_data_status(self):
        """
        Returns the current status of price data loading.
        """
        return {
            "loaded": self._price_data_loaded,
            "last_refresh": self._last_price_refresh_time.isoformat() if self._last_price_refresh_time else None
        }

    def get_indices(self):
        """
        Returns a copy of the indices DataFrame.
        """
        self.ensure_metadata_loaded()
        return self._indices_df.clone()

    def get_indices_stocks(self):
        """
        Returns a copy of the indices_stocks DataFrame.
        """
        self.ensure_metadata_loaded()
        return self._indices_stocks_df.clone()

    def get_companies(self):
        """
        Returns a copy of the companies DataFrame.
        """
        self.ensure_metadata_loaded()
        return self._companies_df.clone()

    def get_stocks(self):
        """
        Returns a copy of the stocks DataFrame.
        """
        self.ensure_metadata_loaded()
        return self._stocks_df.clone()

    def get_market_metadata(self, symbols=None):
        """
        Get market metadata for specified symbols.
        If symbols is None, returns all metadata.
        """
        try:
            if symbols is None:
                return self._market_metadata_df.clone()
            
            # Convert symbols to list if it's a single string
            if isinstance(symbols, str):
                symbols = [symbols]
            
            # Filter metadata for specified symbols
            return self._market_metadata_df.filter(pl.col("symbol").is_in(symbols)).clone()
        except Exception as e:
            logger.error(f"Error getting market metadata: {str(e)}", exc_info=True)
            raise

    def get_stock_prices(self, symbols, timeframe="daily", filter_days=None):
        """Get stock prices from DuckDB."""
        with TimerMetric("get_stock_prices", "rrg_metadata"):
            try:
                start_time = t.time()
                
                # Get DuckDB connection
                dd_con = get_duckdb_connection()
                
                # Calculate filter days if not provided
                if filter_days is None:
                    filter_days = return_filter_days(timeframe)
                
                # Get index symbols
                index_symbols = dd_con.execute("""
                    SELECT DISTINCT symbol 
                    FROM public.market_metadata 
                    WHERE security_type_code IN ('5', '26')
                """).fetchall()
                
                # Query for stock prices with proper deduplication
                stock_price_query = f"""
                WITH hourly_prices AS (
                    SELECT 
                        DATE_TRUNC('hour', created_at) as hour,
                        symbol,
                        FIRST_VALUE(current_price) OVER (
                            PARTITION BY symbol, DATE_TRUNC('hour', created_at)
                            ORDER BY created_at DESC
                        ) as close_price,
                        FIRST_VALUE(security_code) OVER (
                            PARTITION BY symbol, DATE_TRUNC('hour', created_at)
                            ORDER BY created_at DESC
                        ) as security_code,
                        FIRST_VALUE(created_at) OVER (
                            PARTITION BY symbol, DATE_TRUNC('hour', created_at)
                            ORDER BY created_at DESC
                        ) as created_at,
                        FIRST_VALUE(previous_close) OVER (
                            PARTITION BY symbol, DATE_TRUNC('hour', created_at)
                            ORDER BY created_at DESC
                        ) as previous_close,
                        FIRST_VALUE(ticker) OVER (
                            PARTITION BY symbol, DATE_TRUNC('hour', created_at)
                            ORDER BY created_at DESC
                        ) as ticker
                    FROM public.stock_prices
                    WHERE created_at >= CURRENT_DATE - INTERVAL '{filter_days}' DAY
                    AND symbol IN ({','.join([f"'{s[0]}'" for s in index_symbols])})
                    AND current_price > 0
                    AND current_price < 1000000
                ),
                unique_prices AS (
                    SELECT DISTINCT
                        created_at,
                        symbol,
                        close_price,
                        security_code,
                        previous_close,
                        ticker
                    FROM hourly_prices
                ),
                final_prices AS (
                    SELECT 
                        created_at,
                        symbol,
                        close_price,
                        security_code,
                        previous_close,
                        ticker,
                        ROW_NUMBER() OVER (PARTITION BY symbol, DATE_TRUNC('hour', created_at) ORDER BY created_at DESC) as rn
                    FROM unique_prices
                )
                SELECT 
                    created_at,
                    symbol,
                    close_price,
                    security_code,
                    previous_close,
                    ticker
                FROM final_prices
                WHERE rn = 1
                ORDER BY created_at DESC, symbol
                """
                
                # Query for EOD stock data with proper deduplication
                eod_stock_query = f"""
                WITH daily_prices AS (
                    SELECT 
                        DATE_TRUNC('day', created_at) as day,
                        symbol,
                        FIRST_VALUE(close_price) OVER (
                            PARTITION BY symbol, DATE_TRUNC('day', created_at)
                            ORDER BY created_at DESC
                        ) as close_price,
                        FIRST_VALUE(security_code) OVER (
                            PARTITION BY symbol, DATE_TRUNC('day', created_at)
                            ORDER BY created_at DESC
                        ) as security_code,
                        FIRST_VALUE(created_at) OVER (
                            PARTITION BY symbol, DATE_TRUNC('day', created_at)
                            ORDER BY created_at DESC
                        ) as created_at,
                        FIRST_VALUE(previous_close) OVER (
                            PARTITION BY symbol, DATE_TRUNC('day', created_at)
                            ORDER BY created_at DESC
                        ) as previous_close,
                        FIRST_VALUE(ticker) OVER (
                            PARTITION BY symbol, DATE_TRUNC('day', created_at)
                            ORDER BY created_at DESC
                        ) as ticker
                    FROM public.eod_stock_data
                    WHERE created_at >= CURRENT_DATE - INTERVAL '{filter_days}' DAY
                    AND symbol IN ({','.join([f"'{s[0]}'" for s in index_symbols])})
                    AND close_price > 0
                    AND close_price < 1000000
                ),
                unique_prices AS (
                    SELECT DISTINCT
                        created_at,
                        symbol,
                        close_price,
                        security_code,
                        previous_close,
                        ticker
                    FROM daily_prices
                ),
                final_prices AS (
                    SELECT 
                        created_at,
                        symbol,
                        close_price,
                        security_code,
                        previous_close,
                        ticker,
                        ROW_NUMBER() OVER (PARTITION BY symbol, DATE_TRUNC('day', created_at) ORDER BY created_at DESC) as rn
                    FROM unique_prices
                )
                SELECT 
                    created_at,
                    symbol,
                    close_price,
                    security_code,
                    previous_close,
                    ticker
                FROM final_prices
                WHERE rn = 1
                ORDER BY created_at DESC, symbol
                """
                
                # Execute queries
                stock_prices = dd_con.execute(stock_price_query).fetchall()
                eod_stock_data = dd_con.execute(eod_stock_query).fetchall()
                
                # Convert to DataFrames
                df1 = pl.DataFrame(stock_prices, schema=["created_at", "symbol", "close_price", "security_code", "previous_close", "ticker"])
                df2 = pl.DataFrame(eod_stock_data, schema=["created_at", "symbol", "close_price", "security_code", "previous_close", "ticker"])
                
                # Combine and sort
                df = pl.concat([df1, df2])
                
                # Remove duplicates based on symbol and timestamp
                df = df.unique(subset=["symbol", "created_at"], keep="last")
                
                # Sort by timestamp and symbol
                df = df.sort(["created_at", "symbol"])
                
                # Join with market_metadata to get additional fields
                metadata_df = dd_con.execute("""
                    SELECT symbol, name, slug, security_type_code, ticker
                    FROM public.market_metadata
                """).fetchall()
                metadata_df = pl.DataFrame(metadata_df, schema=["symbol", "name", "slug", "security_type_code", "ticker"])
                
                df = df.join(metadata_df, on="symbol", how="left")
                
                # Validate data
                if df.is_empty():
                    raise ValueError("No price data found")
                
                # Check for required columns
                required_columns = ["created_at", "symbol", "close_price", "security_code", "name", "slug", "security_type_code", "ticker", "previous_close"]
                for col in required_columns:
                    if col not in df.columns:
                        raise ValueError(f"Missing required column: {col}")
                
                # Filter out rows with null values
                df = df.filter(pl.col("close_price").is_not_null())
                
                # Format the data but keep datetime as datetime
                df = df.with_columns([
                    pl.col("close_price").round(2).cast(pl.Utf8),
                    pl.col("previous_close").round(2).cast(pl.Utf8),
                    pl.col("security_type_code").cast(pl.Utf8)
                ])
                
                return df
                
            except Exception as e:
                logger.error(f"[RRG Price Data] Error loading price data: {str(e)}", exc_info=True)
                raise

    def get_eod_stock_data(self, tickers=None, symbols=None, filter_days=None):
        """
        Returns EOD stock data for the specified tickers and symbols.
        
        Args:
            tickers: Optional list of tickers to get data for
            symbols: Optional list of symbols to get data for
            filter_days: Optional number of days of historical data to return
        """
        self.ensure_price_data_loaded()
        
        # Filter the data
        df = self._eod_stock_data_df.filter(
            (pl.col("ticker").is_in(tickers) if tickers else True) &
            (pl.col("symbol").is_in(symbols) if symbols else True)
        )
        
        # Filter by date if needed
        if filter_days:
            cutoff_date = datetime.now() - timedelta(days=filter_days)
            df = df.filter(pl.col("created_at") >= cutoff_date)
        
        return df.clone()
