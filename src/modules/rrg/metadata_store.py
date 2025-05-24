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
from typing import List, Optional

INDIAN_TZ = "Asia/Kolkata"

# Configure logger to show INFO level logs
logger = get_logger("rrg_metadata")
logger.setLevel(logging.INFO)

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
        self._price_data_days = 3650  # 10 years of historical data
        self._price_data = {}
        self.ensure_metadata_loaded()

    def _load_metadata(self):
        """Internal function to load metadata from DuckDB."""
        try:
            with get_duckdb_connection() as conn:
                # Query market_metadata table with correct schema
                self._indices_df = conn.sql("""
                    SELECT 
                        security_code, 
                        symbol,
                        ticker,
                        nse_index_name AS company_name,
                        security_type_code
                    FROM public.market_metadata 
                    WHERE symbol IS NOT NULL
                    AND security_type_code IN ('5', '26')  -- Indices
                """).pl()
                
                self._stocks_df = conn.sql("""
                    SELECT 
                        security_code, 
                        symbol,
                        ticker,
                        company_name,
                        security_type_code
                    FROM public.market_metadata 
                    WHERE symbol IS NOT NULL
                    AND security_type_code NOT IN ('5', '26')  -- Non-indices
                """).pl()
                
                # Load market metadata
                self._market_metadata_df = conn.sql("""
                    SELECT 
                        security_code,
                        symbol,
                        ticker,
                        COALESCE(company_name, nse_index_name) AS company_name,
                        security_type_code
                    FROM public.market_metadata
                    WHERE symbol IS NOT NULL
                """).pl()
                
                logger.info(f"Loaded {len(self._indices_df)} indices, {len(self._stocks_df)} stocks, and {len(self._market_metadata_df)} total market metadata records")
                return True
        except Exception as e:
            logger.error(f"[RRG Metadata] Error loading metadata: {str(e)}", exc_info=True)
            return False

    def _load_price_data(self, days=None):
        """Internal function to load price data from DuckDB."""
        try:
            if days is not None:
                self._price_data_days = days
            
            with get_duckdb_connection() as conn:
                # Load EOD stock data
                eod_query = f"""
                SELECT 
                    created_at,
                    ticker as symbol,
                    close_price,
                    security_code,
                    ticker
                FROM public.eod_stock_data
                WHERE created_at >= CURRENT_DATE - INTERVAL '{self._price_data_days} days'
                AND close_price > 0
                ORDER BY created_at ASC
                """
                self._stock_prices_df = conn.execute(eod_query).fetchdf()
                self._eod_stock_data_df = self._stock_prices_df.copy()
                logger.info(f"Loaded {len(self._stock_prices_df)} price records")
                return True
        except Exception as e:
            logger.error(f"[RRG Price Data] Error loading price data: {str(e)}")
            return False

    def ensure_metadata_loaded(self):
        """Ensures that metadata is loaded if it hasn't been already."""
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
        """Ensures that price data is loaded if it hasn't been already."""
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
        """Forces a refresh of all metadata tables."""
        with TimerMetric("refresh_metadata", "rrg_metadata"):
            success = self._load_metadata()
            
            if success:
                self._metadata_loaded = True
                self._last_refresh_time = datetime.now(timezone.utc)
                return True
            else:
                logger.error("[RRG Metadata] Failed to refresh metadata")
                return False

    def get_metadata_status(self):
        """Returns the current status of metadata loading."""
        return {
            "loaded": self._metadata_loaded,
            "last_refresh": self._last_refresh_time.isoformat() if self._last_refresh_time else None,
            "indices_count": len(self._indices_df) if self._indices_df is not None else 0,
            "stocks_count": len(self._stocks_df) if self._stocks_df is not None else 0,
            "market_metadata_count": len(self._market_metadata_df) if self._market_metadata_df is not None else 0
        }

    def get_price_data_status(self):
        """Returns the current status of price data loading."""
        return {
            "loaded": self._price_data_loaded,
            "last_refresh": self._last_price_refresh_time.isoformat() if self._last_price_refresh_time else None,
            "records_count": len(self._stock_prices_df) if self._stock_prices_df is not None else 0
        }

    def get_indices(self):
        """Returns a copy of the indices DataFrame."""
        self.ensure_metadata_loaded()
        return self._indices_df.clone() if self._indices_df is not None else None

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
        """Returns a copy of the stocks DataFrame."""
        self.ensure_metadata_loaded()
        return self._stocks_df.clone() if self._stocks_df is not None else None

    def get_market_metadata(self, symbols: List[str] = None) -> Optional[pl.DataFrame]:
        """Get market metadata for given symbols. If symbols is None, returns all metadata."""
        try:
            if self._market_metadata_df is None:
                self.ensure_metadata_loaded()
                if self._market_metadata_df is None:
                    return None
            
            if symbols:
                return self._market_metadata_df.filter(pl.col("symbol").is_in(symbols)).clone()
            return self._market_metadata_df.clone()
        except Exception as e:
            logger.error(f"Error getting market metadata: {str(e)}", exc_info=True)
            return None

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
                        DATE_TRUNC('hour', timestamp) as hour,
                        symbol,
                        FIRST_VALUE(close_price) OVER (
                            PARTITION BY symbol, DATE_TRUNC('hour', timestamp)
                            ORDER BY timestamp DESC
                        ) as close_price,
                        FIRST_VALUE(security_code) OVER (
                            PARTITION BY symbol, DATE_TRUNC('hour', timestamp)
                            ORDER BY timestamp DESC
                        ) as security_code,
                        FIRST_VALUE(timestamp) OVER (
                            PARTITION BY symbol, DATE_TRUNC('hour', timestamp)
                            ORDER BY timestamp DESC
                        ) as created_at,
                        FIRST_VALUE(ticker) OVER (
                            PARTITION BY symbol, DATE_TRUNC('hour', timestamp)
                            ORDER BY timestamp DESC
                        ) as ticker
                    FROM public.hourly_stock_data
                    WHERE timestamp >= CURRENT_DATE - INTERVAL '{filter_days}' DAY
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
                        ticker
                    FROM hourly_prices
                ),
                final_prices AS (
                    SELECT 
                        created_at,
                        symbol,
                        close_price,
                        security_code,
                        ticker,
                        ROW_NUMBER() OVER (PARTITION BY symbol, DATE_TRUNC('hour', created_at) ORDER BY created_at DESC) as rn
                    FROM unique_prices
                )
                SELECT 
                    created_at,
                    symbol,
                    close_price,
                    security_code,
                    ticker
                FROM final_prices
                WHERE rn = 1
                ORDER BY created_at DESC, symbol
                """
                
                # Query for EOD stock data with proper deduplication
                eod_stock_query = f"""
                WITH daily_prices AS (
                    SELECT 
                        DATE_TRUNC('day', timestamp) as day,
                        symbol,
                        FIRST_VALUE(close_price) OVER (
                            PARTITION BY symbol, DATE_TRUNC('day', timestamp)
                            ORDER BY timestamp DESC
                        ) as close_price,
                        FIRST_VALUE(security_code) OVER (
                            PARTITION BY symbol, DATE_TRUNC('day', timestamp)
                            ORDER BY timestamp DESC
                        ) as security_code,
                        FIRST_VALUE(timestamp) OVER (
                            PARTITION BY symbol, DATE_TRUNC('day', timestamp)
                            ORDER BY timestamp DESC
                        ) as created_at,
                        FIRST_VALUE(ticker) OVER (
                            PARTITION BY symbol, DATE_TRUNC('day', timestamp)
                            ORDER BY timestamp DESC
                        ) as ticker
                    FROM public.hourly_stock_data
                    WHERE timestamp >= CURRENT_DATE - INTERVAL '{filter_days}' DAY
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
                        ticker
                    FROM daily_prices
                ),
                final_prices AS (
                    SELECT 
                        created_at,
                        symbol,
                        close_price,
                        security_code,
                        ticker,
                        ROW_NUMBER() OVER (PARTITION BY symbol, DATE_TRUNC('day', created_at) ORDER BY created_at DESC) as rn
                    FROM unique_prices
                )
                SELECT 
                    created_at,
                    symbol,
                    close_price,
                    security_code,
                    ticker
                FROM final_prices
                WHERE rn = 1
                ORDER BY created_at DESC, symbol
                """
                
                # Execute queries
                stock_prices = dd_con.execute(stock_price_query).fetchall()
                eod_stock_data = dd_con.execute(eod_stock_query).fetchall()
                
                # Convert to DataFrames
                df1 = pl.DataFrame(stock_prices, schema=["created_at", "symbol", "close_price", "security_code", "ticker"])
                df2 = pl.DataFrame(eod_stock_data, schema=["created_at", "symbol", "close_price", "security_code", "ticker"])
                
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
                required_columns = ["created_at", "symbol", "close_price", "security_code", "name", "slug", "security_type_code", "ticker"]
                for col in required_columns:
                    if col not in df.columns:
                        raise ValueError(f"Missing required column: {col}")
                
                return df
                
            except Exception as e:
                logger.error(f"Error getting stock prices: {str(e)}", exc_info=True)
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

    def test_metadata_loading(self):
        """Test method to verify metadata loading functionality"""
        try:
            # Force refresh metadata
            self.refresh_metadata()
            
            # Verify indices data
            indices_df = self.get_indices()
            if indices_df is None or indices_df.is_empty():
                logger.error("Failed to load indices data")
                return False
            logger.info(f"Successfully loaded {len(indices_df)} indices")
            
            # Verify stocks data
            stocks_df = self.get_stocks()
            if stocks_df is None or stocks_df.is_empty():
                logger.error("Failed to load stocks data")
                return False
            logger.info(f"Successfully loaded {len(stocks_df)} stocks")
            
            # Verify market metadata
            market_metadata_df = self.get_market_metadata()
            if market_metadata_df is None or market_metadata_df.is_empty():
                logger.error("Failed to load market metadata")
                return False
            logger.info(f"Successfully loaded {len(market_metadata_df)} market metadata records")
            
            return True
        except Exception as e:
            logger.error(f"Error testing metadata loading: {str(e)}", exc_info=True)
            return False



# 


