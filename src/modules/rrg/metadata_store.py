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

INDIAN_TZ = "Asia/Kolkata"

logger = get_logger("rrg_metadata")

_indices_df = None
_indices_stocks_df = None
_companies_df = None
_stocks_df = None
_market_metadata_df = None
_metadata_loaded = False
_last_refresh_time = None

_stock_prices_df = None
_eod_stock_data_df = None
_price_data_loaded = False
_last_price_refresh_time = None
_price_data_days = 3650  # Increased to 10 years to ensure complete historical data

def _load_metadata():
    """
    Internal function to load all metadata from DuckDB.
    This is called by ensure_metadata_loaded and refresh_metadata.
    """
    global _indices_df, _indices_stocks_df, _companies_df, _stocks_df, _market_metadata_df
    
    conn = get_duckdb_connection()
    logger.debug("[RRG Metadata] Connected to DuckDB")
    
    try:
        with DuckDBQueryTimer("indices_query"):
            _indices_df = conn.sql("SELECT security_code, name, slug, symbol FROM public.indices").pl()
        logger.info(f"[RRG Metadata] Loaded {len(_indices_df)} indices")
        
        with DuckDBQueryTimer("indices_stocks_query"):
            _indices_stocks_df = conn.sql("SELECT security_code FROM public.indices_stocks").pl()
        logger.info(f"[RRG Metadata] Loaded {len(_indices_stocks_df)} indices_stocks entries")
        
        with DuckDBQueryTimer("companies_query"):
            _companies_df = conn.sql("SELECT slug, name FROM public.companies").pl()
        logger.info(f"[RRG Metadata] Loaded {len(_companies_df)} companies")
        
        with DuckDBQueryTimer("stocks_query"):
            _stocks_df = conn.sql("SELECT company_name, security_code FROM public.stocks").pl()
        logger.info(f"[RRG Metadata] Loaded {len(_stocks_df)} stocks")
        
        with DuckDBQueryTimer("market_metadata_query"):
            _market_metadata_df = conn.sql("SELECT symbol, name, slug, ticker, security_code, security_type_code FROM public.market_metadata").pl()
        logger.info(f"[RRG Metadata] Loaded {len(_market_metadata_df)} market metadata entries")

        
        return True
    except Exception as e:
        logger.error(f"[RRG Metadata] Error loading metadata: {str(e)}", exc_info=True)
        return False

def _load_price_data(days=None):
    """
    Internal function to load stock price and EOD data from DuckDB.
    
    Args:
        days: Number of days of historical data to load (defaults to _price_data_days)
    """
    global _stock_prices_df, _eod_stock_data_df, _price_data_days
    
    if days is not None:
        _price_data_days = days
    
    conn = get_duckdb_connection()
    logger.debug("[RRG Price Data] Connected to DuckDB")
    
    try:
        load_start = datetime.now()
        logger.info(f"[RRG Price Data] Loading price data for last {_price_data_days} days...")
        
        # First get the list of index symbols from market_metadata
        index_symbols = conn.sql(
            """SELECT DISTINCT symbol 
               FROM public.market_metadata 
               WHERE security_type_code IN (5, 26)"""
        ).pl()["symbol"].to_list()
        
        if not index_symbols:
            raise ValueError("No index symbols found in market_metadata")
        
        logger.info(f"[RRG Price Data] Found {len(index_symbols)} index symbols")
        
        # Load index data first
        with DuckDBQueryTimer("index_prices_query"):
            _stock_prices_df = conn.sql(
                f"""SELECT 
                    created_at,
                    symbol,
                    current_price as close_price,
                    security_code,
                    previous_close,
                    ticker
                FROM public.stock_prices
                WHERE created_at >= CURRENT_DATE - INTERVAL '{_price_data_days} days'
                AND symbol IN ({','.join([f"'{s}'" for s in index_symbols])})
                AND current_price > 0  -- Ensure positive prices
                ORDER BY created_at ASC"""
            ).pl()
        logger.info(f"[RRG Price Data] Loaded {len(_stock_prices_df)} index price entries")
        
        # Load EOD data for indices
        with DuckDBQueryTimer("eod_index_data_query"):
            _eod_stock_data_df = conn.sql(
                f"""SELECT 
                    created_at,
                    symbol,
                    close_price,
                    security_code,
                    previous_close,
                    ticker
                FROM public.eod_stock_data 
                WHERE created_at >= CURRENT_DATE - INTERVAL '{_price_data_days} days'
                AND symbol IN ({','.join([f"'{s}'" for s in index_symbols])})
                AND close_price > 0  -- Ensure positive prices
                ORDER BY created_at ASC"""
            ).pl()
        logger.info(f"[RRG Price Data] Loaded {len(_eod_stock_data_df)} EOD index data entries")
        
        # Join with market_metadata to get additional fields
        _stock_prices_df = _stock_prices_df.join(
            conn.sql("SELECT symbol, name, slug, security_type_code FROM public.market_metadata").pl(),
            on="symbol",
            how="left"
        )
        
        _eod_stock_data_df = _eod_stock_data_df.join(
            conn.sql("SELECT symbol, name, slug, security_type_code FROM public.market_metadata").pl(),
            on="symbol",
            how="left"
        )
        
        # Handle timezone conversions
        _stock_prices_df = _stock_prices_df.with_columns(
            pl.col("created_at").dt.replace_time_zone("UTC").dt.convert_time_zone("Asia/Kolkata").alias("created_at")
        )
        _eod_stock_data_df = _eod_stock_data_df.with_columns(
            pl.col("created_at").dt.replace_time_zone("UTC").dt.convert_time_zone("Asia/Kolkata").alias("created_at")
        )
        
        # Remove timezone info from stock prices
        _stock_prices_df = _stock_prices_df.with_columns(
            pl.col("created_at").dt.replace_time_zone(None).alias("created_at")
        )
        
        # Process EOD data dates
        _eod_stock_data_df = _eod_stock_data_df.with_columns(
            pl.col("created_at").dt.replace_time_zone("UTC").dt.convert_time_zone(INDIAN_TZ)
        )
        _eod_stock_data_df = _eod_stock_data_df.with_columns(
            pl.col("created_at").dt.date().cast(pl.Datetime).alias("created_at")
        )
        
        # Add one day to EOD dates
        _eod_stock_data_df = _eod_stock_data_df.with_columns(
            (pl.col("created_at") + pl.duration(days=1)).alias("created_at")
        )
        
        # Format slugs properly
        for df in [_stock_prices_df, _eod_stock_data_df]:
            if "slug" in df.columns:
                df = df.with_columns([
                    pl.col("slug").str.replace(" ", "").str.replace("-", "").str.replace("&", "").str.to_lowercase().alias("slug")
                ])
        
        # Validate data
        logger.info(f"[RRG Price Data] Validating data...")
        
        # Check for required columns
        required_columns = ["created_at", "symbol", "close_price", "security_code", "ticker", "name", "slug", "security_type_code"]
        for df_name, df in [("stock_prices", _stock_prices_df), ("eod_stock_data", _eod_stock_data_df)]:
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns in {df_name}: {missing_columns}")
        
        # Check for valid security type codes
        for df_name, df in [("stock_prices", _stock_prices_df), ("eod_stock_data", _eod_stock_data_df)]:
            invalid_types = df.filter(~pl.col("security_type_code").is_in([5, 26]))["symbol"].unique()
            if len(invalid_types) > 0:
                raise ValueError(f"Invalid security type codes in {df_name} for symbols: {invalid_types}")
        
        # Check for valid prices
        for df_name, df in [("stock_prices", _stock_prices_df), ("eod_stock_data", _eod_stock_data_df)]:
            invalid_prices = df.filter(pl.col("close_price") <= 0)
            if len(invalid_prices) > 0:
                raise ValueError(f"Found non-positive prices in {df_name}")
        
        logger.info(f"[RRG Price Data] Data validation completed")
        logger.info(f"[RRG Price Data] EOD stock data max date: {_eod_stock_data_df['created_at'].max()}")
        
        load_duration = (datetime.now() - load_start).total_seconds()
        logger.info(f"[RRG Price Data] Price data loading completed in {load_duration:.2f} seconds")
        
        return True
    except Exception as e:
        logger.error(f"[RRG Price Data] Error loading price data: {str(e)}", exc_info=True)
        return False

def ensure_metadata_loaded():
    """
    Ensures that metadata is loaded if it hasn't been already.
    """
    global _metadata_loaded, _last_refresh_time
    
    with TimerMetric("ensure_metadata_loaded", "rrg_metadata"):
        if _metadata_loaded:
            logger.debug("[RRG Metadata] Metadata already loaded, using existing data")
            return True
        
        logger.info("[RRG Metadata] Loading metadata tables...")
        success = _load_metadata()
        
        if success:
            _metadata_loaded = True
            _last_refresh_time = datetime.now(timezone.utc)
            logger.info(f"[RRG Metadata] Initial metadata load completed at {_last_refresh_time}")
            return True
        else:
            logger.error("[RRG Metadata] Failed to load metadata")
            return False

def ensure_price_data_loaded(days=None):
    """
    Ensures that price data is loaded if it hasn't been already.
    
    Args:
        days: Optional number of days of historical data to load
    """
    global _price_data_loaded, _last_price_refresh_time
    
    with TimerMetric("ensure_price_data_loaded", "rrg_metadata"):
        if _price_data_loaded:
            logger.debug("[RRG Price Data] Price data already loaded, using existing data")
            return True
        
        logger.info("[RRG Price Data] Loading price data tables...")
        success = _load_price_data(days)
        
        if success:
            _price_data_loaded = True
            _last_price_refresh_time = datetime.now(timezone.utc)
            logger.info(f"[RRG Price Data] Initial price data load completed at {_last_price_refresh_time}")
            return True
        else:
            logger.error("[RRG Price Data] Failed to load price data")
            return False

def refresh_metadata():
    """
    Refreshes all metadata by reloading from DuckDB.
    This function will be called by a Celery task in the future.
    """
    global _metadata_loaded, _last_refresh_time
    
    with TimerMetric("refresh_metadata", "rrg_metadata"):
        logger.info("[RRG Metadata] Refreshing metadata tables...")
        
        _metadata_loaded = False
        
        success = _load_metadata()
        
        if success:
            _metadata_loaded = True
            _last_refresh_time = datetime.now(timezone.utc)
            logger.info(f"[RRG Metadata] Metadata refreshed at {_last_refresh_time}")
            return True
        else:
            logger.error("[RRG Metadata] Failed to refresh metadata")
            return False

def refresh_price_data(days=None):
    """
    Refreshes all price data by reloading from DuckDB.
    This function will be called by a Celery task every 10 minutes.
    
    Args:
        days: Optional number of days of historical data to load
    """
    global _price_data_loaded, _last_price_refresh_time
    
    with TimerMetric("refresh_price_data", "rrg_metadata"):
        logger.info("[RRG Price Data] Refreshing price data tables...")
        
        _price_data_loaded = False
        
        # Reload price data
        success = _load_price_data(days)
        
        if success:
            _price_data_loaded = True
            _last_price_refresh_time = datetime.now(timezone.utc)
            logger.info(f"[RRG Price Data] Price data refreshed at {_last_price_refresh_time}")
            return True
        else:
            logger.error("[RRG Price Data] Failed to refresh price data")
            return False

def get_metadata_status():
    """
    Returns the current status of the metadata store.
    """
    return {
        "loaded": _metadata_loaded,
        "last_refresh": _last_refresh_time if _last_refresh_time else None,
        "indices_count": len(_indices_df) if _indices_df is not None else 0,
        "indices_stocks_count": len(_indices_stocks_df) if _indices_stocks_df is not None else 0,
        "companies_count": len(_companies_df) if _companies_df is not None else 0,
        "stocks_count": len(_stocks_df) if _stocks_df is not None else 0,
        "market_metadata_count": len(_market_metadata_df) if _market_metadata_df is not None else 0
    }

def get_price_data_status():
    """
    Returns the current status of the price data store.
    """
    return {
        "loaded": _price_data_loaded,
        "last_refresh": _last_price_refresh_time if _last_price_refresh_time else None,
        "stock_prices_count": len(_stock_prices_df) if _stock_prices_df is not None else 0,
        "eod_stock_data_count": len(_eod_stock_data_df) if _eod_stock_data_df is not None else 0,
        "days_loaded": _price_data_days
    }


def get_indices():
    """
    Returns a clone of the indices DataFrame.
    """
    ensure_metadata_loaded()
    return _indices_df.clone() if _indices_df is not None else None

def get_indices_stocks():
    """
    Returns a clone of the indices_stocks DataFrame.
    """
    ensure_metadata_loaded()
    return _indices_stocks_df.clone() if _indices_stocks_df is not None else None

def get_companies():
    """
    Returns a clone of the companies DataFrame.
    """
    ensure_metadata_loaded()
    return _companies_df.clone() if _companies_df is not None else None

def get_stocks():
    """
    Returns a clone of the stocks DataFrame.
    """
    ensure_metadata_loaded()
    return _stocks_df.clone() if _stocks_df is not None else None

def get_market_metadata(symbols=None):
    """
    Returns a clone of the market_metadata DataFrame, optionally filtered by symbols.
    
    Args:
        symbols: Optional list of symbols to filter by
        
    Returns:
        A cloned and optionally filtered DataFrame
    """
    ensure_metadata_loaded()
    
    if _market_metadata_df is None:
        return None
        
    if symbols is not None:
        return _market_metadata_df.filter(pl.col("symbol").is_in(symbols)).clone()
    else:
        return _market_metadata_df.clone()

def get_stock_prices(tickers, symbols, filter_days=30):
    """Get stock prices for the given tickers and symbols."""
    try:
        logger.debug(f"[RRG Price Data] Starting get_stock_prices with:")
        logger.debug(f"[RRG Price Data] - tickers: {tickers}")
        logger.debug(f"[RRG Price Data] - symbols: {symbols}")
        logger.debug(f"[RRG Price Data] - filter_days: {filter_days}")

        # Check if price data is loaded
        if not _price_data_loaded:
            logger.error("[RRG Price Data] Price data not loaded")
            return None

        # Convert timeframe to days if it's a string
        if isinstance(filter_days, str):
            filter_days = return_filter_days(filter_days)

        # Calculate cutoff date in UTC
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=filter_days)
        
        # Get data from the appropriate source
        if isinstance(tickers, list) and len(tickers) > 0:
            # If symbols is a string (like "5 days"), just use tickers
            if isinstance(symbols, str):
                df = _stock_prices_df.filter(
                    pl.col("symbol").is_in(tickers)
                )
            else:
                # If symbols is a list, combine with tickers
                df = _stock_prices_df.filter(
                    pl.col("symbol").is_in(tickers + symbols)
                )
        else:
            # If no tickers, use symbols (which should be a list)
            if isinstance(symbols, str):
                logger.error("[RRG Price Data] Invalid symbols parameter: expected list, got string")
                return None
            df = _stock_prices_df.filter(
                pl.col("symbol").is_in(symbols)
            )

        # Convert created_at to UTC if it's not already
        df = df.with_columns(
            pl.col("created_at").dt.replace_time_zone("UTC")
        )

        # Filter by date
        df = df.filter(pl.col("created_at") > cutoff_date)

        # Add required columns if they don't exist
        if "security_code" not in df.columns:
            df = df.with_columns(pl.col("symbol").alias("security_code"))
        if "ticker" not in df.columns:
            df = df.with_columns(pl.col("symbol").alias("ticker"))
        if "name" not in df.columns:
            df = df.with_columns(pl.col("symbol").alias("name"))
        if "slug" not in df.columns:
            df = df.with_columns(pl.col("symbol").str.to_lowercase().str.replace(" ", "-").alias("slug"))

        # Log data summary
        logger.debug(f"[RRG Price Data] Final data summary:")
        logger.debug(f"[RRG Price Data] - Total rows: {len(df)}")
        logger.debug(f"[RRG Price Data] - Unique symbols: {len(df['symbol'].unique())}")
        logger.debug(f"[RRG Price Data] - Date range: {df['created_at'].min()} to {df['created_at'].max()}")

        # Log symbol statistics
        symbol_stats = df.groupby("symbol").agg([
            pl.count().alias("data_points"),
            pl.col("close_price").null_count().alias("null_prices"),
            pl.col("created_at").min().alias("first_date"),
            pl.col("created_at").max().alias("last_date")
        ]).sort("symbol")
        logger.debug(f"[RRG Price Data] Symbol statistics:\n{symbol_stats}")

        return df

    except Exception as e:
        logger.error(f"[RRG Price Data] Error getting stock prices: {str(e)}", exc_info=True)
        return None

def get_eod_stock_data(tickers=None, symbols=None, filter_days=None):
    """
    Returns EOD stock data from the in-memory store, optionally filtered.
    
    Args:
        tickers: Optional list of tickers to filter by
        symbols: Optional list of symbols to filter by
        filter_days: Optional number of days to filter to
        
    Returns:
        A cloned and optionally filtered DataFrame
    """
    logger.debug(f"[RRG Price Data] Starting get_eod_stock_data with:")
    logger.debug(f"[RRG Price Data] - tickers: {tickers}")
    logger.debug(f"[RRG Price Data] - symbols: {symbols}")
    logger.debug(f"[RRG Price Data] - filter_days: {filter_days}")
    
    ensure_price_data_loaded()
    
    if _eod_stock_data_df is None:
        logger.error("[RRG Price Data] No EOD stock data available")
        return None
        
    df = _eod_stock_data_df.clone()
    logger.debug(f"[RRG Price Data] Initial data size: {len(df)} rows")
    
    # First apply date filtering if needed
    if filter_days is not None:
        cutoff_date = datetime.now() - timedelta(days=filter_days)
        cutoff_date = cutoff_date.replace(tzinfo=None)
        
        # Convert created_at to naive datetime for comparison
        df = df.with_columns(
            pl.col("created_at").dt.replace_time_zone(None).alias("created_at_naive")
        )
        
        df = df.filter(pl.col("created_at_naive") > cutoff_date)
        df = df.drop("created_at_naive")
        
        logger.debug(f"[RRG Price Data] After filter_days filtering: {len(df)} rows remaining")
    
    # Then apply symbol/ticker filtering if needed
    if tickers is not None or symbols is not None:
        filter_conditions = []
        if tickers is not None:
            filter_conditions.append(pl.col("ticker").is_in(tickers))
        if symbols is not None:
            filter_conditions.append(pl.col("symbol").is_in(symbols))
        
        # Apply the combined filter - use any_horizontal to get data matching either condition
        df = df.filter(pl.any_horizontal(filter_conditions))
        logger.debug(f"[RRG Price Data] After combined filtering: {len(df)} rows remaining")
        
        # Only check for minimum data points if we have data
        if len(df) > 0:
            # Count data points per symbol using groupby
            symbol_counts = df.groupby("symbol").agg(pl.count().alias("count"))
            
            # Calculate expected points based on filter_days
            expected_points = filter_days if filter_days is not None else 365
            min_required_points = int(expected_points * 0.5)  # Allow 50% missing data
            
            # Get symbols with very few data points
            insufficient_symbols = symbol_counts.filter(pl.col("count") < min_required_points)["symbol"].to_list()
            
            if insufficient_symbols:
                logger.warning(f"[RRG Price Data] Symbols with insufficient data points (< {min_required_points}): {insufficient_symbols}")
                # Remove symbols with very few data points
                df = df.filter(~pl.col("symbol").is_in(insufficient_symbols))
                logger.debug(f"[RRG Price Data] After removing insufficient data: {len(df)} rows remaining")
    
    # Log final data summary
    if len(df) > 0:
        logger.debug(f"[RRG Price Data] Final data summary:")
        logger.debug(f"[RRG Price Data] - Total rows: {len(df)}")
        logger.debug(f"[RRG Price Data] - Unique symbols: {df['symbol'].n_unique()}")
        logger.debug(f"[RRG Price Data] - Date range: {df['created_at'].min()} to {df['created_at'].max()}")
        
        # Add data quality metrics
        symbol_stats = df.groupby("symbol").agg([
            pl.count().alias("data_points"),
            pl.col("close_price").null_count().alias("null_prices"),
            pl.col("created_at").min().alias("first_date"),
            pl.col("created_at").max().alias("last_date")
        ])
        logger.debug(f"[RRG Price Data] Symbol statistics:\n{symbol_stats}")
    else:
        logger.warning("[RRG Price Data] No data remaining after filtering")
    
    return df
