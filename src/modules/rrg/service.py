import json
import os
import time
import polars as pl
from typing import Dict, Any
from .schemas import RrgRequest, RrgResponse
from .generate import generate_csv
from src.utils.manager.cache_manager import CacheManager
from src.utils.metrics import (
    TimerMetric, DuckDBQueryTimer, RRGRequestMetrics,
    record_rrg_cache_hit, record_rrg_cache_miss,
    record_rrg_data_points, record_rrg_error
)
from src.utils.logger import get_logger
from src.modules.db.data_manager import load_data
from datetime import datetime, timezone, timedelta
import numpy as np
import logging
from src.utils.duck_pool import get_duckdb_connection
from src.utils.clickhouse_pool import pool as ClickHousePool
from src.modules.db.config import MAX_RECORDS
import pandas as pd
from .helper.csv_generator import CSVCenerator
from .helper.rrg_executor import RRGExecutor

# Configure logger to show INFO level logs
logger = get_logger("rrg_service")
logger.setLevel(logging.INFO)

# Add console handler if not already present
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

def clean_for_json(obj):
    """Clean objects to ensure they're JSON serializable"""
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items() if not isinstance(v, type)}
    elif isinstance(obj, list):
        return [clean_for_json(item) for item in obj if not isinstance(item, type)]
    elif hasattr(obj, 'to_dict') and callable(obj.to_dict):
        return clean_for_json(obj.to_dict())
    elif isinstance(obj, pl.DataType):
        logger.debug(f"Converting polars data type to string: {obj}")
        return str(obj)
    else:
        return obj

class RRGService:
    def __init__(self):
        self.cache_manager = CacheManager("rrg")
        self.db_path = 'data/pydb.duckdb'
        self.csv_generator = None
        self.rrg_executor = RRGExecutor()
        logger.debug("RRGService initialized")

    async def load_hourly_data(self):
        """Load hourly stock data without chunk size limitations."""
        pass

    async def load_data(self, start_date=None, end_date=None):
        """Load EOD data from ClickHouse to DuckDB, only if DuckDB is empty."""
        try:
            # Set default dates if not provided
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

            # Check if DuckDB table exists and has data
            with get_duckdb_connection(self.db_path) as conn:
                table_exists = False
                try:
                    res = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='eod_stock_data'").fetchone()
                    table_exists = res[0] > 0
                except Exception as e:
                    logger.info("Table check failed, assuming table does not exist.")
                    table_exists = False
                if table_exists:
                    try:
                        count = conn.execute("SELECT COUNT(*) FROM public.eod_stock_data").fetchone()[0]
                        if count > 0:
                            logger.info(f"DuckDB already has {count} records in eod_stock_data. Skipping load.")
                            return True
                    except Exception as e:
                        logger.info("eod_stock_data table exists but count failed, will reload.")

            logger.info(f"Loading data from {start_date} to {end_date}")

            # Count total records to load
            count_query = f"""
                SELECT COUNT(*) as total
                FROM strike.equity_prices_1d e
                JOIN strike.mv_stocks s ON e.security_code = s.security_code
                WHERE e.date_time BETWEEN '{start_date}' AND '{end_date}'
                AND e.close > 0
            """
            result = ClickHousePool.execute_query(count_query)
            total_records = result[0][0]
            logger.info(f"Found {total_records:,} records to load")

            # Load data in chunks
            chunk_size = 100000
            offset = 0
            total_loaded = 0

            while offset < total_records:
                # Query to get data with all required columns
                query = f"""
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
                        WHERE e.date_time BETWEEN '{start_date}' AND '{end_date}'
                        AND e.close > 0
                        ORDER BY e.date_time DESC
                        LIMIT {chunk_size} OFFSET {offset}
                    )
                    SELECT * FROM ordered_data
                """

                logger.info(f"Executing query for chunk {offset//chunk_size + 1}...")
                result = ClickHousePool.execute_query(query)

                if not result:
                    logger.warning(f"No data returned for chunk {offset//chunk_size + 1}")
                    break

                chunk_records = len(result)
                total_loaded += chunk_records

                # Calculate and log progress metrics
                progress = (total_loaded / total_records) * 100
                logger.info(f"Chunk {offset//chunk_size + 1} loaded: {chunk_records} records")
                logger.info(f"Total loaded: {total_loaded}/{total_records} records ({progress:.1f}%)")

                # Insert into DuckDB
                with get_duckdb_connection(self.db_path) as conn:
                    logger.info(f"Inserting chunk {offset//chunk_size + 1} into DuckDB...")
                    conn.executemany("""
                        INSERT OR IGNORE INTO public.eod_stock_data 
                        (created_at, ratio, momentum, close_price, change_percentage, metric_1, metric_2, metric_3, signal, security_code, previous_close, ticker)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, result)
                    logger.info(f"Chunk {offset//chunk_size + 1} inserted successfully")

                offset += chunk_size

            logger.info("EOD data load completed:")
            logger.info(f"Total records loaded: {total_loaded:,}")

            return True

        except Exception as e:
            logger.error(f"Error loading EOD data: {str(e)}", exc_info=True)
            return False

    async def get_rrg_data(self, request: RrgRequest) -> RrgResponse:
        """Get RRG data for the specified request"""
        try:
            # Generate cache key based on request parameters
            cache_key = f"rrg_data_{request.index_symbol}_{request.timeframe}_{request.date_range}_{hash(tuple(sorted(request.tickers)))}"
            
            # Try to get from cache first
            cached_data = self.cache_manager.getCache(cache_key)
            if cached_data:
                logger.info(f"Cache hit for RRG data: {cache_key}")
                record_rrg_cache_hit("rrg_data")
                return RrgResponse(
                    status="success",
                    data=json.loads(cached_data),
                    filename=f"cached_{request.index_symbol}_{request.timeframe}",
                    cacheHit=True
                )
            
            record_rrg_cache_miss("rrg_data")
            logger.info(f"Cache miss for RRG data: {cache_key}")
            
            with get_duckdb_connection() as conn:
                # Get index stocks
                index_query = f"""
                    SELECT DISTINCT
                        m.security_code,
                        m.ticker,
                        m.symbol,
                        m.company_name as name,
                        m.company_name as meaningful_name,
                        LOWER(REPLACE(m.company_name, ' ', '-')) as slug,
                        m.security_type_code
                    FROM public.market_metadata m
                    WHERE m.symbol = '{request.index_symbol}'
                """
                index_result = conn.execute(index_query).fetchdf()
                logger.info(f"Index query result: {index_result.to_dict('records')}")
                
                if index_result.empty:
                    logger.warning(f"Index not found: {request.index_symbol}")
                    return RrgResponse(
                        status="error",
                        data={},
                        filename="",
                        error=f"Index not found: {request.index_symbol}"
                    )
                
                # Get stock codes for the index
                stock_codes = index_result['security_code'].tolist()
                logger.info(f"Stock codes for index {request.index_symbol}: {stock_codes}")
                
                # Determine data source based on timeframe
                if request.timeframe in ["15m", "30m", "1h"]:
                    # Use stock_prices for minute-based timeframes
                    query = f"""
                        SELECT 
                            p.created_at,
                            p.close_price,
                            p.security_code,
                            m.ticker,
                            m.symbol,
                            m.company_name as name,
                            m.nse_index_name as meaningful_name,
                            m.slug,
                            m.security_type_code
                        FROM public.stock_prices p
                        JOIN public.market_metadata m ON p.security_code = m.security_code
                        WHERE p.security_code IN ({','.join([f"'{code}'" for code in stock_codes])})
                        AND p.created_at >= CURRENT_DATE - INTERVAL '{request.date_range * 3}' DAY
                        ORDER BY p.created_at DESC
                    """
                else:
                    # Use eod_stock_data for daily/weekly/monthly
                    query = f"""
                        SELECT 
                            e.created_at,
                            e.close_price,
                            e.security_code,
                            m.ticker,
                            m.symbol,
                            m.company_name as name,
                            m.nse_index_name as meaningful_name,
                            m.slug,
                            m.security_type_code
                        FROM public.eod_stock_data e
                        JOIN public.market_metadata m ON e.security_code = m.security_code
                        WHERE e.security_code IN ({','.join([f"'{code}'" for code in stock_codes])})
                        AND e.created_at >= CURRENT_DATE - INTERVAL '{request.date_range}' DAY
                        ORDER BY e.created_at DESC
                    """
                
                result = conn.execute(query).fetchdf()
                logger.info(f"Price data query returned {len(result)} rows")
                
                if result.empty:
                    logger.warning(f"No price data found for stocks in index: {request.index_symbol}")
                    return RrgResponse(
                        status="error",
                        data={},
                        filename="",
                        error=f"No price data found for stocks in index: {request.index_symbol}"
                    )
                
                # Transform data into initial RRG format
                rrg_data = {
                    "benchmark": request.index_symbol,
                    "indexdata": [],
                    "datalists": [],
                    "change_data": []
                }
                
                # Process index data first
                index_data = result[result['ticker'] == request.index_symbol]
                if not index_data.empty:
                    index_points = []
                    for _, row in index_data.iterrows():
                        index_points.append(str(row['close_price']))
                    rrg_data["indexdata"] = index_points
                    logger.info(f"Index data points: {len(index_points)} points")
                
                # Process stock data
                for ticker in request.tickers:
                    ticker_data = result[result['ticker'] == ticker]
                    if not ticker_data.empty:
                        ticker_points = []
                        for _, row in ticker_data.iterrows():
                            point = [
                                row['created_at'].strftime('%Y-%m-%d %H:%M:%S'),
                                "0",  # ratio will be calculated by RRG binary
                                "0",  # momentum will be calculated by RRG binary
                                str(row['close_price'])
                            ]
                            ticker_points.append(point)
                        
                        # Get metadata for this ticker
                        metadata = ticker_data.iloc[0]
                        datalist = {
                            "code": metadata['symbol'],
                            "name": metadata['name'],
                            "meaningful_name": metadata['meaningful_name'],
                            "slug": metadata['slug'],
                            "ticker": ticker,
                            "symbol": metadata['symbol'],
                            "security_code": metadata['security_code'],
                            "security_type_code": float(metadata['security_type_code']),
                            "data": ticker_points
                        }
                        rrg_data["datalists"].append(datalist)
                        logger.info(f"Added datalist for {ticker} with {len(ticker_points)} points")
                
                # Generate change_data
                start_date = result['created_at'].min()
                for ticker in request.tickers:
                    ticker_data = result[result['ticker'] == ticker]
                    if not ticker_data.empty:
                        first_day_data = ticker_data[ticker_data['created_at'] == start_date].iloc[0]
                        change = {
                            "symbol": ticker,
                            "created_at": start_date.strftime('%Y-%m-%d'),
                            "change_percentage": 0,  # Will be calculated by RRG binary
                            "close_price": float(first_day_data['close_price'])
                        }
                        rrg_data["change_data"].append(change)
                        logger.info(f"Added change data for {ticker}: {change}")
                
                # Generate CSV and process with RRG binary
                self.csv_generator = CSVCenerator(request.index_symbol)
                input_file, output_file = self.csv_generator.generate_csv(rrg_data, request.timeframe)
                
                # Execute RRG binary
                processed_data = self.rrg_executor.execute(input_file, output_file, request.timeframe)
                
                # Merge momentum data
                final_data = self.rrg_executor._merge_momentum(processed_data, rrg_data["change_data"], request.timeframe)
                
                # Generate filename
                filename = f"{request.index_symbol}_{'_'.join(request.tickers)}_{request.timeframe}_{request.date_range}_{datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
                
                # Log final response structure
                logger.info(f"Final RRG response structure:")
                logger.info(f"- Benchmark: {final_data['benchmark']}")
                logger.info(f"- Number of index points: {len(final_data['indexdata'])}")
                logger.info(f"- Number of datalists: {len(final_data['datalists'])}")
                logger.info(f"- Number of change data entries: {len(rrg_data['change_data'])}")
                
                # Cache the RRG data
                self.cache_manager.setCache(cache_key, json.dumps(final_data), expiry_in_minutes=60)  # Cache for 1 hour
                
                return RrgResponse(
                    status="success",
                    data=final_data,
                    filename=filename,
                    cacheHit=False
                )
                
        except Exception as e:
            logger.error(f"Error getting RRG data: {str(e)}", exc_info=True)
            return RrgResponse(
                status="error",
                data={},
                filename="",
                error=f"Error getting RRG data: {str(e)}"
            )

    def _check_apply_pl(self, data_values, meaningful_name):
        """Process daily data using polars operations."""
        with TimerMetric("_check_apply_pl", "rrg_service"):
            logger.debug(f"Processing daily data for {meaningful_name}")
            
            result = []
            for x in data_values:
                result.append({
                    "date": x[0].replace("00", "").replace(":", "").replace(" ", ""),
                    "momentum": x[2],
                    "ratio": x[1],
                    "code": meaningful_name,
                })
            
            logger.debug(f"Generated {len(result)} records for {meaningful_name}")
            record_rrg_data_points(len(result), "daily_processed")
            return result

    def _check_apply_hourly_pl(self, data_values, meaningful_name):
        """Process hourly data using polars operations."""
        with TimerMetric("_check_apply_hourly_pl", "rrg_service"):
            logger.debug(f"Processing hourly data for {meaningful_name}")
            
            result = []
            for x in data_values:
                result.append({
                    "date": x[0],
                    "momentum": x[2],
                    "ratio": x[1],
                    "code": meaningful_name,
                })
            
            logger.debug(f"Generated {len(result)} records for {meaningful_name}")
            record_rrg_data_points(len(result), "hourly_processed")
            return result

    def _merge_momentum(self, data: Dict[str, Any], change_data_json: Dict[str, Any], timeframe: str) -> str:
        """
        Merges RRG data points with historical change data using Polars vectorized operations.

        Args:
            data: Dictionary containing the RRG data structure, including 'datalists'.
            change_data_json: Dictionary representing the historical change data, loaded from JSON.
            timeframe: The timeframe string for the request (e.g., "15m", "daily").

        Returns:
            A JSON string representing the merged data (list of dictionaries),
            or an empty JSON list '[]' if errors occur or no data is merged.
        """
        with TimerMetric("_merge_momentum", "rrg_service"):
            start_time = time.time()
            logger.debug(f"Starting momentum merge with timeframe {timeframe}")
            try:
                with TimerMetric("dataframe_creation", "rrg_service"):
                    pl_data = pl.DataFrame(data["datalists"])
                    pl_change = pl.DataFrame(change_data_json)
                    logger.debug(f"Created DataFrames: data({pl_data.shape}), change({pl_change.shape})")
                    record_rrg_data_points(pl_data.shape[0], "momentum_data_input")
                    record_rrg_data_points(pl_change.shape[0], "change_data_input")
            except Exception as e:
                record_rrg_error("dataframe_creation")
                logger.error(f"Failed to create Polars DataFrames: {e}", exc_info=True)
                return "[]" # Return empty JSON list on initial loading failure

            if pl_data.is_empty() or pl_change.is_empty():
                 logger.warning("Input data or change data is empty. Returning empty result.")
                 return "[]"

            # Process pl_change
            with TimerMetric("process_dates", "rrg_service"):
                try:
                    # Define the expected format after initial cleaning from cache/source
                    datetime_format_change = "%Y-%m-%d %H:%M:%S" # Adjust if format differs
                    pl_change = pl_change.with_columns(
                        pl.col("created_at").str.replace_all("T", " ")
                        .str.replace_all("\\+05:30", "") # Adapt to actual timezone offset if needed
                        .str.replace_all("\\+00:00", "")
                        # Convert to Datetime using strict=False to return null on error
                        .str.strptime(pl.Datetime, format=datetime_format_change, strict=False)
                        .alias("created_at")
                    )
                    # Filter out rows where conversion resulted in null
                    pl_change = pl_change.filter(pl.col("created_at").is_not_null())
                    logger.debug(f"Processed timestamps in change data: {pl_change.shape} rows")
                    record_rrg_data_points(pl_change.shape[0], "change_data_processed")
                except Exception as e:
                    record_rrg_error("timestamp_processing")
                    logger.error(f"Failed processing change data timestamps: {e}", exc_info=True)
                    return "[]"

            # Process pl_data (vectorized)
            with TimerMetric("explode_data", "rrg_service"):
                try:
                    exploded_df = pl_data.explode("data")
                    logger.debug(f"Exploded data shape: {exploded_df.shape}")
                    record_rrg_data_points(exploded_df.shape[0], "exploded_data")

                    if exploded_df.is_empty():
                        logger.warning("Exploded data is empty, returning empty result")
                        return "[]"

                    # Define the expected format in the 'data' list element 0
                    datetime_format_data = "%Y-%m-%d %H:%M:%S" # Adjust if format differs

                    # Extract list elements safely
                    df_with_extracted = exploded_df.with_columns([
                        pl.col("data").list.get(0).alias("date_str"),
                        # Cast directly during extraction if type is consistent, otherwise cast later
                        pl.col("data").list.get(1).cast(pl.Float64, strict=False).alias("ratio_val"),
                        pl.col("data").list.get(2).cast(pl.Float64, strict=False).alias("momentum_val"),
                        pl.col("symbol").alias("symbol")
                    ])

                    # Select, convert date string to Datetime, cast ratio/momentum
                    processed_pl = df_with_extracted.select([
                         pl.col("date_str")
                        .str.strptime(pl.Datetime, format=datetime_format_data, strict=False)
                        .alias("created_at"),
                        pl.col("symbol"),
                        pl.col("ratio_val").alias("ratio"),
                        pl.col("momentum_val").alias("momentum")
                    ])
                    # Filter out rows where conversion resulted in null
                    processed_pl = processed_pl.filter(pl.col("created_at").is_not_null())
                    logger.debug(f"Processed RRG data shape: {processed_pl.shape}")
                    record_rrg_data_points(processed_pl.shape[0], "rrg_data_processed")

                except Exception as e:
                    record_rrg_error("data_explosion")
                    logger.error(f"Failed processing RRG data: {e}", exc_info=True)
                    return "[]"

            if processed_pl.is_empty():
                logger.warning("Processed RRG data is empty after processing, returning empty result")
                return "[]"

            # Join on Datetime objects
            with TimerMetric("join_data", "rrg_service"):
                try:
                    final_pl = pl_change.join(
                        processed_pl,
                        on=["symbol", "created_at"], # Join keys are now Datetime
                        how="inner"
                    )
                    logger.debug(f"Join resulted in {final_pl.shape[0]} rows")
                    record_rrg_data_points(final_pl.shape[0], "joined_data")
                except Exception as e:
                    record_rrg_error("data_join")
                    logger.error(f"Failed during join operation: {e}", exc_info=True)
                    return "[]"

            if final_pl.is_empty():
                logger.warning("Join resulted in empty DataFrame. No matching timestamps between change data and RRG data.")
                return "[]"

            # Convert Datetime back to String for JSON Serialization
            with TimerMetric("prepare_output", "rrg_service"):
                try:
                    if "created_at" in final_pl.columns and final_pl["created_at"].dtype == pl.Datetime:
                        logger.debug("Converting final timestamps to ISO format for JSON")
                        final_pl = final_pl.with_columns(
                            # Use ISO 8601 format (YYYY-MM-DDTHH:MM:SS) - standard for APIs
                            pl.col("created_at").dt.strftime("%Y-%m-%dT%H:%M:%S").alias("created_at")
                        )
                except Exception as e:
                    record_rrg_error("timestamp_conversion")
                    logger.error(f"Failed converting timestamps back to string: {e}", exc_info=True)
                    return "[]"

                # Prepare JSON Output
                try:
                    list_data = final_pl.to_dicts()
                    # Pass the list (now with string dates) to _ensure_json_format
                    json_string_output = self._ensure_json_format(list_data)
                    record_rrg_data_points(len(list_data), "final_output")
                except Exception as e:
                    record_rrg_error("json_conversion")
                    logger.error(f"Failed converting final DataFrame to JSON: {e}", exc_info=True)
                    return "[]"

            logger.info(f"Momentum merge completed in {time.time() - start_time:.2f}s with {len(list_data)} records")
            return json_string_output

    def _ensure_json_format(self, data_in: Any) -> str:
        """Ensure the data is returned as a JSON formatted string."""
        try:
            if isinstance(data_in, str):
                return data_in
            elif isinstance(data_in, (list, dict)):
                # Convert any datetime objects to ISO format strings
                def datetime_handler(obj):
                    if isinstance(obj, datetime):
                        return obj.isoformat()
                    elif isinstance(obj, (np.datetime64, np.int64, np.float64)):
                        return obj.item()
                    elif isinstance(obj, pl.Datetime):
                        return obj.isoformat()
                    return str(obj)
                return json.dumps(data_in, ensure_ascii=False, default=datetime_handler)
            else:
                logger.warning(f"Unexpected type in _ensure_json_format: {type(data_in)}. Attempting dump.")
                return json.dumps(data_in, ensure_ascii=False, default=str)
        except Exception as e:
            record_rrg_error("json_format")
            logger.error(f"Error ensuring JSON format: {str(e)}", exc_info=True)
            return "[]"

def load_eod_data(days=None):
    """
    Load EOD data from DuckDB without chunk size limitations.
    Args:
        days: Number of days of historical data to load
    """
    with TimerMetric("load_eod_data", "rrg_service"):
        try:
            start_time = time.time()
            logger.info(f"Starting EOD data load at {datetime.now().isoformat()}")
            if days is None:
                days = 3650  # Default to 10 years
            logger.info(f"Loading data for the past {days} days")
            
            with get_duckdb_connection() as conn:
                # Load all data at once
                query = f"""
                SELECT 
                    e.date_time as created_at,
                    s.symbol,
                    e.close as close_price,
                    s.security_code,
                    lagInFrame(e.close) OVER (
                        PARTITION BY s.security_code 
                        ORDER BY e.date_time
                    ) as previous_close,
                    s.ticker
                FROM strike.equity_prices_1d e
                JOIN strike.mv_stocks s ON e.security_code = s.security_code
                WHERE e.date_time >= now() - interval '{days}' day
                AND e.close > 0
                ORDER BY e.date_time DESC
                """

                try:
                    logger.info("Executing ClickHouse query for EOD data")
                    result = pool.execute_query(query)
                
                    if not result:
                        logger.warning("No EOD data found")
                        return None
                        
                    df = pl.DataFrame(result)
                    records_loaded = len(df)
                    
                    if records_loaded > 0:
                        # Convert to Arrow and load into DuckDB
                        df_arrow = df.to_arrow()
                        conn.execute("INSERT INTO public.eod_stock_data SELECT * FROM df_arrow")
                        logger.info(f"Loaded {records_loaded:,} EOD records into DuckDB")
                    
                    total_time = time.time() - start_time
                    logger.info(f"Successfully loaded total of {records_loaded:,} records in {total_time:.2f} seconds")
                    return df
                        
                except Exception as e:
                    logger.error(f"Error loading EOD data: {str(e)}")
                    raise

        except Exception as e:
            logger.error(f"Error in load_eod_data: {str(e)}")
            raise 
