import json
import os
import time
import polars as pl
from typing import Dict, Any
from .schemas import RrgRequest, RrgResponse
from .generate import generate_csv, get_market_metadata
from src.utils.manager.cache_manager import CacheManager
from src.utils.metrics import (
    TimerMetric, DuckDBQueryTimer, RRGRequestMetrics,
    record_rrg_cache_hit, record_rrg_cache_miss,
    record_rrg_data_points, record_rrg_error
)
from src.utils.logger import get_logger
from src.modules.db.data_manager import load_data, load_hourly_data
from datetime import datetime, timezone
import numpy as np
import logging
from src.utils.duck_pool import get_duckdb_connection

# Configure logger to only show errors
logger = get_logger("rrg_service")
logger.setLevel(logging.ERROR)

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
        logger.debug("RRGService initialized")

    async def load_data(self):
        with TimerMetric("load_data", "rrg_service"):
            start_time = time.time()
            logger.info("Loading EOD data...")
            try:
                with DuckDBQueryTimer("load_eod_data"):
                    load_data()
                logger.info(f"EOD data loaded successfully in {time.time() - start_time:.2f}s")
                return True
            except Exception as e:
                record_rrg_error("eod_data_load")
                logger.error(f"Failed to load EOD data: {str(e)}", exc_info=True)
                raise

    async def load_hourly_data(self):
        with TimerMetric("load_hourly_data", "rrg_service"):
            start_time = time.time()
            logger.info("Loading hourly data...")
            try:
                with DuckDBQueryTimer("load_hourly_data"):
                    load_hourly_data()
                logger.info(f"Hourly data loaded successfully in {time.time() - start_time:.2f}s")
                return True
            except Exception as e:
                record_rrg_error("hourly_data_load")
                logger.error(f"Failed to load hourly data: {str(e)}", exc_info=True)
                raise
    
    async def get_rrg_data(self, request: RrgRequest) -> RrgResponse:
        """
        Get RRG data for the specified request.
        """
        with TimerMetric("get_rrg_data", "rrg_service"):
            try:
                # Validate request
                if not request.index_symbol or not request.timeframe:
                    raise ValueError("Index symbol and timeframe are required")
                
                # Get tickers from request
                tickers = request.tickers or []
                if request.index_symbol not in tickers:
                    tickers.append(request.index_symbol)
                
                # Generate a filename using timestamp and hash
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                tickers_hash = hash("_".join(tickers)) % 10000  # Use last 4 digits of hash
                filename = f"rrg_{timestamp}_{tickers_hash}"
                
                # Convert date_range to integer
                try:
                    date_range = int(request.date_range)
                except ValueError:
                    raise ValueError(f"Invalid date_range value: {request.date_range}")
                
                # Generate CSV data
                resp = generate_csv(
                    tickers=tickers,
                    date_range=date_range,
                    index_symbol=request.index_symbol,
                    timeframe=request.timeframe,
                    channel_name=request.channel_name,
                    filename=filename,
                    cache_manager=self.cache_manager
                )
                
                if not resp:
                    raise Exception("Failed to generate data")
                
                return RrgResponse(
                    status="success",
                    data=resp["data"],
                    filename=filename,
                    cacheHit=False
                )
                
            except Exception as e:
                logger.error(f"Failed to generate CSV data: {str(e)}", exc_info=True)
                raise Exception("Failed to generate data")

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
    Load EOD data from DuckDB.
    
    Args:
        days: Number of days of historical data to load
    """
    with TimerMetric("load_eod_data", "rrg_service"):
        conn = get_duckdb_connection()
        
        try:
            if days is None:
                days = 3650  # Default to 10 years
            
            with DuckDBQueryTimer("eod_data_query"):
                df = conn.sql(
                    f"""SELECT 
                        created_at,
                        symbol,
                        close_price,
                        security_code,
                        previous_close,
                        ticker
                    FROM public.eod_stock_data 
                    WHERE created_at >= CURRENT_DATE - INTERVAL '{days} days'
                    AND close_price > 0
                    ORDER BY created_at ASC"""
                ).pl()
            
            return df
        except Exception as e:
            logger.error(f"[RRG Service] Error loading EOD data: {str(e)}", exc_info=True)
            return None

def load_hourly_data(days=None):
    """
    Load hourly data from DuckDB.
    
    Args:
        days: Number of days of historical data to load
    """
    with TimerMetric("load_hourly_data", "rrg_service"):
        conn = get_duckdb_connection()
        
        try:
            if days is None:
                days = 3650  # Default to 10 years
            
            with DuckDBQueryTimer("hourly_data_query"):
                df = conn.sql(
                    f"""SELECT 
                        created_at,
                        symbol,
                        current_price as close_price,
                        security_code,
                        previous_close,
                        ticker
                    FROM public.stock_prices
                    WHERE created_at >= CURRENT_DATE - INTERVAL '{days} days'
                    AND current_price > 0
                    ORDER BY created_at ASC"""
                ).pl()
            
            return df
        except Exception as e:
            logger.error(f"[RRG Service] Error loading hourly data: {str(e)}", exc_info=True)
            return None
