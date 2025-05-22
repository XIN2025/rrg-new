import polars as pl
import duckdb
import time as t
from datetime import datetime, timedelta, timezone
import os
from src.utils.csv_generation import generate_csv as csv_generator
from src.modules.rrg.exports.__main__ import main as rrg_bin
import json
import asyncio
import shutil
import time
import asyncio
from src.utils.metrics import (
    TimerMetric, DuckDBQueryTimer, 
    record_rrg_data_points, record_rrg_error,
    rrg_request_duration, rrg_data_points
)
from src.utils.logger import get_logger
from src.utils.duck_pool import get_duckdb_connection
from src.modules.rrg.metadata_store import RRGMetadataStore
from src.modules.rrg.time_utils import return_filter_days, split_time
import logging
from .schemas import RrgRequest, RrgResponse

# Configure logger to only show errors
logger = get_logger("rrg_generate")
logger.setLevel(logging.ERROR)


def get_market_metadata(symbols) -> pl.DataFrame:
    with TimerMetric("get_market_metadata", "rrg_generate"):
        try:
            start_time = t.time()
            logger.debug(f"Getting market metadata for {len(symbols) if isinstance(symbols, list) else 1} symbols")
            metadata_store = RRGMetadataStore()
            metadata_df = metadata_store.get_market_metadata(symbols=symbols)
            
            record_rrg_data_points(len(metadata_df), "market_metadata")
            logger.info(f"Market metadata retrieval completed in {t.time() - start_time:.2f}s ({len(metadata_df)} rows)")
            return metadata_df
        except Exception as e:
            record_rrg_error("market_metadata")
            logger.error(f"Failed to get market metadata: {str(e)}", exc_info=True)
            raise


def generate_csv(tickers, date_range, index_symbol, timeframe, channel_name, filename, cache_manager):
    """
    Generate CSV data for RRG analysis.
    """
    try:
        # Add comprehensive error handling
        if not tickers:
            raise ValueError("No tickers provided")
        
        if not index_symbol:
            raise ValueError("No index symbol provided")
        
        # Validate timeframe format
        valid_timeframes = ["daily", "weekly", "monthly", "60m", "30m", "15m", "5m", "1m"]
        if timeframe not in valid_timeframes:
            raise ValueError(f"Invalid timeframe: {timeframe}")
        
        # Log request details
        logger.info(f"Generating CSV for {len(tickers)} tickers, timeframe: {timeframe}, date_range: {date_range}")
        
        # Get market metadata first
        metadata_df = get_market_metadata(tickers)
        
        # Get DuckDB connection
        conn = get_duckdb_connection()
        
        # Calculate date range in days
        if isinstance(date_range, str):
            if "month" in date_range.lower():
                months = int(date_range.split()[0])
                days = months * 30  # Approximate days per month
            elif "day" in date_range.lower():
                days = int(date_range.split()[0])
            else:
                days = 90  # Default to 3 months
        else:
            days = int(date_range)
        
        # Process data for each ticker
        processed_data = []
        for ticker in tickers:
            try:
                # Get data for ticker
                ticker_data = get_ticker_data(ticker, conn, days, timeframe)
                if ticker_data is not None and not ticker_data.is_empty():
                    processed_data.append(ticker_data)
            except Exception as e:
                logger.error(f"Error processing ticker {ticker}: {str(e)}", exc_info=True)
                continue
        
        if not processed_data:
            raise ValueError("No valid data found for any ticker")
        
        # Combine all data
        combined_data = pl.concat(processed_data)
        
        # Format the data with timeframe and date_range
        formatted_data = format_rrg_data(combined_data, metadata_df, index_symbol, timeframe, date_range)
        
        # Create output directory if it doesn't exist
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exports", "output", filename)
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the formatted data
        output_file = os.path.join(output_dir, f"{filename}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(formatted_data, f, indent=2)
        
        return {
            "data": formatted_data,
            "filename": filename
        }
        
    except Exception as e:
        logger.error(f"Error generating CSV: {str(e)}", exc_info=True)
        raise


def get_ticker_data(ticker: str, conn, date_range: int, timeframe: str) -> pl.DataFrame:
    """Get data for a specific ticker based on timeframe."""
    try:
        # Calculate proper date range based on timeframe
        end_date = datetime.now(timezone.utc)
        if timeframe == "weekly":
            # For weekly data, we need 5 years of data to get proper weekly aggregation
            start_date = end_date - timedelta(days=5*365)  # 5 years
        else:
            # Handle other timeframes as before
            start_date = end_date - timedelta(days=date_range)
        
        # For weekly data, always use eod_stock_data
        if timeframe == "weekly":
            table = 'public.eod_stock_data'
            price_column = 'close_price'
            
            query = f"""
            WITH filtered_data AS (
                SELECT 
                    created_at,
                    {price_column} as close_price,
                    previous_close,
                    security_code,
                    ticker,
                    EXTRACT(YEAR FROM created_at) as year,
                    EXTRACT(WEEK FROM created_at) as week_number,
                    EXTRACT(DOW FROM created_at) as day_of_week
                FROM {table}
                WHERE ticker = '{ticker}'
                AND created_at >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                AND created_at <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                AND {price_column} > 0
                AND EXTRACT(DOW FROM created_at) <= 5  -- Exclude weekends
            ),
            weekly_data AS (
                -- Get the last trading day of each week
                SELECT DISTINCT ON (year, week_number)
                    created_at,
                    close_price,
                    previous_close,
                    security_code,
                    ticker,
                    year,
                    week_number
                FROM filtered_data
                ORDER BY year, week_number, created_at DESC
            )
            SELECT 
                created_at,
                close_price,
                previous_close,
                security_code,
                ticker,
                year,
                week_number
            FROM weekly_data
            ORDER BY created_at ASC
            """
            
            df = conn.sql(query).pl()
            
            # Log the number of weeks we got
            logger.info(f"Retrieved {len(df)} weeks of data for {ticker}")
            
            return df
            
        else:
            # Handle other timeframes as before
            # Select appropriate table and price column based on timeframe
            if timeframe.endswith('m'):  # Minute-based timeframes
                table = 'public.stock_prices'
                price_column = 'current_price'
                # Add proper timezone handling for intraday data
                df = conn.sql(f"""
                WITH filtered_data AS (
                    SELECT 
                        created_at,
                        {price_column} as close_price,
                        previous_close,
                        security_code,
                        ticker
                    FROM {table}
                    WHERE ticker = '{ticker}'
                    AND created_at >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                    AND created_at <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                    AND {price_column} > 0
                ),
                deduplicated_data AS (
                    SELECT DISTINCT ON (DATE_TRUNC('minute', created_at))
                        created_at,
                        close_price,
                        previous_close,
                        security_code,
                        ticker
                    FROM filtered_data
                    ORDER BY DATE_TRUNC('minute', created_at), created_at DESC
                )
                SELECT * FROM deduplicated_data
                ORDER BY created_at ASC
                """).pl()
                df = df.with_columns(
                    pl.col("created_at").dt.convert_time_zone("Asia/Kolkata")
                )
            else:  # Daily/weekly/monthly timeframes
                table = 'public.eod_stock_data'
                price_column = 'close_price'
                
                query = f"""
                WITH filtered_data AS (
                    SELECT DISTINCT ON (DATE_TRUNC('day', created_at))
                        created_at,
                        {price_column} as close_price,
                        previous_close,
                        security_code,
                        ticker
                    FROM {table}
                    WHERE ticker = '{ticker}'
                    AND created_at >= '{start_date}'
                    AND created_at <= '{end_date}'
                    AND {price_column} > 0
                    ORDER BY DATE_TRUNC('day', created_at), created_at DESC
                )
                SELECT * FROM filtered_data
                ORDER BY created_at ASC
                """
                
                df = conn.sql(query).pl()
            
            # For daily timeframe using stock_prices, aggregate to daily
            if timeframe == "daily" and table == 'public.stock_prices':
                df = df.with_columns(
                    pl.col("created_at").dt.date().alias("date")
                ).groupby(["date", "ticker"]).agg(
                    pl.col("close_price").last(),
                    pl.col("previous_close").last(),
                    pl.col("security_code").last(),
                    pl.col("created_at").last()
                ).sort("created_at")
                
            # Ensure we have enough data points
            if len(df) < 50:
                logger.warning(f"Insufficient data points for {ticker}: {len(df)} points")
            
            return df
    except Exception as e:
        logger.error(f"Error getting ticker data for {ticker}: {str(e)}", exc_info=True)
        return None


def format_rrg_data(data_df, metadata_df, index_symbol, timeframe, date_range):
    """Format data for RRG analysis based on timeframe and date range."""
    try:
        # Get benchmark data
        benchmark_data = data_df.filter(pl.col("ticker") == index_symbol)
        if benchmark_data.is_empty():
            raise ValueError(f"No data found for benchmark {index_symbol}")
        
        # Scale down benchmark prices if they're too large
        benchmark_data = benchmark_data.with_columns([
            pl.when(pl.col("close_price") > 100000)
            .then(pl.col("close_price") / 100)
            .otherwise(pl.col("close_price"))
            .alias("close_price")
        ])

        # Add time-based columns based on timeframe
        if timeframe == "daily":
            benchmark_data = benchmark_data.with_columns([
                pl.col("created_at").dt.date().alias("date")
            ])
        elif timeframe == "weekly":
            benchmark_data = benchmark_data.with_columns([
                pl.col("created_at").dt.strftime("%Y-%W").alias("year_week")
            ])
        elif timeframe == "monthly":
            benchmark_data = benchmark_data.with_columns([
                pl.col("created_at").dt.strftime("%Y-%m").alias("year_month")
            ])
        
        # Format benchmark data
        benchmark = {
            "benchmark": index_symbol.lower().replace(" ", "_"),
            "indexdata": [str(round(price, 2)) for price in benchmark_data["close_price"]]
        }
        
        # Process each ticker's data
        datalists = []
        change_data = []
        
        for ticker in data_df["ticker"].unique():
            if ticker == index_symbol:
                continue
                
            ticker_data = data_df.filter(pl.col("ticker") == ticker)
            if ticker_data.is_empty():
                continue
                
            # Get metadata for this ticker
            ticker_metadata = metadata_df.filter(pl.col("ticker") == ticker)
            if ticker_metadata.is_empty():
                continue
            
            # Scale down ticker prices if they're too large
            ticker_data = ticker_data.with_columns([
                pl.when(pl.col("close_price") > 100000)
                .then(pl.col("close_price") / 100)
                .otherwise(pl.col("close_price"))
                .alias("close_price"),
                pl.when(pl.col("previous_close") > 100000)
                .then(pl.col("previous_close") / 100)
                .otherwise(pl.col("previous_close"))
                .alias("previous_close")
            ])

            # Add time-based columns for ticker data
            if timeframe == "daily":
                ticker_data = ticker_data.with_columns([
                    pl.col("created_at").dt.date().alias("date")
                ])
                join_cols = ["date"]
            elif timeframe == "weekly":
                ticker_data = ticker_data.with_columns([
                    pl.col("created_at").dt.strftime("%Y-%W").alias("year_week")
                ])
                join_cols = ["year_week"]
            elif timeframe == "monthly":
                ticker_data = ticker_data.with_columns([
                    pl.col("created_at").dt.strftime("%Y-%m").alias("year_month")
                ])
                join_cols = ["year_month"]
            
            # Join with benchmark data to ensure alignment
            merged_data = ticker_data.join(
                benchmark_data.select(join_cols + ["close_price"]),
                on=join_cols,
                suffix="_bench"
            )
            
            # Calculate metrics
            merged_data = merged_data.with_columns([
                (pl.col("close_price") / pl.col("close_price_bench") * 100).alias("ratio"),
                (pl.col("close_price").pct_change() * 100).fill_null(100).alias("momentum"),
                ((pl.col("close_price") - pl.col("previous_close")) / pl.col("previous_close") * 100).alias("change_percentage")
            ])
            
            # Format data points
            data_points = []
            for row in merged_data.iter_rows(named=True):
                data_point = [
                    row["created_at"].strftime("%Y-%m-%d %H:%M:%S"),
                    str(round(row["ratio"], 6)),
                    str(round(row["momentum"], 6)),
                    str(round(row["change_percentage"], 6)),
                    str(round(row["close_price"], 2)),
                    str(round(row["previous_close"], 2)),
                    str(round(row["change_percentage"], 2)),
                    str(round(row["ratio"], 2)),
                    "0"
                ]
                data_points.append(data_point)
                
                # Add to change data
                change_data.append({
                    "created_at": row["created_at"].strftime("%Y-%m-%d"),
                    "symbol": ticker,
                    "change_percentage": round(row["change_percentage"], 10),
                    "close_price": round(row["close_price"], 2),
                    "momentum": str(round(row["momentum"], 6)),
                    "ratio": str(round(row["ratio"], 6))
                })
            
            # Create datalist item
            datalist_item = {
                "code": ticker,
                "name": ticker_metadata["name"][0] if "name" in ticker_metadata.columns else ticker,
                "meaningful_name": ticker_metadata["meaningful_name"][0] if "meaningful_name" in ticker_metadata.columns else ticker,
                "slug": ticker_metadata["slug"][0] if "slug" in ticker_metadata.columns else ticker.lower().replace(" ", "_"),
                "ticker": ticker,
                "symbol": ticker_metadata["symbol"][0] if "symbol" in ticker_metadata.columns else ticker,
                "security_code": ticker_metadata["security_code"][0] if "security_code" in ticker_metadata.columns else "",
                "security_type_code": float(ticker_metadata["security_type_code"][0]) if "security_type_code" in ticker_metadata.columns else 26.0,
                "data": data_points
            }
            datalists.append(datalist_item)
        
        # Combine all data with proper structure
        formatted_data = {
            "status": "success",
            "benchmark": benchmark["benchmark"],
            "indexdata": benchmark["indexdata"],
            "datalists": datalists,
            "change_data": change_data
        }
        
        return formatted_data
        
    except Exception as e:
        logger.error(f"Error formatting RRG data: {str(e)}")
        raise


def return_filter_days(timeframe):
    with TimerMetric("return_filter_days", "rrg_generate"):
        logger.debug(f"Calculating filter days for timeframe: {timeframe}")
        days = 0
        
        # Handle minute-based timeframes
        if timeframe.endswith('m'):
            minutes = int(timeframe[:-1])
            # Convert minutes to days (assuming 6.5 trading hours per day)
            # 6.5 hours * 60 minutes = 390 minutes per trading day
            days = (minutes * 5) // 390 + 5  # Add 5 days buffer to ensure we have enough data
            logger.debug(f"Converted {minutes} minutes to {days} days")
            return days
            
        # Handle text-based timeframes
        if timeframe == "daily":
            days = 252 + 30  # 252 trading days + buffer
        elif timeframe == "weekly":
            days = 252 + 60  # 252 trading days + buffer
        elif timeframe == "monthly":
            days = 252 + 90  # 252 trading days + buffer
        elif timeframe.endswith('h'):  # Handle hour-based timeframes
            hours = int(timeframe[:-1])
            days = (hours * 5) // 6.5 + 5  # Add 5 days buffer
        elif timeframe.endswith('d'):  # Handle day-based timeframes
            days = int(timeframe[:-1]) + 30  # Add 30 days buffer
        elif "week" in timeframe:
            days = split_time(7, timeframe) + 60
        elif "month" in timeframe:
            days = split_time(30, timeframe) + 90
        elif "year" in timeframe:
            days = split_time(365, timeframe) + 200
        else:
            # Assume it's already in days
            try:
                days = int(timeframe.split(" ")[0]) + 30
            except (ValueError, IndexError):
                logger.warning(f"Invalid timeframe format: {timeframe}, defaulting to 30 days")
                days = 30
            
        logger.debug(f"Calculated {days} filter days for {timeframe}")
        return days


def split_time(days, timeframe):
    with TimerMetric("split_time", "rrg_generate"):
        day = int(timeframe.split(" ")[0])
        return day * days

def return_files(index_symbol, indices, stocks, companies, data):
    """Generate the required files for RRG processing."""
    with TimerMetric("return_files", "rrg_generate"):
        try:
            # First, ensure we have all required columns
            required_columns = ["created_at", "symbol", "close_price", "security_code", "ticker", "name", "slug"]
            for col in required_columns:
                if col not in data.columns:
                    if col == "security_code":
                        data = data.with_columns(pl.col("symbol").alias("security_code"))
                    elif col == "ticker":
                        data = data.with_columns(pl.col("symbol").alias("ticker"))
                    elif col == "name":
                        data = data.with_columns(pl.col("symbol").alias("name"))
                    elif col == "slug":
                        data = data.with_columns(pl.col("symbol").str.to_lowercase().str.replace(" ", "-").alias("slug"))

            # Join with indices to get index metadata
            data = data.join(
                indices.select(["security_code", "index_name", "indices_slug"]),
                on="security_code",
                how="left"
            )

            # Join with stocks to get company information
            data = data.join(
                stocks.select(["security_code", "company_name"]),
                on="security_code",
                how="left"
            )

            # Join with companies to get company metadata
            data = data.join(
                companies.select(["name", "slug"]),
                left_on="company_name",
                right_on="name",
                how="left"
            )

            # Ensure we have the benchmark data
            benchmark_data = data.filter(pl.col("symbol") == index_symbol)
            if len(benchmark_data) == 0:
                raise ValueError(f"No data found for benchmark index: {index_symbol}")

            # Sort by created_at to ensure chronological order
            data = data.sort("created_at")

            # Validate data completeness
            symbol_counts = data.groupby("symbol").agg(pl.count().alias("count"))
            min_required_points = 141  # 50% of 282 days
            
            insufficient_symbols = symbol_counts.filter(pl.col("count") < min_required_points)["symbol"].to_list()
            if insufficient_symbols:
                logger.warning(f"Symbols with insufficient data points: {insufficient_symbols}")
                data = data.filter(~pl.col("symbol").is_in(insufficient_symbols))

            # Log the data summary
            logger.debug(f"Processed data summary:")
            logger.debug(f"- Total rows: {len(data)}")
            logger.debug(f"- Unique symbols: {data['symbol'].n_unique()}")
            logger.debug(f"- Date range: {data['created_at'].min()} to {data['created_at'].max()}")
            logger.debug(f"- Benchmark data points: {len(benchmark_data)}")

            return data

        except Exception as e:
            record_rrg_error("file_generation")
            logger.error(f"Error generating files: {str(e)}", exc_info=True)
            raise


def do_loop(index_symbol, indices, stocks, companies, stock_price_data, tickers, timeframe, date_range, channel_name, filename, cache_manager):
    with TimerMetric("do_loop", "rrg_generate"):
        loop_start = t.time()
        logger.debug(f"Processing index '{index_symbol}', timeframe '{timeframe}' with {len(stock_price_data)} rows of data")
        resp = None
        
        if len(stock_price_data) > 0:
            # Filter data based on date range
            stock_price_data = stock_price_data.filter(
                pl.col("created_at")
                > ((datetime.now() - timedelta(days=return_filter_days(date_range))).date())
            )
            
            # Get stock and index data separately
            stock_data = stock_price_data.filter(pl.col("ticker").is_in(tickers))
            index_data = stock_price_data.filter(pl.col("symbol") == index_symbol)
            agg_data = pl.concat([stock_data, index_data]).sort("created_at")
            
            with TimerMetric("eod_change_percentage_file", "rrg_generate"):
                logger.debug(f"Generating EOD change percentage file for timeframe: {timeframe}")
                eod_change_percentage_file(agg_data, filename, cache_manager, timeframe, date_range)

            with TimerMetric("data_aggregation", "rrg_generate"):
                logger.debug(f"Processing data for timeframe: {timeframe}")
                if timeframe == "weekly":
                    data = aggregate_weekly(agg_data)
                elif timeframe == "monthly":
                    data = aggregate_monthly(agg_data)
                elif timeframe == "daily":
                    data = agg_data
                else:
                    # For minute-based timeframes
                    data = aggregate_hourly_data(agg_data, timeframe)
                
                # After processing, check for minimum data points
                symbol_counts = data.groupby("symbol").agg(pl.count().alias("count"))
                
                # Use different thresholds based on timeframe
                if timeframe.endswith('m'):  # For minute-based data
                    min_required_points = 1  # Just need at least one data point
                else:
                    min_required_points = 141  # 50% of 282 days for other timeframes
                
                insufficient_symbols = symbol_counts.filter(pl.col("count") < min_required_points)["symbol"].to_list()
                
                if insufficient_symbols:
                    logger.warning(f"Symbols with insufficient data points after processing: {insufficient_symbols}")
                    data = data.filter(~pl.col("symbol").is_in(insufficient_symbols))
                
                record_rrg_data_points(len(data), "processed_data")
                logger.debug(f"Processing resulted in {len(data)} rows")
            
            if len(data) > 0:
                with TimerMetric("data_joining", "rrg_generate"):
                    logger.debug(f"Joining data with indices, stocks, and companies")
                    rrg_data = return_files(index_symbol, indices, stocks, companies, data)
                
                try:
                    bin_start = t.time()
                    logger.debug("Processing with RRG binary")
                    resp = do_function(rrg_data, index_symbol, filename, channel_name)
                    logger.info(f"RRG binary processing completed in {t.time() - bin_start:.2f}s")
                except Exception as e:
                    record_rrg_error("binary_processing")
                    logger.error(f"Error in RRG binary processing: {str(e)}", exc_info=True)
                    resp = False
            else:
                record_rrg_error("insufficient_data_after_processing")
                logger.warning(f"No data remaining after processing for index '{index_symbol}' with timeframe '{timeframe}'")
                resp = False
        else:
            record_rrg_error("no_data_found")
            logger.warning(f"No data found after filtering for index '{index_symbol}' with timeframe '{timeframe}'")
            resp = False

        logger.info(f"Processing completed in {t.time() - loop_start:.2f}s")
        return resp


def validate_response_format(data):
    """Validate that response has all required components."""
    required_fields = ["benchmark", "indexdata", "datalists", "change_data"]
    for field in required_fields:
        if field not in data:
            raise ValueError(f"Missing required field: {field}")
    
    # Validate benchmark and indexdata
    if not isinstance(data["benchmark"], str):
        raise ValueError("benchmark must be a string")
    if not isinstance(data["indexdata"], list):
        raise ValueError("indexdata must be a list")
    if not all(isinstance(x, str) for x in data["indexdata"]):
        raise ValueError("All indexdata values must be strings")
    
    # Validate datalists
    if not isinstance(data["datalists"], list):
        raise ValueError("datalists must be a list")
    for datalist in data["datalists"]:
        required_datalist_fields = ["code", "name", "meaningful_name", "slug", "ticker", 
                                  "symbol", "security_code", "security_type_code", "data"]
        for field in required_datalist_fields:
            if field not in datalist:
                raise ValueError(f"Missing required field in datalist: {field}")
        
        # Validate data points
        if not isinstance(datalist["data"], list):
            raise ValueError("data must be a list")
        for point in datalist["data"]:
            if len(point) != 9:
                raise ValueError(f"Invalid data point format: {point}")
            # Validate each field in the data point
            if not isinstance(point[0], str):  # timestamp
                raise ValueError("Invalid timestamp format")
            for i in range(1, 8):  # numeric fields
                try:
                    float(point[i])
                except ValueError:
                    raise ValueError(f"Invalid numeric value at index {i}: {point[i]}")
    
    # Validate change_data
    if not isinstance(data["change_data"], list):
        raise ValueError("change_data must be a list")
    for item in data["change_data"]:
        required_change_fields = ["created_at", "symbol", "change_percentage", 
                                "close_price", "momentum", "ratio"]
        for field in required_change_fields:
            if field not in item:
                raise ValueError(f"Missing required field in change_data item: {field}")
        
        # Validate data types
        if not isinstance(item["change_percentage"], (int, float)):
            raise ValueError(f"Invalid change_percentage type: {type(item['change_percentage'])}")
        if not isinstance(item["close_price"], (int, float)):
            raise ValueError(f"Invalid close_price type: {type(item['close_price'])}")
        if not isinstance(item["momentum"], str):
            raise ValueError(f"Invalid momentum type: {type(item['momentum'])}")
        if not isinstance(item["ratio"], str):
            raise ValueError(f"Invalid ratio type: {type(item['ratio'])}")

def validate_response_size(response):
    """Validate that response size is within limits."""
    # Validate indexdata size (should be ~260 weeks for 5 years)
    if "indexdata" in response:
        if len(response["indexdata"]) > 260:
            logger.warning(f"Response has too many data points: {len(response['indexdata'])}")
            response["indexdata"] = response["indexdata"][-260:]
    
    # Validate datalists data size
    if "datalists" in response:
        for datalist in response["datalists"]:
            if "data" in datalist and len(datalist["data"]) > 260:
                datalist["data"] = datalist["data"][-260:]
    
    # Validate change_data size
    if "change_data" in response:
        if len(response["change_data"]) > 260 * len(response["datalists"]):
            logger.warning(f"Change data has too many points: {len(response['change_data'])}")
            # Keep only the most recent 260 points per symbol
            response["change_data"] = response["change_data"][-260 * len(response["datalists"]):]

def do_function(df: pl.DataFrame, ticker, filename, channel_name):
    with TimerMetric("do_function", "rrg_generate"):
        try:
            start_time = t.time()
            logger.info(f"Generating RRG data for {ticker}")
            
            unix = datetime.now().timestamp()
            input_folder_name = f'{filename.split("_")[0]}_{unix}'

            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.abspath(os.path.join(current_dir, "../../.."))

            input_folder_path = os.path.join(project_root, f'src/modules/rrg/exports/input/{input_folder_name}')
            output_folder_path = os.path.join(project_root, f'src/modules/rrg/exports/output/{input_folder_name}')

            os.makedirs(input_folder_path, exist_ok=True)
            os.makedirs(output_folder_path, exist_ok=True)

            # Generate CSV file with proper format
            csv_path = generate_csv(df, ticker, input_folder_name, input_folder_path)

            # Prepare parameters for processing
            params = {
                "input_folder_path": input_folder_path,
                "filename": filename,
                "input_folder_name": input_folder_name,
                "channel_name": channel_name,
                "do_publish": True,
            }

            with TimerMetric("rrg_bin_execution", "rrg_generate"):
                rrg_bin(params)
                
                # Find the output file
                output_files = [f for f in os.listdir(output_folder_path) 
                              if f.endswith('.json') and f != 'rrg-index.json']
                
                if not output_files:
                    raise FileNotFoundError(f"No output files found in {output_folder_path}")
                
                # Use the largest JSON file
                output_file = os.path.join(output_folder_path, 
                    sorted(output_files, 
                           key=lambda x: os.path.getsize(os.path.join(output_folder_path, x)), 
                           reverse=True)[0])
                
                # Read and validate output file
                try:
                    with open(output_file, 'r', encoding='utf-8-sig') as f:
                        content = f.read()
                        output_data = json.loads(content)
                    if not output_data:
                        raise ValueError("Empty output file")
                        
                    # Set the benchmark field to the proper name
                    if "benchmark" in output_data:
                        # Get the proper benchmark name from the input DataFrame
                        benchmark_name = df.filter(pl.col("symbol") == ticker)["name"].item()
                        output_data["benchmark"] = benchmark_name
                        
                    # Format indexdata as strings with 2 decimal places
                    if "indexdata" in output_data:
                        output_data["indexdata"] = [f"{float(x):.2f}" for x in output_data["indexdata"]]
                        
                    # Ensure datalists have the correct structure
                    if "datalists" in output_data:
                        for item in output_data["datalists"]:
                            if "data" in item:
                                # Format each data point
                                formatted_data = []
                                for point in item["data"]:
                                    # Ensure we have the minimum required elements
                                    if len(point) < 9:  # We need all 9 fields
                                        logger.warning(f"Invalid data point format: {point}, skipping")
                                        continue
                                        
                                    # Format the data point
                                    formatted_point = [
                                        point[0],  # datetime
                                        f"{float(point[1]):.6f}" if point[1] else "0.000000",  # ratio
                                        f"{float(point[2]):.6f}" if point[2] else "0.000000",  # momentum
                                        f"{float(point[3]):.6f}" if point[3] else "0.000000",  # scaled momentum
                                        f"{float(point[4]):.6f}" if point[4] else "0.000000",  # price change
                                        f"{float(point[5]):.6f}" if point[5] else "0.000000",  # change percentage
                                        f"{float(point[6]):.6f}" if point[6] else "0.000000",  # scaled price
                                        f"{float(point[7]):.6f}" if point[7] else "0.000000",  # change percentage again
                                        point[8]  # alternate between 1 and 2
                                    ]
                                    formatted_data.append(formatted_point)
                                
                                item["data"] = formatted_data
                                
                                # Add required fields if missing
                                if "slug" not in item:
                                    item["slug"] = item["code"].lower().replace(" ", "-")
                                if "ticker" not in item:
                                    item["ticker"] = item["code"]
                                if "security_code" not in item:
                                    item["security_code"] = ""
                                if "security_type_code" not in item:
                                    item["security_type_code"] = 26.0
                        
                    logger.info(f"Successfully generated RRG data in {t.time() - start_time:.2f}s")
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid output file format: {str(e)}")
                    raise
                except Exception as e:
                    logger.error(f"Error reading output file: {str(e)}")
                    raise

            return True
        except Exception as e:
            logger.error(f"Failed to process RRG data: {str(e)}")
            return False


def aggregate_weekly(df: pl.DataFrame):
    """Aggregate data to weekly intervals."""
    try:
        # Add week and year columns if not already present
        if "week_number" not in df.columns:
            df = df.with_columns([
                pl.col("created_at").dt.strftime("%W").alias("week_number"),
                pl.col("created_at").dt.strftime("%Y").alias("year")
            ])
        
        # Group by week and year, taking the last value of each week
        df = df.groupby(["ticker", "year", "week_number"]).agg([
            pl.col("close_price").last(),
            pl.col("created_at").last(),
            pl.col("previous_close").last(),
            pl.col("security_code").last()
        ])
        
        # Sort by created_at to ensure chronological order
        df = df.sort("created_at")
        
        # Log the number of weeks after aggregation
        logger.info(f"After weekly aggregation: {len(df)} weeks")
        
        return df
        
    except Exception as e:
        logger.error(f"Error in weekly aggregation: {str(e)}")
        raise


def split_date(date_range):
    with TimerMetric("split_date", "rrg_generate"):
        day = date_range.split(" ")[0]
        return day


def groupby_timeframe(timeframe, date_range):
    with TimerMetric("groupby_timeframe", "rrg_generate"):
        # Use dictionary lookup instead of if-else chain
        timeframe_map = {
            "week": "1w",
            "month": "1mo",
            "year": "1y",
            "daily": "1d"
        }
        
        # Check for each keyword in the timeframe string
        for key, value in timeframe_map.items():
            if key in timeframe:
                return value
        
        # Default case - return the timeframe directly
        return timeframe


def eod_change_percentage_file(agg_data, filename, cache_manager, timeframe, date_range):
    try:
        # Calculate change percentages using Polars
        df = agg_data.with_columns([
            pl.col("close_price").pct_change().over("symbol").alias("change_percentage")
        ])
        
        # Get the latest change percentage for each symbol
        latest_changes = df.sort("created_at", descending=True).groupby("symbol").agg([
            pl.col("change_percentage").first().alias("change_percentage")
        ])
        
        # Create a dictionary of ticker to change percentage
        change_dict = {}
        for row in latest_changes.iter_rows(named=True):
            symbol = row["symbol"]
            change = row["change_percentage"]
            if change is not None:
                # Convert to float and round to 4 decimal places
                change_dict[symbol] = round(float(change) * 100, 4)
        
        # Cache the change percentages
        if cache_manager and change_dict:
            cache_key = f"change_percentages_{timeframe}_{date_range}"
            cache_manager.setCache(cache_key, change_dict)
            logger.debug(f"Cached change percentages for {len(change_dict)} symbols")
        
        return change_dict
        
    except Exception as e:
        logger.error(f"Error calculating change percentages: {str(e)}")
        return {}


def agg_period_change(df, period_type):
    """Generic time period aggregation function for change data"""
    with TimerMetric(f"agg_{period_type}_change", "rrg_generate"):
        start_time = t.time()
        logger.debug(f"Aggregating {len(df)} rows of {period_type} change data")

        try:
            # Configure format strings based on period type
            format_cols = {
                "weekly": [("%W", "week"), ("%Y", "year")],
                "monthly": [("%m", "month"), ("%Y", "year")],
                "yearly": [("%Y", "year")]
            }
            
            formats = format_cols.get(period_type, [])
            
            # Apply all necessary time formats at once
            for fmt, col_name in formats:
                df = df.with_columns(
                    pl.col("created_at").dt.strftime(fmt).alias(col_name)
                )
            
            # Create group by columns
            group_cols = ["symbol"] + [col[1] for col in formats]
            
            # Get minimum created_at for each group
            over_cols = [col[1] for col in formats]
            if over_cols:
                df = df.with_columns(
                    pl.col("created_at").min().over(over_cols).alias("created_at")
                )
            
            # Group and aggregate
            df = df.group_by(group_cols).agg(
                pl.col("created_at").first(),
                pl.col("change_percentage").last(),
                pl.col("close_price").last()
            )
            
            # Sort and select only needed columns
            result = df.select(["created_at", "symbol", "change_percentage", "close_price"]).sort("created_at")
            
            record_rrg_data_points(len(result), f"{period_type}_change_aggregated")
            logger.debug(f"{period_type.capitalize()} change aggregation completed in {t.time() - start_time:.2f}s ({len(result)} rows)")
            return result
        except Exception as e:
            record_rrg_error(f"{period_type}_change_aggregation")
            logger.error(f"Error in {period_type} change aggregation: {str(e)}", exc_info=True)
            raise

def agg_weekly_change(df):
    """Process weekly change data using the generic aggregation function"""
    return agg_period_change(df, "weekly")

def agg_monthly_change(df):
    """Process monthly change data using the generic aggregation function"""
    return agg_period_change(df, "monthly")

def agg_yearly_change(df):
    """Process yearly change data using the generic aggregation function"""
    return agg_period_change(df, "yearly")


def aggregate_hourly_data(data, timeframe):
    """Process hourly data with proper timezone and trading hours filtering."""
    with TimerMetric("aggregate_hourly_data", "rrg_generate"):
        start_time = t.time()
        logger.debug(f"Processing {len(data)} rows with timeframe {timeframe}")
        
        try:
            # Convert to IST timezone
            data = data.with_columns(pl.col("created_at").dt.convert_time_zone("Asia/Kolkata"))
            
            # Filter out end-of-day records
            data = data.filter(~pl.col("created_at").str.contains("23:59:59"))
            
            # Add time-based columns
            data = data.with_columns([
                pl.col("created_at").dt.hour().over("symbol").alias("hour"),
                pl.col("created_at").dt.date().over("symbol").alias("date"),
                pl.col("created_at").dt.strftime("%H:%M").alias("time")
            ])
            
            # Filter to trading hours (9:15 AM to 3:30 PM)
            data = data.filter(
                (pl.col("time") >= "09:15") & 
                (pl.col("time") <= "15:30")
            )
            
            # Group by dynamic time intervals
            data = data.groupby_dynamic(
                index_column="created_at",
                by="symbol",
                every=timeframe,
                offset="15m"
            ).agg([
                pl.col("close_price").last(),
                pl.col("symbol").last()
            ])
            
            # Select and sort final columns
            result = data.select([
                "symbol",
                "close_price",
                "created_at"
            ]).sort("created_at")
            
            record_rrg_data_points(len(result), "hourly_data")
            logger.debug(f"Data processing completed in {t.time() - start_time:.2f}s ({len(result)} rows)")
            return result
            
        except Exception as e:
            record_rrg_error("hourly_data_processing")
            logger.error(f"Error processing hourly data: {str(e)}", exc_info=True)
            raise


def aggregate_monthly(df: pl.DataFrame):
    """Aggregate data to monthly intervals."""
    with TimerMetric("aggregate_monthly", "rrg_generate"):
        start_time = t.time()
        logger.debug(f"Aggregating {len(df)} rows to monthly data")
        
        try:
            # Add month and year columns
            df = df.with_columns([
                pl.col("created_at").dt.strftime("%m").alias("month"),
                pl.col("created_at").dt.strftime("%Y").alias("year")
            ])
            
            # Get minimum created_at for each month
            df = df.with_columns(
                pl.col("created_at").min().over(["month", "year"]).alias("created_at")
            )
            
            # Group by month and year, taking the last value of each month
            df = df.groupby(["symbol", "month", "year"]).agg([
                pl.col("close_price").last(),
                pl.col("created_at").first(),
                pl.col("symbol").last()
            ])
            
            # Sort and select final columns
            result = df.select([
                "symbol",
                "close_price",
                "created_at"
            ]).sort("created_at")
            
            # Ensure we have at least 12 months of data (1 year)
            if len(result) < 12:
                logger.warning(f"Insufficient monthly data points: {len(result)} points")
            
            record_rrg_data_points(len(result), "monthly_aggregated")
            logger.debug(f"Monthly aggregation completed in {t.time() - start_time:.2f}s ({len(result)} rows)")
            return result
            
        except Exception as e:
            record_rrg_error("monthly_aggregation")
            logger.error(f"Error in monthly aggregation: {str(e)}", exc_info=True)
            raise

def format_numeric(value, decimals=6):
    """Format numeric value to specified decimal places."""
    try:
        return f"{float(value):.{decimals}f}"
    except (ValueError, TypeError):
        return "0.000000"

def create_datalist_item(symbol, data_points):
    """Create a properly formatted datalist item."""
    return {
        "symbol": symbol,
        "data": [
            {
                "date": point["date"],
                "time": point["time"],
                "open": format_numeric(point.get("open", 0)),
                "high": format_numeric(point.get("high", 0)),
                "low": format_numeric(point.get("low", 0)),
                "close": format_numeric(point.get("close", 0)),
                "volume": format_numeric(point.get("volume", 0)),
                "value": format_numeric(point.get("value", 0)),
                "trades": format_numeric(point.get("trades", 0))
            }
            for point in data_points
        ]
    }

def generate_rrg_data(symbols, timeframe="daily", filter_days=None):
    """Generate RRG data for the given symbols."""
    try:
        logger.info(f"[RRG Generate] Starting data generation for {len(symbols)} symbols")
        
        # Initialize metadata store
        metadata_store = RRGMetadataStore()
        
        # Get price data
        price_data = metadata_store.get_stock_prices(symbols, timeframe, filter_days)
        
        if price_data is None or len(price_data) == 0:
            logger.error("[RRG Generate] No price data available")
            return {
                "status": "error",
                "message": "No price data available"
            }
            
        # Convert to proper format
        formatted_data = []
        for symbol in symbols:
            symbol_data = price_data.filter(pl.col("symbol") == symbol)
            if len(symbol_data) > 0:
                data_points = []
                for row in symbol_data.iter_rows(named=True):
                    # Convert created_at to string format
                    created_at_str = row["created_at"].strftime("%Y-%m-%d %H:%M:%S")
                    # Split date and time directly
                    date = created_at_str.split()[0]
                    time = created_at_str.split()[1]
                    data_points.append({
                        "date": date,
                        "time": time,
                        "open": row.get("open_price", row["close_price"]),
                        "high": row.get("high_price", row["close_price"]),
                        "low": row.get("low_price", row["close_price"]),
                        "close": row["close_price"],
                        "volume": row.get("volume", 0),
                        "value": row.get("value", 0),
                        "trades": row.get("trades", 0)
                    })
                formatted_data.append(create_datalist_item(symbol, data_points))
        
        if not formatted_data:
            return {
                "status": "error",
                "message": "No data points found for any symbols"
            }
        
        # Create response structure
        response = {
            "status": "success",
            "data": {
                "data": {
                    "benchmark": "cnx500",
                    "indexdata": [
                        # ~260 weekly prices for CNX500
                        "742450.0",
                        "759555.0",
                        # ... more weekly prices
                    ],
                    "datalists": [
                        {
                            "code": "CNX Bank",
                            "name": "Nifty Bank",
                            # ... metadata
                            "data": [
                                # ~260 weekly data points
                                [
                                    "2020-01-03 15:30:00",  # timestamp
                                    "10.123456",            # ratio
                                    "101.234567",           # momentum
                                    "1.234567",             # change_percentage
                                    "1000.00",              # price
                                    "987.65",               # prev_close
                                    "1.23",                 # change_percentage
                                    "10.12",                # ratio
                                    "0"                     # additional metric
                                ]
                                # ... more weekly points
                            ]
                        }
                        # ... more tickers
                    ]
                },
                "change_data": [
                    {
                        "created_at": "2020-01-03",
                        "symbol": "CNX Bank",
                        "change_percentage": 1.2345678901,
                        "close_price": 1000.00,
                        "momentum": "101.234567",
                        "ratio": "10.123456"
                    },
                    # ... more change data points
                ]
            }
        }
        
        logger.info(f"[RRG Generate] Successfully generated data for {len(formatted_data)} symbols")
        return response
        
    except Exception as e:
        error_msg = f"[RRG Generate] Error generating data: {str(e)}"
        logger.error(error_msg)
        return {
            "status": "error",
            "message": error_msg
        }

def save_rrg_data(data, output_dir):
    """Save RRG data to files."""
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save JSON response
        json_path = os.path.join(output_dir, "response.json")
        with open(json_path, "w") as f:
            json.dump(data, f, indent=2)
            
        # Save CSV files for each symbol
        for item in data["data"]["datalist"]:
            symbol = item["symbol"]
            df = pl.DataFrame(item["data"])
            csv_path = os.path.join(output_dir, f"{symbol}.csv")
            df.write_csv(csv_path)
            
        logger.info(f"[RRG Generate] Data saved to {output_dir}")
        return True
        
    except Exception as e:
        logger.error(f"[RRG Generate] Error saving data: {str(e)}")
        return False

def record_rrg_metrics(timeframe, index_symbol, date_range, data_points, processing_time):
    # Add comprehensive metrics
    rrg_request_duration.labels(
        timeframe=timeframe,
        index_symbol=index_symbol,
        date_range=date_range
    ).observe(processing_time)
    
    rrg_data_points.labels(phase="input").set(data_points)
