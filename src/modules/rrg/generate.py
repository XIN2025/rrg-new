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
    record_rrg_data_points, record_rrg_error
)
from src.utils.logger import get_logger
from src.utils.duck_pool import get_duckdb_connection
from src.modules.rrg.metadata_store import RRGMetadataStore
from src.modules.rrg.time_utils import return_filter_days, split_time
import logging
from src.utils.clickhouse import get_clickhouse_connection
from typing import List, Tuple
import pandas as pd
import numpy as np

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


def check_table_data():
    """Check if required tables exist and have data."""
    with get_duckdb_connection() as conn:
        # Check market_metadata table
        metadata_query = "SELECT COUNT(*) as count FROM public.market_metadata"
        metadata_count = conn.execute(metadata_query).fetchone()[0]
        logger.info(f"Found {metadata_count} records in market_metadata table")
        
        # Check stock_prices table
        prices_query = "SELECT COUNT(*) as count FROM public.stock_prices"
        prices_count = conn.execute(prices_query).fetchone()[0]
        logger.info(f"Found {prices_count} records in stock_prices table")
        
        # Check eod_stock_data table
        eod_query = "SELECT COUNT(*) as count FROM public.eod_stock_data"
        eod_count = conn.execute(eod_query).fetchone()[0]
        logger.info(f"Found {eod_count} records in eod_stock_data table")
        
        return metadata_count > 0 and prices_count > 0 and eod_count > 0


def check_clickhouse_data():
    """Check data in ClickHouse."""
    try:
        conn = get_clickhouse_connection()
        
        # Check current_stock_price table
        current_prices = conn.execute("""
            SELECT 
                symbol,
                ticker,
                current_price as close_price,
                created_at,
                security_code,
                previous_close
            FROM indiacharts.current_stock_price
            WHERE current_price > 0
            ORDER BY created_at DESC
            LIMIT 5
        """).fetchall()
        
        # Check historical_stock_price table
        historical_prices = conn.execute("""
            SELECT 
                symbol,
                ticker,
                close_price,
                created_at,
                security_code,
                previous_close
            FROM indiacharts.historical_stock_price
            WHERE close_price > 0
            ORDER BY created_at DESC
            LIMIT 5
        """).fetchall()
        
        logger.info("=== ClickHouse Data Check ===")
        logger.info("\nCurrent Stock Prices (Latest 5 rows):")
        for row in current_prices:
            logger.info(f"Symbol: {row[0]}, Ticker: {row[1]}, Price: {row[2]}, Date: {row[3]}, Code: {row[4]}")
            
        logger.info("\nHistorical Stock Prices (Latest 5 rows):")
        for row in historical_prices:
            logger.info(f"Symbol: {row[0]}, Ticker: {row[1]}, Price: {row[2]}, Date: {row[3]}, Code: {row[4]}")
            
        return {
            "current_prices": current_prices,
            "historical_prices": historical_prices
        }
    except Exception as e:
        logger.warning(f"Could not connect to ClickHouse, continuing with DuckDB data only: {str(e)}")
        return None


def generate_csv(tickers, date_range, index_symbol, timeframe, channel_name, filename, cache_manager):
    """
    Generate CSV file for RRG analysis.
    """
    try:
        # Get metadata for all tickers
        metadata_df = get_market_metadata(tickers + [index_symbol])
        
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
        
        # Process data for each ticker including index
        processed_data = []
        for ticker in tickers + [index_symbol]:
            try:
                # Get data for ticker
                ticker_data, ticker_metadata = get_ticker_data([ticker], date_range, index_symbol)
                if ticker_data is not None and not ticker_data.empty:
                    processed_data.append(ticker_data)
            except Exception as e:
                logger.error(f"Error processing ticker {ticker}: {str(e)}", exc_info=True)
                continue
        
        if not processed_data:
            raise ValueError("No valid data found for any ticker")
        
        # Combine all data
        combined_data = pd.concat(processed_data)
        
        # Format the data
        formatted_data = format_rrg_data(combined_data, metadata_df, index_symbol)
        
        # Create output directory if it doesn't exist
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exports", "output", filename)
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the formatted data
        output_file = os.path.join(output_dir, f"{filename}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(formatted_data, f, indent=2)
        
        # Create input CSV file for RRG binary
        input_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exports", "input", filename)
        os.makedirs(input_dir, exist_ok=True)
        
        # Create CSV with proper format
        csv_data = []
        
        # Row 1: Index symbol and empty cells
        csv_data.append([index_symbol] + [""] * (len(tickers)))
        
        # Row 2: Column headers
        csv_data.append(["column1"] + tickers)
        
        # Row 3: URLs
        urls = []
        for ticker in tickers:
            metadata = metadata_df.filter(pl.col("ticker") == ticker)
            if not metadata.is_empty():
                slug = metadata["slug"][0]
                urls.append(f"https://strike-analytics-dev.netlify.app/stocks/{slug}")
            else:
                urls.append(f"https://strike-analytics-dev.netlify.app/stocks/{ticker.lower().replace(' ', '-')}")
        csv_data.append(["URL"] + urls)
        
        # Row 4: Names
        names = []
        for ticker in tickers:
            metadata = metadata_df.filter(pl.col("ticker") == ticker)
            if not metadata.is_empty():
                name = metadata["name"][0]
                if "National Stock Exchange" in name:
                    names.append(ticker.lower().replace(" ", "_"))
                else:
                    names.append(name)
            else:
                names.append(ticker)
        csv_data.append(["Name"] + names)
        
        # Get all unique dates
        dates = combined_data["created_at"].unique()
        dates = np.sort(dates)
        
        # Get index data first
        index_data = combined_data[combined_data["symbol"] == index_symbol]
        
        # Data rows: Dates and prices
        for date in dates:
            row = [date.strftime("%Y-%m-%d")]
            
            # Add index price first
            index_price = index_data[index_data["created_at"] == date]
            if not index_price.empty:
                row.append(str(index_price["close_price"].iloc[0]))
            else:
                row.append("")
            
            # Add other ticker prices
            for ticker in tickers:
                ticker_data = combined_data[
                    (combined_data["symbol"] == ticker) & 
                    (combined_data["created_at"] == date)
                ]
                if not ticker_data.empty:
                    row.append(str(ticker_data["close_price"].iloc[0]))
                else:
                    row.append("")
            csv_data.append(row)
        
        # Write CSV file
        csv_path = os.path.join(input_dir, f"{filename}.csv")
        with open(csv_path, "w", encoding="utf-8") as f:
            for row in csv_data:
                f.write("\t".join(row) + "\n")
        
        return {
            "data": formatted_data,
            "filename": filename
        }
        
    except Exception as e:
        logger.error(f"Error generating CSV: {str(e)}", exc_info=True)
        raise


def get_ticker_data(tickers: List[str], date_range: str, index_symbol: str = "CNX500") -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Get data for specific tickers from DuckDB.
    """
    max_retries = 3
    retry_delay = 1  # seconds
    
    for attempt in range(max_retries):
        try:
            with get_duckdb_connection() as conn:
                # Calculate cutoff date based on date_range
                cutoff_date = None
                if date_range == "1 month":
                    cutoff_date = "current_date - interval '1' month"
                elif date_range == "3 months":
                    cutoff_date = "current_date - interval '3' month"
                elif date_range == "6 months":
                    cutoff_date = "current_date - interval '6' month"
                elif date_range == "1 year":
                    cutoff_date = "current_date - interval '1' year"
                else:
                    cutoff_date = "current_date - interval '3' month"  # Default to 3 months
                    
                # Get metadata for all tickers - only get required columns
                metadata_query = f"""
                SELECT 
                    ticker,
                    symbol,
                    name,
                    slug,
                    security_code
                FROM public.market_metadata 
                WHERE ticker IN ({','.join([f"'{ticker}'" for ticker in tickers])})
                """
                
                metadata_df = conn.execute(metadata_query).df()
                logger.info(f"Retrieved metadata for {len(metadata_df)} tickers")
                
                # Get stock prices with only required columns and proper filtering
                stock_prices_query = f"""
                WITH filtered_data AS (
                    SELECT 
                        ticker as symbol,
                        created_at,
                        current_price as close_price,
                        previous_close,
                        security_code
                    FROM public.stock_prices 
                    WHERE ticker IN ({','.join([f"'{ticker}'" for ticker in tickers])})
                    AND date_trunc('day', created_at) >= date_trunc('day', {cutoff_date})
                    AND current_price > 0
                    ORDER BY created_at ASC
                )
                SELECT * FROM filtered_data
                """
                stock_prices_df = conn.execute(stock_prices_query).df()
                logger.info(f"Retrieved {len(stock_prices_df)} rows of stock prices")
                
                # Get EOD data with only required columns and proper filtering
                eod_query = f"""
                WITH filtered_data AS (
                    SELECT 
                        ticker as symbol,
                        created_at,
                        close_price,
                        previous_close,
                        security_code
                    FROM public.eod_stock_data 
                    WHERE ticker IN ({','.join([f"'{ticker}'" for ticker in tickers])})
                    AND date_trunc('day', created_at) >= date_trunc('day', {cutoff_date})
                    AND close_price > 0
                    ORDER BY created_at ASC
                )
                SELECT * FROM filtered_data
                """
                eod_df = conn.execute(eod_query).df()
                logger.info(f"Retrieved {len(eod_df)} rows of EOD data")
                
                # Combine stock prices and EOD data
                combined_df = pd.concat([stock_prices_df, eod_df])
                
                # Remove duplicates based on symbol and created_at
                combined_df = combined_df.drop_duplicates(subset=['symbol', 'created_at'], keep='last')
                
                # Sort by created_at
                combined_df = combined_df.sort_values('created_at')
                
                return combined_df, metadata_df
                
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"All {max_retries} attempts failed: {str(e)}", exc_info=True)
                raise
    
    raise Exception("Failed to get ticker data after all retries")


def format_rrg_data(data_df: pd.DataFrame, metadata_df: pd.DataFrame, index_symbol: str) -> dict:
    """
    Format data for RRG analysis.
    """
    try:
        # Convert Polars DataFrame to Pandas if needed
        if hasattr(data_df, 'to_pandas'):
            data_df = data_df.to_pandas()
        if hasattr(metadata_df, 'to_pandas'):
            metadata_df = metadata_df.to_pandas()
            
        # Join with metadata using symbol (data) == ticker (metadata)
        data_df = data_df.merge(metadata_df, left_on='symbol', right_on='ticker', how='left', suffixes=('', '_meta'))
        
        # Initialize formatted data structure
        formatted_data = {
            "data": {
                "benchmark": index_symbol,
                "indexdata": [],
                "datalists": []
            },
            "change_data": [],
            "filename": None,
            "cacheHit": False
        }
        
        # Process index data
        index_data = data_df[data_df['symbol'] == index_symbol].sort_values('created_at')
        if not index_data.empty:
            formatted_data["data"]["indexdata"] = [
                f"{price:.2f}" for price in index_data['close_price'].tolist()
            ]
        
        # Create benchmark price lookup
        benchmark_prices = dict(zip(index_data['created_at'], index_data['close_price']))
        
        # Process tickers in order
        processed_tickers = set()
        for ticker in data_df['symbol'].unique():
            if ticker in processed_tickers or ticker == index_symbol:
                continue
            
            ticker_data = data_df[data_df['symbol'] == ticker].sort_values('created_at')
            if ticker_data.empty:
                continue
            
            # Get ticker metadata (from merged columns)
            ticker_meta = ticker_data.iloc[0]
            
            # Create ticker object
            ticker_obj = {
                "code": ticker_meta['symbol'],
                "name": ticker_meta['name'],
                "meaningful_name": ticker_meta.get('meaningful_name', ''),
                "slug": ticker_meta.get('slug', ''),
                "symbol": ticker_meta['symbol'],
                "security_code": ticker_meta.get('security_code', ''),
                "data": []
            }
            
            # Calculate ratios and momentum
            for row_num, (_, row) in enumerate(ticker_data.iterrows()):
                if row['close_price'] <= 0:
                    continue
                        
                # Get benchmark price with fallback
                benchmark_price = benchmark_prices.get(row['created_at'])
                if benchmark_price is None or benchmark_price <= 0:
                    continue
            
                # Calculate ratio against benchmark
                ratio = (row['close_price'] / benchmark_price) * 100

                # Calculate momentum (5-day change)
                if row_num >= 5:
                    prev_price = ticker_data.iloc[row_num-5]['close_price']
                    if prev_price > 0:
                        momentum = ((row['close_price'] - prev_price) / prev_price) * 100
                else:
                    momentum = 0

                # Format timestamp
                timestamp = row['created_at'].strftime("%Y-%m-%d %H:%M:%S")

                # Add data point
                ticker_obj["data"].append([
                    timestamp,
                    f"{ratio:.6f}",
                    f"{momentum:.6f}",
                    safe_float_fmt(row['close_price']),
                    safe_float_fmt(row.get('high_price', row['close_price'])),
                    safe_float_fmt(row.get('low_price', row['close_price'])),
                    safe_float_fmt(row.get('volume', 0)),
                    safe_float_fmt(row.get('turnover', 0)),
                    "0"  # Default indicator
                ])
            
            # Add to datalists if we have data
            if ticker_obj["data"]:
                formatted_data["data"]["datalists"].append(ticker_obj)
                processed_tickers.add(ticker)
                
                # Add to change_data
                first_data = ticker_obj["data"][0]
                last_data = ticker_obj["data"][-1]
                try:
                    change_percentage = ((float(last_data[2]) - float(first_data[2])) / float(first_data[2])) * 100 if float(first_data[2]) != 0 else 0
                except Exception:
                    change_percentage = 0
                formatted_data["change_data"].append({
                    "created_at": first_data[0],
                    "symbol": ticker_meta['symbol'],
                    "change_percentage": f"{change_percentage:.6f}",
                    "close_price": first_data[2],
                    "momentum": first_data[1],
                    "ratio": first_data[0]
                })
        
        if not formatted_data["data"]["datalists"]:
            raise ValueError("No valid data points found for any ticker")
            
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


def do_loop(
    index_symbol,
    indices,
    stocks,
    companies,
    stock_price_data,
    tickers,
    timeframe,
    date_range,
    channel_name,
    filename,
    cache_manager
):
    with TimerMetric("do_loop", "rrg_generate"):
        loop_start = t.time()
        logger.debug(f"Processing index '{index_symbol}', timeframe '{timeframe}' with {len(stock_price_data)} rows of data")
        resp = None
        if len(stock_price_data) > 0:
            agg_data = stock_price_data.sort("created_at")
            
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
                    # Just directly use aggregate_hourly_data
                    data = aggregate_hourly_data(agg_data, timeframe)
                
                # After processing, check for minimum data points
                symbol_counts = data.groupby("symbol").agg(pl.count().alias("count"))
                
                # Use different thresholds based on timeframe
                if timeframe.endswith('m'):  # For hourly data
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


def do_function(request_body: dict) -> bool:
    """
    Generate RRG data for specified ticker.
    """
    try:
        # Extract parameters from request body
        tickers = request_body.get("tickers", [])
        date_range = request_body.get("date_range", "3 months")
        timeframe = request_body.get("timeframe", "daily")
        index_symbol = request_body.get("index_symbol", "CNX500")
        last_traded_time = request_body.get("last_traded_time", datetime.now().isoformat())
        
        # Validate input parameters
        if not tickers:
            raise ValueError("No tickers provided")
            
        if not isinstance(tickers, list):
            raise ValueError("Tickers must be a list")
            
        if not all(isinstance(t, str) for t in tickers):
            raise ValueError("All tickers must be strings")
            
        valid_date_ranges = ["1 month", "3 months", "6 months", "1 year"]
        if date_range not in valid_date_ranges:
            raise ValueError(f"Invalid date_range. Must be one of: {valid_date_ranges}")
            
        valid_timeframes = ["daily", "weekly", "monthly"]
        if timeframe not in valid_timeframes:
            raise ValueError(f"Invalid timeframe. Must be one of: {valid_timeframes}")
            
        if not isinstance(index_symbol, str):
            raise ValueError("index_symbol must be a string")
        
        # Create input and output folder paths
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        input_folder_path = os.path.join("data", "input", timestamp)
        output_folder_path = os.path.join("data", "output", timestamp)
        
        # Create folders if they don't exist
        os.makedirs(input_folder_path, exist_ok=True)
        os.makedirs(output_folder_path, exist_ok=True)
        
        # Generate CSV file
        filename = generate_csv(tickers, date_range, timeframe, index_symbol, last_traded_time, input_folder_path)
        
        # Process the data
        data_df, metadata_df = get_ticker_data(tickers, date_range, index_symbol)
        
        # Validate data
        if data_df.empty:
            raise ValueError("No data retrieved from database")
            
        if metadata_df.empty:
            raise ValueError("No metadata retrieved from database")
            
        # Check if we have data for all tickers
        missing_tickers = set(tickers) - set(data_df['symbol'].unique())
        if missing_tickers:
            raise ValueError(f"No data found for tickers: {missing_tickers}")
            
        # Check if we have data for the index
        if index_symbol not in data_df['symbol'].unique():
            raise ValueError(f"No data found for index symbol: {index_symbol}")
        
        formatted_data = format_rrg_data(data_df, metadata_df, index_symbol)
        
        # Generate output filename
        output_filename = f"{index_symbol}_{'_'.join(tickers)}_{timeframe}_{date_range}_{last_traded_time}.json"
        formatted_data["filename"] = output_filename
        
        # Write output JSON file
        output_file_path = os.path.join(output_folder_path, output_filename)
        with open(output_file_path, 'w') as f:
            json.dump(formatted_data, f, indent=2)
            
        logger.info(f"Successfully generated RRG data for {len(tickers)} tickers")
        return True
        
    except Exception as e:
        logger.error(f"Error generating RRG data: {str(e)}")
        return False


def aggregate_weekly(df: pl.DataFrame):
    with TimerMetric("aggregate_weekly", "rrg_generate"):
        start_time = t.time()
        logger.debug(f"Aggregating {len(df)} rows to weekly data")
        
        try:
            df = df.with_columns(
                pl.col("created_at").dt.strftime("%W").alias("week"),
                pl.col("created_at").dt.strftime("%Y").alias("year"),
            )
            df = df.with_columns(
                pl.col("created_at").min().over(["week", "year"]).alias("created_at")
            )

            df = df.group_by(["security_code", "week", "year"]).agg(
                pl.col("close_price").last(),
                pl.col("created_at").first(),
                pl.col("symbol").last(),
                pl.col("ticker").last(),
            )

            df = df.sort("created_at")
            df = df[["security_code", "close_price", "symbol", "created_at", "ticker"]].sort(
                "created_at"
            )
            
            record_rrg_data_points(len(df), "weekly_aggregated")
            logger.debug(f"Weekly aggregation completed in {t.time() - start_time:.2f}s ({len(df)} rows)")
            return df
        except Exception as e:
            record_rrg_error("weekly_aggregation")
            logger.error(f"Error in weekly aggregation: {str(e)}", exc_info=True)
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
        
        # Skip cache operations
        logger.debug(f"Calculated change percentages for {len(change_dict)} symbols")
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
    """Process hourly data without aggregation, just sorting by time."""
    with TimerMetric("aggregate_hourly_data", "rrg_generate"):
        start_time = t.time()
        logger.debug(f"Processing {len(data)} rows with timeframe {timeframe}")
        
        try:
            # Convert UTC to IST for logging purposes
            data = data.with_columns([
                (pl.col("created_at") + pl.duration(hours=5, minutes=30)).alias("ist_time")
            ])
            
            # Log time range in both UTC and IST
            min_utc = data["created_at"].min()
            max_utc = data["created_at"].max()
            min_ist = data["ist_time"].min()
            max_ist = data["ist_time"].max()
            logger.debug(f"Time range in UTC: {min_utc} to {max_utc}")
            logger.debug(f"Time range in IST: {min_ist} to {max_ist}")
            
            # Just sort by time and return all data points
            result = data.sort("created_at")
            
            # Log the number of data points per symbol
            symbol_counts = result.groupby("symbol").agg(pl.count().alias("count"))
            logger.debug(f"Data points per symbol:\n{symbol_counts}")
            
            record_rrg_data_points(len(result), "hourly_data")
            logger.debug(f"Data processing completed in {t.time() - start_time:.2f}s ({len(result)} rows)")
            return result
        except Exception as e:
            record_rrg_error("hourly_data_processing")
            logger.error(f"Error processing hourly data: {str(e)}", exc_info=True)
            raise


def aggregate_monthly(df: pl.DataFrame):
    with TimerMetric("aggregate_monthly", "rrg_generate"):
        start_time = t.time()
        logger.debug(f"Aggregating {len(df)} rows to monthly data")
        
        try:
            df = df.with_columns(
                pl.col("created_at").dt.strftime("%m").alias("month"),
                pl.col("created_at").dt.strftime("%Y").alias("year"),
            )
            df = df.with_columns(
                pl.col("created_at").min().over(["month", "year"]).alias("created_at")
            )

            df = df.group_by(["security_code", "month", "year"]).agg(
                pl.col("close_price").last(),
                pl.col("created_at").first(),
                pl.col("symbol").last(),
            )
            df = df.sort("created_at")
            df = df[["security_code", "close_price", "symbol", "created_at"]].sort("created_at")
            
            record_rrg_data_points(len(df), "monthly_aggregated")
            logger.debug(f"Monthly aggregation completed in {t.time() - start_time:.2f}s ({len(df)} rows)")
            return df
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
                "datalist": formatted_data
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

def check_schema():
    """Check if database schema is properly set up."""
    with get_duckdb_connection() as conn:
        # Check if tables exist
        tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
            WHERE table_schema = 'public' 
        """
        tables = [row[0] for row in conn.execute(tables_query).fetchall()]
        required_tables = ['market_metadata', 'stock_prices', 'eod_stock_data']
        
        missing_tables = [table for table in required_tables if table not in tables]
        if missing_tables:
            logger.error(f"Missing required tables: {missing_tables}")
            return False
            
        return True

def test_db_connection():
    """Test database connection and basic queries."""
    try:
        with get_duckdb_connection() as conn:
            # Test simple query
            result = conn.execute("SELECT 1").fetchone()
            if result[0] != 1:
                raise Exception("Basic query test failed")
            logger.info("Database connection test passed")
        return True
    except Exception as e:
        logger.error(f"Database connection test failed: {str(e)}")
        return False

def test_date_query():
    """Test date-based queries."""
    try:
        with get_duckdb_connection() as conn:
            # Test date range query
            query = """
            SELECT COUNT(*) as count 
        FROM public.stock_prices
            WHERE date_trunc('day', created_at) >= date_trunc('day', current_date - interval '3' month)
            """
            result = conn.execute(query).fetchone()
            logger.info(f"Found {result[0]} records in last 3 months")
            return True
    except Exception as e:
        logger.error(f"Date query test failed: {str(e)}")
        return False

def test_full_pipeline():
    """Test the full RRG data pipeline with sample parameters."""
    try:
        # Sample parameters (replace with real tickers/index if needed)
        tickers = ["RELIANCE", "TCS"]
        date_range = "3 months"
        index_symbol = "CNX500"
        
        print("[TEST] Running get_ticker_data...")
        data_df, metadata_df = get_ticker_data(tickers + [index_symbol], date_range, index_symbol)
        print(f"[TEST] Data rows: {len(data_df)}, Metadata rows: {len(metadata_df)}")
        
        print("[TEST] Running format_rrg_data...")
        formatted = format_rrg_data(data_df, metadata_df, index_symbol)
        print("[TEST] Output JSON structure:")
        import pprint
        pprint.pprint(formatted)
        print("[TEST] Test completed successfully.")
        return formatted
    except Exception as e:
        print(f"[TEST] Error in full pipeline: {e}")
        import traceback
        traceback.print_exc()
        return None

def safe_float_fmt(value):
    try:
        return f"{float(value):.6f}"
    except Exception:
        return "0.000000"

def test_rrg_generation():
    """Test RRG generation functionality."""
    print("\n=== Starting RRG Generation Test ===\n")
    
    # Test data
    test_tickers = ["RELIANCE", "TCS", "HDFCBANK"]
    test_index = "NIFTY50"
    test_date_range = "3 months"
    
    try:
        print("Test 1: Checking data retrieval...")
        data_df, metadata_df = get_ticker_data(test_tickers + [test_index], test_date_range, test_index)
        
        # Force conversion to pandas DataFrame if needed
        if hasattr(data_df, 'to_pandas'):
            data_df = data_df.to_pandas()
        if hasattr(metadata_df, 'to_pandas'):
            metadata_df = metadata_df.to_pandas()
        
        if data_df.empty or metadata_df.empty:
            raise Exception("No data retrieved")
            
        print(f"✓ Successfully retrieved data for {len(data_df)} rows and {len(metadata_df)} tickers")
        print("Data DataFrame columns:", data_df.columns.tolist())
        print("Data DataFrame dtypes:\n", data_df.dtypes)
        print("Data DataFrame head:")
        print(data_df.head(10))
        print("Metadata DataFrame columns:", metadata_df.columns.tolist())
        print("Metadata DataFrame dtypes:\n", metadata_df.dtypes)
        print("Metadata DataFrame head:")
        print(metadata_df.head(10))
        print(f"Unique symbols in data_df: {data_df['symbol'].unique()}")
        print(f"Unique tickers in metadata_df: {metadata_df['ticker'].unique() if 'ticker' in metadata_df.columns else 'N/A'}")
        print(f"Date range in data_df: {data_df['created_at'].min()} to {data_df['created_at'].max()}")
        
        print("\nTest 2: Checking data formatting...")
        formatted_data = format_rrg_data(data_df, metadata_df, test_index)
        
        if not formatted_data or "data" not in formatted_data:
            raise Exception("Data formatting failed")
            
        print("✓ Successfully formatted data for RRG visualization")
        
        print("\n=== All Tests Passed Successfully ===\n")
        return True
        
    except Exception as e:
        print(f"\nTest failed with error: {str(e)}")
        return False

if __name__ == "__main__":
    test_rrg_generation()
