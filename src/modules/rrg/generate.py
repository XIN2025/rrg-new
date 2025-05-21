import polars as pl
import duckdb
import time as t
from datetime import datetime, timedelta
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

# Get logger for this module
logger = get_logger("rrg_generate")


def get_market_metadata(symbol) -> pl.DataFrame:
    with TimerMetric("get_market_metadata", "rrg_generate"):
        try:
            start_time = t.time()
            logger.debug(f"Getting market metadata for {len(symbol) if isinstance(symbol, list) else 1} symbols")
            metadata_store = RRGMetadataStore()
            metadata_df = metadata_store.get_market_metadata(symbols=symbol)
            
            record_rrg_data_points(len(metadata_df), "market_metadata")
            logger.info(f"Market metadata retrieval completed in {t.time() - start_time:.2f}s ({len(metadata_df)} rows)")
            return metadata_df
        except Exception as e:
            record_rrg_error("market_metadata")
            logger.error(f"Failed to get market metadata: {str(e)}", exc_info=True)
            raise


def generate_csv(tickers, date_range, index_symbol, timeframe, channel_name, filename, cache_manager):
    try:
        logger.info(f"Starting CSV generation with timeframe: {timeframe}")
        
        # Get absolute paths
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_dir, "../../.."))
        
        # Create unique folder for this run
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        input_folder_name = f"rrg_{timestamp}"
        
        # Set paths
        input_folder_path = os.path.join(project_root, "src/modules/rrg/exports/input", input_folder_name)
        output_folder_path = os.path.join(project_root, "src/modules/rrg/exports/output", input_folder_name)
        
        # Create directories
        os.makedirs(input_folder_path, exist_ok=True, mode=0o755)
        os.makedirs(output_folder_path, exist_ok=True, mode=0o755)
        
        # Get market metadata
        market_metadata = get_market_metadata(tickers)
        if market_metadata.is_empty():
            raise ValueError("No market metadata found")
            
        # Get price data
        metadata_store = RRGMetadataStore()
        
        # Convert date_range to filter_days
        filter_days = None
        if isinstance(date_range, str):
            if "month" in date_range.lower():
                filter_days = 30
            elif "week" in date_range.lower():
                filter_days = 7
            elif "year" in date_range.lower():
                filter_days = 365
            else:
                try:
                    filter_days = int(date_range.split()[0])
                except (ValueError, IndexError):
                    filter_days = 30
        else:
            filter_days = 30
            
        # Get price data and ensure it's not empty
        price_data = metadata_store.get_stock_prices(tickers, timeframe, filter_days)
        if price_data.is_empty():
            raise ValueError("No price data found")
            
        # Validate price data
        if "close_price" not in price_data.columns:
            raise ValueError("Missing close_price column in price data")
            
        # Filter out rows with null close prices
        price_data = price_data.filter(pl.col("close_price").is_not_null())
        if price_data.is_empty():
            raise ValueError("No valid price data after filtering nulls")
            
        # Join data
        df = price_data.join(market_metadata, on="symbol", how="left")
        if df.is_empty():
            raise ValueError("No data after joining price and metadata")
            
        # Ensure benchmark symbol exists and is first
        benchmark_data = df.filter(pl.col("symbol") == index_symbol)
        if len(benchmark_data) == 0:
            raise ValueError(f"Benchmark symbol {index_symbol} not found in data")
            
        # Sort symbols to ensure benchmark is first
        all_symbols = df["symbol"].unique().sort()
        benchmark_first = pl.Series([index_symbol] + [s for s in all_symbols if s != index_symbol])
        
        # Reorder the data to ensure benchmark is first
        df = df.sort(["symbol", "created_at"])
        
        # Prepare data for CSV
        rrg_data = []
        
        # Process each symbol
        for symbol in benchmark_first:
            symbol_data = df.filter(pl.col("symbol") == symbol)
            if len(symbol_data) > 0:
                # Get only the last 50 data points
                symbol_data = symbol_data.tail(50)
                
                # Sort by date to ensure chronological order
                symbol_data = symbol_data.sort("created_at")
                
                # Calculate price changes and previous prices
                symbol_data = symbol_data.with_columns([
                    pl.col("close_price").cast(pl.Float64).pct_change().over("symbol").alias("price_change"),
                    pl.col("close_price").cast(pl.Float64).shift(1).over("symbol").alias("prev_price")
                ])
                
                for row in symbol_data.iter_rows(named=True):
                    try:
                        # Format date and time
                        created_at = row["created_at"]
                        date_str = created_at.strftime("%Y-%m-%d")
                        time_str = created_at.strftime("%H:%M:%S")
                        
                        # Get the actual price data with validation
                        close_price = float(row["close_price"]) if row["close_price"] is not None else 0.0
                        prev_price = float(row["prev_price"]) if row["prev_price"] is not None else close_price
                        price_change = float(row["price_change"]) if row["price_change"] is not None else 0.0
                        
                        # Validate price values
                        if close_price <= 0:
                            logger.warning(f"Invalid close price {close_price} for {symbol}, skipping")
                            continue
                            
                        # Create data row in RRG format - exactly matching binary expectations
                        data_row = [
                            f"{date_str} {time_str}",  # datetime
                            str(row["symbol"]),  # symbol
                            f"{close_price:.2f}",  # close price
                            str(row.get("security_code", "")),  # security code
                            f"{prev_price:.2f}",  # previous close
                            str(row.get("ticker", "")),  # ticker
                            str(row.get("name", "")),  # name
                            str(row.get("slug", "")),  # slug
                            str(row.get("security_type_code", "26"))  # security type code
                        ]
                        rrg_data.append(data_row)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error processing row for {symbol}: {str(e)}, skipping")
                        continue
        
        if not rrg_data:
            raise ValueError("No valid data points after processing")
            
        # Create DataFrame from RRG data
        rrg_df = pl.DataFrame(rrg_data, schema=[
            "datetime", "symbol", "close_price", "security_code",
            "previous_close", "ticker", "name", "slug", "security_type_code"
        ])
            
        # Generate CSV with shorter filename
        csv_filename = f"{input_folder_name}.csv"
        csv_path = os.path.join(input_folder_path, csv_filename)
        
        # Write CSV without header and with proper formatting
        rrg_df.write_csv(
            csv_path,
            separator=",",
            quote='"',
            has_header=False,
            null_value="",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )
        
        # Set proper permissions on the input file
        os.chmod(csv_path, 0o644)
        
        # Process with RRG binary
        args = {
            "input_folder_name": input_folder_name,
            "input_folder_path": input_folder_path,
            "output_folder_path": output_folder_path,
            "channel_name": channel_name,
            "do_publish": True
        }
        
        # Process with RRG binary
        result = rrg_bin(args)
        if not result or "error" in result:
            raise ValueError(f"RRG binary processing failed: {result.get('error', 'Unknown error')}")
            
        # Format the response data
        formatted_datalists = []
        for item in result.get("datalists", []):
            formatted_item = {
                "code": item.get("code", ""),
                "name": item.get("name", ""),
                "meaningful_name": item.get("code", ""),
                "slug": item.get("slug", "").lower().replace(" ", "-"),
                "ticker": item.get("ticker", ""),
                "symbol": item.get("symbol", ""),
                "security_code": item.get("security_code", ""),
                "security_type_code": float(item.get("security_type_code", 26.0)),
                "data": []
            }
            
            # Format data points
            for point in item.get("data", []):
                if len(point) >= 9:
                    try:
                        # Convert all values to float and format to 2 decimal places
                        formatted_point = [
                            point[0],  # date
                            f"{float(point[1]):.2f}",  # value1
                            f"{float(point[2]):.2f}",  # value2
                            f"{float(point[3]):.2f}",  # value3
                            f"{float(point[4]):.2f}",  # value4
                            f"{float(point[5]):.2f}",  # value5
                            f"{float(point[6]):.2f}",  # value6
                            f"{float(point[7]):.2f}",  # value7
                            f"{float(point[8]):.2f}"   # value8
                        ]
                        formatted_item["data"].append(formatted_point)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error formatting data point: {str(e)}, skipping")
                        continue
            
            formatted_datalists.append(formatted_item)
        
        # Create response structure
        response = {
            "data": {
                "datalists": formatted_datalists,
                "benchmark": index_symbol.lower(),
                "indexdata": [f"{float(x):.2f}" for x in result.get("indexdata", [])[:50]]  # Limit to 50 points
            },
            "change_data": result.get("change_data"),
            "filename": filename,
            "cacheHit": False
        }
        
        # Save response to output file
        output_filename = f"{index_symbol}_{timeframe}.json"
        output_path = os.path.join(output_folder_path, output_filename)
        
        with open(output_path, 'w') as f:
            json.dump(response, f, indent=2)
            
        return response
        
    except Exception as e:
        logger.error(f"Error in generate_csv: {str(e)}", exc_info=True)
        return {"error": str(e)}


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
                                # Format each data point to have 9 elements
                                formatted_data = []
                                for point in item["data"]:
                                    if len(point) < 9:
                                        point = point + ["0"] * (9 - len(point))
                                    formatted_data.append(point)
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
