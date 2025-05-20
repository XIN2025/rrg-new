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
# Import our metadata and price data store
from src.modules.rrg.metadata_store import (
    get_indices, get_indices_stocks, get_companies, 
    get_stocks, get_market_metadata as get_md,
    get_metadata_status, get_stock_prices, get_eod_stock_data,
    get_price_data_status
)

# Get logger for this module
logger = get_logger("rrg_generate")


def get_market_metadata(symbol) -> pl.DataFrame:
    with TimerMetric("get_market_metadata", "rrg_generate"):
        try:
            start_time = t.time()
            logger.debug(f"Getting market metadata for {len(symbol) if isinstance(symbol, list) else 1} symbols")
            metadata_df = get_md(symbols=symbol)
            
            record_rrg_data_points(len(metadata_df), "market_metadata")
            logger.info(f"Market metadata retrieval completed in {t.time() - start_time:.2f}s ({len(metadata_df)} rows)")
            return metadata_df
        except Exception as e:
            record_rrg_error("market_metadata")
            logger.error(f"Failed to get market metadata: {str(e)}", exc_info=True)
            raise


def generate_csv(tickers, date_range, index_symbol, timeframe, channel_name, filename, cache_manager):
    try:
        # Get absolute paths - use the correct project root
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_dir, "../../.."))  # Fixed: go up 3 levels instead of 4
        
        # Create unique folder for this run
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        input_folder_name = f"rrg_{timestamp}"
        
        # Set input and output paths relative to project root
        input_folder_path = os.path.join(project_root, "src/modules/rrg/exports/input", input_folder_name)
        output_folder_path = os.path.join(project_root, "src/modules/rrg/exports/output", input_folder_name)
        
        print(f"[DEBUG] Project root: {project_root}")
        print(f"[DEBUG] Input folder path: {input_folder_path}")
        print(f"[DEBUG] Output folder path: {output_folder_path}")
        
        # Create base directories first
        base_input_dir = os.path.join(project_root, "src/modules/rrg/exports/input")
        base_output_dir = os.path.join(project_root, "src/modules/rrg/exports/output")
        
        # Create base directories with proper permissions
        os.makedirs(base_input_dir, exist_ok=True, mode=0o755)
        os.makedirs(base_output_dir, exist_ok=True, mode=0o755)
        
        # Verify base directories exist
        if not os.path.exists(base_input_dir) or not os.path.exists(base_output_dir):
            raise RuntimeError(f"Failed to create base directories: {base_input_dir}, {base_output_dir}")
        
        # Create run-specific directories
        os.makedirs(input_folder_path, exist_ok=True, mode=0o755)
        os.makedirs(output_folder_path, exist_ok=True, mode=0o755)
        
        # Verify run-specific directories exist
        if not os.path.exists(input_folder_path) or not os.path.exists(output_folder_path):
            raise RuntimeError(f"Failed to create run directories: {input_folder_path}, {output_folder_path}")
        
        # Get market metadata
        market_metadata = get_market_metadata(tickers)
        if market_metadata.is_empty():
            raise ValueError("No market metadata found")
            
        # Get price data
        price_data = get_stock_prices(tickers, date_range, timeframe)
        if price_data.is_empty():
            raise ValueError("No price data found")
            
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
        df = df.with_columns(
            pl.col("symbol").cast(pl.Categorical).cat.set_ordering("lexical")
        )
        
        # Generate CSV
        csv_path = csv_generator(df, index_symbol, input_folder_name, input_folder_path)
        if not csv_path or not os.path.exists(csv_path):
            raise ValueError(f"Failed to generate CSV file at {csv_path}")
            
        print(f"[DEBUG] CSV file generated at: {csv_path}")
        print(f"[DEBUG] CSV file exists: {os.path.exists(csv_path)}")
        print(f"[DEBUG] CSV file size: {os.path.getsize(csv_path)} bytes")
        
        # Verify the input file exists and has content
        input_file = os.path.join(input_folder_path, f"{input_folder_name}.csv")
        if not os.path.exists(input_file):
            raise ValueError(f"Input file not found at {input_file}")
            
        if os.path.getsize(input_file) == 0:
            raise ValueError(f"Input file is empty at {input_file}")
            
        # Verify CSV content
        with open(input_file, 'r') as f:
            content = f.read()
            if not content.strip():
                raise ValueError("CSV file is empty")
            
            # Split into lines and verify each line
            lines = content.strip().split('\n')
            if not lines:
                raise ValueError("CSV file has no content")
                
            # Get expected number of columns from header
            header = lines[0]
            expected_columns = len(header.split(','))
            print(f"[DEBUG] Expected columns: {expected_columns}")
            
            # Verify each line has the correct number of columns
            for i, line in enumerate(lines[1:], 1):
                columns = line.split(',')
                if len(columns) != expected_columns:
                    print(f"[DEBUG] Line {i} has {len(columns)} columns, expected {expected_columns}")
                    print(f"[DEBUG] Line content: {line}")
                    raise ValueError(f"CSV file has inconsistent number of columns at line {i}")
            
            # Verify no line ends with a comma
            for i, line in enumerate(lines[1:], 1):
                if line.endswith(','):
                    print(f"[DEBUG] Line {i} ends with a comma: {line}")
                    raise ValueError(f"CSV file has trailing comma at line {i}")
            
        # Set proper permissions on the input file
        os.chmod(input_file, 0o644)
        
        # Process with RRG binary
        args = {
            "input_folder_name": input_folder_name,
            "input_folder_path": input_folder_path,
            "output_folder_path": output_folder_path,
            "channel_name": channel_name,
            "do_publish": True
        }
        
        result = rrg_bin(args)
        if not result or "error" in result:
            raise ValueError(f"RRG binary processing failed: {result.get('error', 'Unknown error')}")
            
        # Keep the input files for inspection
        print(f"[DEBUG] Input files preserved in: {input_folder_path}")
        print(f"[DEBUG] Input file contents: {os.listdir(input_folder_path)}")
            
        return result
        
    except Exception as e:
        print(f"Error in generate_csv: {str(e)}")
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
        elif "week" in timeframe:
            days = split_time(7, timeframe) + 60
        elif "month" in timeframe:
            days = split_time(30, timeframe) + 90
        elif "year" in timeframe:
            days = split_time(365, timeframe) + 200
        else:
            # Handle numeric timeframes like "5 days"
            days = int(timeframe.split(" ")[0]) + 30
            
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
