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
    """Check the actual data in our tables."""
    try:
        conn = get_duckdb_connection()
        
        # Check stock_prices table
        stock_prices = conn.sql("""
            SELECT 
                symbol,
                ticker,
                current_price,
                created_at,
                security_code
            FROM public.stock_prices
            WHERE current_price > 0
            ORDER BY created_at DESC
            LIMIT 5
        """).fetchall()
        
        # Check eod_stock_data table
        eod_data = conn.sql("""
            SELECT 
                symbol,
                ticker,
                close_price,
                created_at,
                security_code
            FROM public.eod_stock_data
            WHERE close_price > 0
            ORDER BY created_at DESC
            LIMIT 5
        """).fetchall()
        
        # Check market_metadata table
        metadata = conn.sql("""
            SELECT 
                symbol,
                ticker,
                security_type_code,
                name,
                slug
            FROM public.market_metadata
            WHERE security_type_code IN ('5', '26')
            LIMIT 5
        """).fetchall()
        
        logger.info("=== Table Data Check ===")
        logger.info("\nStock Prices Table (Latest 5 rows):")
        for row in stock_prices:
            logger.info(f"Symbol: {row[0]}, Ticker: {row[1]}, Price: {row[2]}, Date: {row[3]}, Code: {row[4]}")
            
        logger.info("\nEOD Stock Data Table (Latest 5 rows):")
        for row in eod_data:
            logger.info(f"Symbol: {row[0]}, Ticker: {row[1]}, Price: {row[2]}, Date: {row[3]}, Code: {row[4]}")
            
        logger.info("\nMarket Metadata Table (Sample):")
        for row in metadata:
            logger.info(f"Symbol: {row[0]}, Ticker: {row[1]}, Type: {row[2]}, Name: {row[3]}, Slug: {row[4]}")
            
        return {
            "stock_prices": stock_prices,
            "eod_data": eod_data,
            "metadata": metadata
        }
    except Exception as e:
        logger.error(f"Error checking table data: {str(e)}", exc_info=True)
        return None


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
    Generate CSV data for RRG analysis.
    """
    with TimerMetric("generate_csv", "rrg_generate"):
        try:
            # Get market metadata first
            metadata_df = get_market_metadata(tickers + [index_symbol])
            
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
            
            # Process data for each ticker including index
            processed_data = []
            for ticker in tickers + [index_symbol]:
                try:
                    # Get data for ticker
                    ticker_data = get_ticker_data(ticker, conn, days)
                    if ticker_data is not None and not ticker_data.is_empty():
                        processed_data.append(ticker_data)
                except Exception as e:
                    logger.error(f"Error processing ticker {ticker}: {str(e)}", exc_info=True)
                    continue
            
            if not processed_data:
                raise ValueError("No valid data found for any ticker")
            
            # Combine all data
            combined_data = pl.concat(processed_data)
            
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
            dates = combined_data["created_at"].unique().sort()
            
            # Get index data first
            index_data = combined_data.filter(pl.col("ticker") == index_symbol)
            
            # Data rows: Dates and prices
            for date in dates:
                row = [date.strftime("%Y-%m-%d")]
                
                # Add index price first
                index_price = index_data.filter(pl.col("created_at") == date)
                if not index_price.is_empty():
                    row.append(str(index_price["close_price"][0]))
                else:
                    row.append("")
                
                # Add other ticker prices
                for ticker in tickers:
                    ticker_data = combined_data.filter(
                        (pl.col("ticker") == ticker) & 
                        (pl.col("created_at") == date)
                    )
                    if not ticker_data.is_empty():
                        row.append(str(ticker_data["close_price"][0]))
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


def get_ticker_data(ticker: str, conn, date_range: int = 3650) -> pl.DataFrame:
    """Get data for a specific ticker."""
    try:
        # First check if the ticker exists in market_metadata
        metadata_check = conn.sql(f"""
            SELECT symbol, ticker, security_type_code, name
            FROM public.market_metadata
            WHERE ticker = '{ticker}'
        """).fetchall()
        
        if not metadata_check:
            logger.error(f"Ticker {ticker} not found in market_metadata")
            return None
            
        logger.info(f"Found metadata for {ticker}: {metadata_check[0]}")
        
        # Define the required columns that all DataFrames should have
        required_columns = ["created_at", "symbol", "close_price", "security_code", "ticker", "previous_close"]
        
        # Query for stock prices
        stock_price_query = f"""
        SELECT 
            created_at,
            symbol,
            current_price as close_price,
            security_code,
            previous_close,
            ticker
        FROM public.stock_prices 
        WHERE ticker = '{ticker}'
        AND current_price > 0
        AND current_price < 1000000
        AND created_at >= CURRENT_DATE - INTERVAL '{date_range} days'
        """
        
        # Query for EOD data
        eod_query = f"""
        SELECT 
            created_at,
            symbol,
            close_price,
            security_code,
            previous_close,
            ticker
        FROM public.eod_stock_data
        WHERE ticker = '{ticker}'
        AND close_price > 0
        AND close_price < 1000000
        AND created_at >= CURRENT_DATE - INTERVAL '{date_range} days'
        """
        
        # Execute queries
        stock_prices = conn.sql(stock_price_query).pl()
        eod_data = conn.sql(eod_query).pl()
        
        logger.info(f"Stock prices for {ticker}: {len(stock_prices)} rows")
        if not stock_prices.is_empty():
            logger.info(f"Sample stock price: {stock_prices.head(1).to_dicts()}")
            
        logger.info(f"EOD data for {ticker}: {len(eod_data)} rows")
        if not eod_data.is_empty():
            logger.info(f"Sample EOD data: {eod_data.head(1).to_dicts()}")
        
        # Convert both DataFrames to use datetime[μs] for created_at
        if not stock_prices.is_empty():
            stock_prices = stock_prices.with_columns(
                pl.col("created_at").cast(pl.Datetime("us")).alias("created_at")
            )
        
        if not eod_data.is_empty():
            eod_data = eod_data.with_columns(
                pl.col("created_at").cast(pl.Datetime("us")).alias("created_at")
            )
        
        # Combine and sort
        if not stock_prices.is_empty() and not eod_data.is_empty():
            # Ensure both DataFrames have the same schema
            stock_prices = stock_prices.select(required_columns)
            eod_data = eod_data.select(required_columns)
            
            # Now concatenate
            df = pl.concat([stock_prices, eod_data])
        elif not stock_prices.is_empty():
            df = stock_prices.select(required_columns)
        elif not eod_data.is_empty():
            df = eod_data.select(required_columns)
        else:
            logger.warning(f"No data found for {ticker}")
            return None
        
        # Remove duplicates based on symbol and timestamp
        df = df.unique(subset=["symbol", "created_at"], keep="last")
        
        # Sort by timestamp
        df = df.sort("created_at")
        
        logger.info(f"Final combined data for {ticker}: {len(df)} rows")
        if not df.is_empty():
            logger.info(f"Date range: {df['created_at'].min()} to {df['created_at'].max()}")
            logger.info(f"Price range: {df['close_price'].min()} to {df['close_price'].max()}")
        
        # Ensure we have enough data points
        if len(df) < 50:
            logger.warning(f"Insufficient data points for {ticker}: {len(df)} points")
            
        return df
    except Exception as e:
        logger.error(f"Error getting ticker data for {ticker}: {str(e)}", exc_info=True)
        return None


def format_rrg_data(data_df, metadata_df, index_symbol):
    """
    Format data for RRG analysis.
    Args:
        data_df: DataFrame containing price data
        metadata_df: DataFrame containing metadata
        index_symbol: The index symbol to use as benchmark
    """
    try:
        # Join with metadata using ticker
        df = data_df.join(metadata_df, on="ticker", how="left")
        
        # Format the data - simplified structure
        formatted_data = {
            "data": {
                "benchmark": index_symbol.lower(),
                "indexdata": [],
                "datalists": [],
                "change_data": []
            }
        }
        
        # Get the index data first
        index_data = df.filter(pl.col("ticker") == index_symbol)
        if not index_data.is_empty():
            # Sort by created_at
            index_data = index_data.sort("created_at")
            
            # Get unique dates and their last values
            index_data = index_data.with_columns(
                pl.col("created_at").dt.date().alias("date")
            ).groupby("date").agg(
                pl.col("close_price").last(),
                pl.col("created_at").last()
            ).sort("date")
            
            # Calculate index values
            index_values = []
            for row in index_data.iter_rows(named=True):
                try:
                    price = float(row["close_price"])
                    if price > 0:  # Only add positive prices
                        formatted_price = f"{price:.1f}"  # One decimal place as per example
                        index_values.append(formatted_price)
                except (ValueError, TypeError):
                    continue
            
            # Add all index values to indexdata without limitation
            formatted_data["data"]["indexdata"] = index_values
        
        # Get unique dates for change_data
        unique_dates = df.select(pl.col("created_at").dt.date().alias("date")).unique().sort("date")
        unique_dates = unique_dates["date"].to_list()
        
        # Process each ticker
        for ticker in df["ticker"].unique():
            ticker_data = df.filter(pl.col("ticker") == ticker)
            
            if ticker_data.is_empty():
                continue
            
            # Sort by created_at and limit to last 60 days
            ticker_data = ticker_data.sort("created_at")
            if len(ticker_data) > 60:
                ticker_data = ticker_data.tail(60)
            
            # Get metadata for this ticker
            ticker_metadata = metadata_df.filter(pl.col("ticker") == ticker)
            if not ticker_metadata.is_empty():
                name = ticker_metadata["name"][0]
                slug = ticker_metadata["slug"][0]
                security_code = ticker_metadata["security_code"][0]
            else:
                name = ticker
                slug = ticker.lower().replace(" ", "-")
                security_code = ""
            
            # Create datalist entry
            datalist = {
                "code": ticker,
                "name": name,
                "meaningful_name": ticker,
                "slug": slug,
                "ticker": ticker,
                "symbol": ticker,
                "security_code": security_code,
                "security_type_code": 26.0,
                "data": []
            }
            
            # Get benchmark prices for ratio calculation
            benchmark_prices = {}
            for row in index_data.iter_rows(named=True):
                date = row["created_at"].date()
                benchmark_prices[date] = float(row["close_price"])
            
            # Calculate price changes and momentum
            prices = []
            timestamps = []
            ratios = []
            momentum = []
            
            for row in ticker_data.iter_rows(named=True):
                try:
                    price = float(row["close_price"])
                    if price <= 0:  # Skip invalid prices
                        continue
                        
                    date = row["created_at"].date()
                    
                    # Calculate ratio using benchmark price
                    if date in benchmark_prices and benchmark_prices[date] > 0:
                        benchmark_price = benchmark_prices[date]
                        ratio = (price / benchmark_price) * 100
                    else:
                        ratio = 0.0
                    
                    prices.append(price)
                    timestamps.append(row["created_at"])
                    ratios.append(ratio)
                except (ValueError, TypeError):
                    continue
            
            if not prices:
                continue
            
            # Calculate momentum (5-day rate of change of ratio)
            for i in range(len(ratios)):
                if i < 5:  # Need at least 5 points for momentum
                    momentum.append(0.0)
                else:
                    # Calculate momentum as percentage change over 5 periods
                    if ratios[i-5] != 0:
                        momentum_val = ((ratios[i] - ratios[i-5]) / ratios[i-5]) * 100
                    else:
                        momentum_val = 0.0
                    momentum.append(momentum_val)
            
            # Add data points with calculated values - limit to last 60 points
            for i, (timestamp, price) in enumerate(zip(timestamps, prices)):
                try:
                    # Format timestamp to daily at 00:00:00
                    formatted_timestamp = timestamp.strftime("%Y-%m-%d 00:00:00")
                    
                    # Calculate additional metrics
                    prev_price = float(ticker_data["previous_close"][i]) if "previous_close" in ticker_data.columns and not ticker_data["previous_close"][i] is None else price
                    if prev_price <= 0:  # Skip invalid previous prices
                        continue
                        
                    price_change = price - prev_price
                    change_percentage = (price_change / prev_price) * 100 if prev_price != 0 else 0
                    
                    # Calculate scaled momentum and other metrics
                    scaled_momentum = momentum[i] * 0.5 if i < len(momentum) else 0.0
                    scaled_price = price * 0.5  # Scale factor to match example
                    
                    # Format values to match example response
                    data_point = [
                        formatted_timestamp,
                        f"{ratios[i]:.5f}" if i < len(ratios) else "0.00000",  # 5 decimal places as per example
                        f"{momentum[i]:.6f}" if i < len(momentum) else "0.000000",
                        f"{scaled_momentum:.6f}",
                        f"{price_change:.6f}",
                        f"{change_percentage:.6f}",
                        f"{scaled_price:.6f}",
                        f"{change_percentage:.6f}",
                        "0"  # Fixed value as per example
                    ]
                    datalist["data"].append(data_point)
                    
                    # Add to change_data only for the last data point of each date
                    current_date = timestamp.date()
                    if current_date in unique_dates:
                        # Only add if this is the last data point for this date
                        is_last_point_for_date = True
                        for j in range(i + 1, len(timestamps)):
                            if timestamps[j].date() == current_date:
                                is_last_point_for_date = False
                                break
                        
                        if is_last_point_for_date:
                            # Calculate period change percentage
                            if i >= 5:  # Need at least 5 days of data
                                period_change = ((price - prices[i-5]) / prices[i-5]) * 100
                            else:
                                period_change = 0.0
                                
                            change_data_point = {
                                "symbol": ticker,
                                "created_at": current_date.strftime("%Y-%m-%d"),
                                "change_percentage": period_change,
                                "close_price": price,
                                "momentum": f"{momentum[i]:.6f}" if i < len(momentum) else "0.000000",
                                "ratio": f"{ratios[i]:.6f}" if i < len(ratios) else "0.000000"
                            }
                            formatted_data["data"]["change_data"].append(change_data_point)
                    
                except Exception as e:
                    logger.error(f"Error processing data point for {ticker}: {str(e)}")
                    continue
            
            if datalist["data"]:  # Only add if we have data points
                formatted_data["data"]["datalists"].append(datalist)
        
        if not formatted_data["data"]["datalists"]:
            raise ValueError("No valid data points found for any ticker")
            
        # Sort change_data by date and symbol
        formatted_data["data"]["change_data"].sort(key=lambda x: (x["created_at"], x["symbol"]))
            
        return formatted_data
        
    except Exception as e:
        logger.error(f"Error formatting RRG data: {str(e)}", exc_info=True)
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
                                # Log raw data for debugging
                                logger.info(f"Raw data for {item.get('code', 'unknown')}:")
                                for point in item["data"][:5]:  # Log first 5 points
                                    logger.info(f"Data point: {point}")
                                
                                # Format each data point
                                formatted_data = []
                                for point in item["data"]:
                                    # Ensure we have the minimum required elements
                                    if len(point) < 3:  # We need at least datetime, ratio, and momentum
                                        logger.warning(f"Invalid data point format: {point}, skipping")
                                        continue
                                        
                                    # Format the data point
                                    formatted_point = [
                                        point[0],  # datetime
                                        f"{float(point[1]):.2f}" if point[1] else "0.00",  # ratio
                                        f"{float(point[2]):.2f}" if point[2] else "0.00",  # momentum
                                    ]
                                    
                                    # Add additional fields if they exist
                                    if len(point) > 3:
                                        formatted_point.extend(point[3:])
                                    
                                    formatted_data.append(formatted_point)
                                
                                # Log formatted data for debugging
                                logger.info(f"Formatted data for {item.get('code', 'unknown')}:")
                                for point in formatted_data[:5]:  # Log first 5 points
                                    logger.info(f"Formatted point: {point}")
                                
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
