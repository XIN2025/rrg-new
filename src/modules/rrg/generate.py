import polars as pl
import duckdb
import time as t
from datetime import datetime, timedelta, timezone
import os
from src.utils.logger import get_logger
from src.modules.db.minute_data_loader import get_duckdb_connection
from src.modules.rrg.metadata_store import RRGMetadataStore
from src.modules.rrg.init import init_database, verify_database
import pandas as pd
import numpy as np
import json
from typing import List, Tuple, Dict, Any, Optional

logger = get_logger(__name__)

def get_ticker_data(tickers: List[str], date_range: str, index_symbol: str = "CNX500") -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Get ticker data from DuckDB."""
    try:
        if not tickers:
            raise ValueError("No tickers provided")
            
        if not date_range or not date_range.isdigit():
            raise ValueError("Invalid date range")
        
        # Initialize database if needed
        if not init_database():
            raise ValueError("Failed to initialize database")
            
        # Verify database setup
        if not verify_database():
            raise ValueError("Database verification failed")
            
        with get_duckdb_connection() as conn:
            if not conn:
                raise ValueError("Failed to establish DuckDB connection")
            
            # Get metadata
            metadata_df = RRGMetadataStore().get_market_metadata(tickers)
            if metadata_df is None or metadata_df.is_empty():
                raise ValueError("Failed to get metadata for tickers")
            
            # Get price data with proper filtering
            query = f"""
                WITH filtered_data AS (
                    SELECT 
                        ticker as symbol,
                        timestamp,
                        close_price,
                        security_code,
                        ROW_NUMBER() OVER (PARTITION BY ticker, DATE_TRUNC('day', timestamp) ORDER BY timestamp DESC) as rn
                    FROM public.hourly_stock_data
                    WHERE ticker IN ({','.join([f"'{t}'" for t in tickers])})
                    AND timestamp >= CURRENT_DATE - INTERVAL '{date_range}' DAY
                    AND close_price > 0
                )
                SELECT 
                    symbol,
                    timestamp,
                    close_price,
                    security_code
                FROM filtered_data
                WHERE rn = 1
                ORDER BY timestamp
            """
            
            result = conn.execute(query).fetchdf()
            
            if result.empty:
                logger.warning(f"No data found for tickers: {tickers}")
                return None, metadata_df
            
            # Convert to pandas DataFrame and ensure proper data types
            combined_df = pd.DataFrame(result)
            combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
            combined_df['close_price'] = pd.to_numeric(combined_df['close_price'], errors='coerce')
            
            # Verify data quality
            if combined_df['close_price'].isnull().any():
                logger.warning("Found null values in close_price")
            
            if combined_df['timestamp'].isnull().any():
                logger.warning("Found null values in timestamp")
            
            # Verify we have data for all tickers
            missing_tickers = set(tickers) - set(combined_df['symbol'].unique())
            if missing_tickers:
                logger.warning(f"No data found for tickers: {missing_tickers}")
            
            return combined_df, metadata_df
            
    except Exception as e:
        logger.error(f"Error in get_ticker_data: {str(e)}", exc_info=True)
        return None, None

def format_rrg_data(data: pd.DataFrame, metadata_df: pd.DataFrame, index_symbol: str) -> Dict[str, Any]:
    """Format data for RRG analysis."""
    try:
        # Get unique dates and sort them
        dates = pd.to_datetime(data["timestamp"].unique())
        dates = np.sort(dates)
        
        # Get index data
        index_data = data[data["symbol"] == index_symbol].copy()
        index_data.set_index("timestamp", inplace=True)
        
        # Format data
        formatted_data = {
            "data": {
                "benchmark": index_symbol.lower(),
                "indexdata": [],
                "datalists": []
            },
            "change_data": []
        }
        
        # Add index prices
        for date in dates:
            if date in index_data.index:
                price = float(index_data.loc[date, "close_price"])
                formatted_data["data"]["indexdata"].append(str(price))
            else:
                formatted_data["data"]["indexdata"].append("")
        
        # Process each ticker
        for ticker in data["symbol"].unique():
            if ticker == index_symbol:
                continue
                
            stock_data = data[data["symbol"] == ticker].copy()
            stock_data.set_index("timestamp", inplace=True)
            
            # Get metadata for the ticker
            ticker_metadata = metadata_df.filter(pl.col("ticker") == ticker)
            
            # Create ticker data object
            ticker_obj = {
                "code": ticker,
                "name": ticker,
                "meaningful_name": ticker,
                "slug": ticker.lower().replace(" ", "-"),
                "ticker": ticker,
                "symbol": ticker,
                "security_code": "",
                "security_type_code": 26.0,
                "data": []
            }
            
            # Add metadata if available
            if not ticker_metadata.is_empty():
                ticker_obj.update({
                    "name": ticker_metadata["name"][0],
                    "slug": ticker_metadata["slug"][0],
                    "security_code": ticker_metadata["security_code"][0],
                    "security_type_code": float(ticker_metadata["security_type_code"][0])
                })
            
            # Calculate metrics for each date
            for i, date in enumerate(dates):
                if date in stock_data.index:
                    price = float(stock_data.loc[date, "close_price"])
                    
                    # Calculate momentum (using 14-day period)
                    momentum = 100.0
                    if i >= 14:
                        prev_dates = dates[i-14:i]
                        prev_prices = [float(stock_data.loc[d, "close_price"]) for d in prev_dates if d in stock_data.index]
                        if prev_prices:
                            momentum = (price / prev_prices[0]) * 100
                    
                    # Calculate ratio (relative strength vs benchmark)
                    ratio = 100.0
                    if date in index_data.index:
                        benchmark_price = float(index_data.loc[date, "close_price"])
                        ratio = (price / benchmark_price) * 100
                    
                    # Calculate change percentage
                    change_pct = 0.0
                    if i > 0:
                        prev_date = dates[i-1]
                        if prev_date in stock_data.index:
                            prev_price = float(stock_data.loc[prev_date, "close_price"])
                            change_pct = ((price - prev_price) / prev_price) * 100
                    
                    # Calculate additional metrics
                    # Metric 5: Volatility (using 14-day standard deviation)
                    volatility = 0.0
                    if i >= 14:
                        prev_dates = dates[i-14:i]
                        prev_prices = [float(stock_data.loc[d, "close_price"]) for d in prev_dates if d in stock_data.index]
                        if len(prev_prices) > 1:
                            volatility = np.std(prev_prices) / np.mean(prev_prices) * 100
                    
                    # Metric 6: Volume (placeholder)
                    volume = price * 1000  # Placeholder calculation
                    
                    # Metric 7: RSI (14-day)
                    rsi = 50.0
                    if i >= 14:
                        prev_dates = dates[i-14:i]
                        prev_prices = [float(stock_data.loc[d, "close_price"]) for d in prev_dates if d in stock_data.index]
                        if len(prev_prices) > 1:
                            price_changes = np.diff(prev_prices)
                            gains = np.where(price_changes > 0, price_changes, 0)
                            losses = np.where(price_changes < 0, -price_changes, 0)
                            avg_gain = np.mean(gains)
                            avg_loss = np.mean(losses)
                            if avg_loss > 0:
                                rs = avg_gain / avg_loss
                                rsi = 100 - (100 / (1 + rs))
                    
                    # Calculate signal (0, 1, 2, or 3)
                    if momentum > 100 and ratio > 100:
                        signal = 2  # Strong buy
                    elif momentum > 100 or ratio > 100:
                        signal = 1  # Buy
                    elif momentum < 100 and ratio < 100:
                        signal = 3  # Strong sell
                    else:
                        signal = 0  # Sell
                    
                    # Add daily data with all 9 metrics
                    ticker_obj["data"].append([
                        date.strftime("%Y-%m-%d %H:%M:%S"),
                        str(ratio),
                        str(momentum),
                        str(price),
                        str(change_pct),
                        str(volatility),
                        str(volume),
                        str(rsi),
                        str(signal)
                    ])
                    
                    # Add to change_data for first date
                    if len(formatted_data["change_data"]) < len(data["symbol"].unique()):
                        formatted_data["change_data"].append({
                            "symbol": ticker,
                            "created_at": date.strftime("%Y-%m-%d"),
                            "change_percentage": change_pct,
                            "close_price": price,
                            "momentum": str(momentum),
                            "ratio": str(ratio)
                        })
            
            formatted_data["data"]["datalists"].append(ticker_obj)
        
        return formatted_data
        
    except Exception as e:
        logger.error(f"Error formatting RRG data: {str(e)}", exc_info=True)
        raise

def test_data_loading(tickers: List[str], date_range: str, index_symbol: str = "CNX500") -> Dict[str, Any]:
    """Test function to verify data loading and processing."""
    try:
        logger.info("Starting data loading test...")
        
        # Test 1: Get ticker data
        logger.info("Test 1: Getting ticker data...")
        data, metadata_df = get_ticker_data(tickers, date_range, index_symbol)
        if data is None or data.empty:
            raise ValueError("No data returned from get_ticker_data")
        logger.info(f"Successfully loaded data for {len(data['symbol'].unique())} tickers")
        
        # Test 2: Verify data structure
        logger.info("Test 2: Verifying data structure...")
        required_columns = ['symbol', 'timestamp', 'close_price', 'security_code']
        for col in required_columns:
            if col not in data.columns:
                raise ValueError(f"Missing required column: {col}")
        
        # Test 3: Check for null values
        logger.info("Test 3: Checking for null values...")
        null_counts = data.isnull().sum()
        if null_counts.any():
            logger.warning(f"Found null values: {null_counts[null_counts > 0]}")
        
        # Test 4: Verify date range
        logger.info("Test 4: Verifying date range...")
        min_date = data['timestamp'].min()
        max_date = data['timestamp'].max()
        logger.info(f"Data range: {min_date} to {max_date}")
        
        # Test 5: Format data
        logger.info("Test 5: Testing data formatting...")
        formatted_data = format_rrg_data(data, metadata_df, index_symbol)
        
        # Test 6: Verify formatted data structure
        logger.info("Test 6: Verifying formatted data structure...")
        if 'data' not in formatted_data:
            raise ValueError("Missing 'data' in formatted response")
        if 'benchmark' not in formatted_data['data']:
            raise ValueError("Missing 'benchmark' in formatted data")
        if 'indexdata' not in formatted_data['data']:
            raise ValueError("Missing 'indexdata' in formatted data")
        if 'datalists' not in formatted_data['data']:
            raise ValueError("Missing 'datalists' in formatted data")
        
        # Test 7: Verify metrics calculations
        logger.info("Test 7: Verifying metrics calculations...")
        for ticker_data in formatted_data['data']['datalists']:
            if not ticker_data['data']:
                logger.warning(f"No data for ticker: {ticker_data['symbol']}")
                continue
            
            # Check first data point
            first_point = ticker_data['data'][0]
            if len(first_point) != 9:
                raise ValueError(f"Invalid number of metrics for {ticker_data['symbol']}: {len(first_point)}")
            
            # Verify all metrics are strings
            for metric in first_point:
                if not isinstance(metric, str):
                    raise ValueError(f"Non-string metric found for {ticker_data['symbol']}: {metric}")
        
        # Test 8: Verify change data
        logger.info("Test 8: Verifying change data...")
        if 'change_data' not in formatted_data:
            raise ValueError("Missing 'change_data' in formatted response")
        
        for change in formatted_data['change_data']:
            required_fields = ['symbol', 'created_at', 'change_percentage', 'close_price', 'momentum', 'ratio']
            for field in required_fields:
                if field not in change:
                    raise ValueError(f"Missing field in change data: {field}")
        
        logger.info("All tests passed successfully!")
        return {
            "status": "success",
            "data": formatted_data,
            "test_results": {
                "ticker_count": len(data['symbol'].unique()),
                "date_range": {
                    "start": min_date.isoformat(),
                    "end": max_date.isoformat()
                },
                "null_values": null_counts.to_dict(),
                "formatted_data_size": len(str(formatted_data))
            }
        }
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }

def generate_csv(tickers: List[str], date_range: str, index_symbol: str, timeframe: str, channel_name: str, filename: str, cache_manager: Any) -> Dict[str, Any]:
    """Generate CSV file for RRG analysis."""
    try:
        # Run tests first
        test_results = test_data_loading(tickers, date_range, index_symbol)
        if test_results["status"] == "error":
            raise ValueError(f"Data loading test failed: {test_results['error']}")
        
        # Get metadata for all tickers
        metadata_df = RRGMetadataStore().get_market_metadata(tickers + [index_symbol])
        
        # Use the validated date_range directly (it's already in days)
        days = int(date_range)
        
        # Process data for each ticker including index
        processed_data = []
        for ticker in tickers + [index_symbol]:
            try:
                # Get data for ticker
                ticker_data, ticker_metadata = get_ticker_data([ticker], str(days), index_symbol)
                if ticker_data is not None and not ticker_data.empty:
                    processed_data.append(ticker_data)
            except Exception as e:
                logger.error(f"Error processing ticker {ticker}: {str(e)}")
                continue
        
        if not processed_data:
            raise ValueError("No valid data found for any ticker")
        
        # Combine all data
        combined_data = pd.concat(processed_data)
        
        # Format the data
        formatted_data = format_rrg_data(combined_data, metadata_df, index_symbol)
        
        # Create output directory if it doesn't exist
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exports", "output")
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the formatted data
        output_file = os.path.join(output_dir, f"{filename}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(formatted_data, f, indent=2)
        
        # Create input CSV file for RRG binary
        input_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exports", "input")
        os.makedirs(input_dir, exist_ok=True)
        
        # Create CSV with proper format
        csv_data = []
        
        # Row 1: Index symbol and empty cells
        csv_data.append([index_symbol] + [""] * (len(tickers)))
        
        # Row 2: Column headers
        csv_data.append(["Date"] + tickers)
        
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
        dates = pd.to_datetime(combined_data["timestamp"].unique())
        dates = np.sort(dates)
        
        # Get index data first
        index_data = combined_data[combined_data["symbol"] == index_symbol].copy()
        index_data.set_index("timestamp", inplace=True)
        
        # Data rows: Dates and prices
        for date in dates:
            row = [date.strftime("%Y-%m-%d")]
            
            # Add index price first
            if date in index_data.index:
                row.append(str(index_data.loc[date, "close_price"]))
            else:
                row.append("")
            
            # Add other ticker prices
            for ticker in tickers:
                ticker_data = combined_data[
                    (combined_data["symbol"] == ticker) & 
                    (combined_data["timestamp"] == date)
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
                f.write(",".join(row) + "\n")
        
        # Log the first few lines of the generated CSV for debugging
        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                lines = [next(f) for _ in range(5)]
            logger.info(f"First 5 lines of generated CSV {csv_path}:\n{''.join(lines)}")
        except Exception as e:
            logger.warning(f"Could not read first lines of CSV {csv_path}: {e}")
        
        # Add cache hit status and error field to match original response
        response = {
            "data": formatted_data["data"],
            "change_data": formatted_data["change_data"],
            "filename": filename,
            "error": None,
            "cacheHit": False
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Error generating CSV: {str(e)}")
        raise

def test_csv_generation(tickers: List[str], date_range: str, index_symbol: str = "CNX500") -> Dict[str, Any]:
    """Test function to verify CSV generation process."""
    try:
        logger.info("Starting CSV generation test...")
        
        # Test 1: Get ticker data
        logger.info("Test 1: Getting ticker data...")
        data, metadata_df = get_ticker_data(tickers, date_range, index_symbol)
        if data is None or data.empty:
            raise ValueError("No data retrieved from DuckDB")
        logger.info(f"Retrieved {len(data)} records")
        
        # Test 2: Format data
        logger.info("Test 2: Formatting data...")
        formatted_data = format_rrg_data(data, metadata_df, index_symbol)
        if not formatted_data or "data" not in formatted_data:
            raise ValueError("Failed to format data")
        logger.info("Data formatted successfully")
        
        # Test 3: Generate CSV
        logger.info("Test 3: Generating CSV...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        tickers_hash = hash("_".join(tickers)) % 10000
        filename = f"test_rrg_{timestamp}_{tickers_hash}"
        
        # Create output directories
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exports", "output")
        input_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exports", "input")
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(input_dir, exist_ok=True)
        
        # Save JSON output
        output_file = os.path.join(output_dir, f"{filename}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(formatted_data, f, indent=2)
        logger.info(f"JSON output saved to {output_file}")
        
        # Create CSV with proper format
        csv_data = []
        
        # Row 1: Index symbol and empty cells
        csv_data.append([index_symbol] + [""] * (len(tickers)))
        
        # Row 2: Column headers
        csv_data.append(["Date"] + tickers)
        
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
        dates = pd.to_datetime(data["timestamp"].unique())
        dates = np.sort(dates)
        
        # Get index data first
        index_data = data[data["symbol"] == index_symbol].copy()
        index_data.set_index("timestamp", inplace=True)
        
        # Data rows: Dates and prices
        for date in dates:
            row = [date.strftime("%Y-%m-%d")]
            
            # Add index price first
            if date in index_data.index:
                row.append(str(index_data.loc[date, "close_price"]))
            else:
                row.append("")
            
            # Add other ticker prices
            for ticker in tickers:
                ticker_data = data[
                    (data["symbol"] == ticker) & 
                    (data["timestamp"] == date)
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
                f.write(",".join(row) + "\n")
        
        # Verify CSV file
        if not os.path.exists(csv_path):
            raise ValueError(f"CSV file not created at {csv_path}")
            
        if os.path.getsize(csv_path) == 0:
            raise ValueError(f"Generated CSV file is empty at {csv_path}")
        
        # Verify CSV content
        with open(csv_path, 'r') as f:
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
            logger.info(f"Expected columns: {expected_columns}")
            
            # Verify each line has the correct number of columns
            for i, line in enumerate(lines[1:], 1):
                columns = line.split(',')
                if len(columns) != expected_columns:
                    logger.error(f"Line {i} has {len(columns)} columns, expected {expected_columns}")
                    logger.error(f"Line content: {line}")
                    raise ValueError(f"CSV file has inconsistent number of columns at line {i}")
            
            # Verify no line ends with a comma
            for i, line in enumerate(lines[1:], 1):
                if line.endswith(','):
                    logger.error(f"Line {i} ends with a comma: {line}")
                    raise ValueError(f"CSV file has trailing comma at line {i}")
        
        logger.info(f"CSV file created successfully at {csv_path}")
        logger.info("CSV generation test completed successfully")
        
        return {
            "status": "success",
            "message": "CSV generation test passed",
            "files": {
                "json": output_file,
                "csv": csv_path
            }
        }
        
    except Exception as e:
        logger.error(f"CSV generation test failed: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }

if __name__ == "__main__":
    # Test with sample data
    test_tickers = ["RELIANCE", "TCS", "HDFCBANK"]
    test_date_range = "7"  # 7 days
    test_index = "CNX500"
    
    result = test_csv_generation(test_tickers, test_date_range, test_index)
    print(f"Test result: {result}")


