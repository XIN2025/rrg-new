import polars as pl
import pandas as pd
from datetime import datetime
import os
import time
from src.utils.logger import get_logger

# Get logger for this module
logger = get_logger("csv_generation")


def attach_url_and_standardized_name(df: pl.DataFrame):
    """Add URL and standardized names to the DataFrame."""
    df = df.with_columns([
        # Add URL column
        pl.when(pl.col("slug") == "national-stock-exchange-of-india-ltd.")
        .then("https://strike-analytics-dev.netlify.app/stocks/" + pl.col("symbol"))
        .otherwise("https://strike-analytics-dev.netlify.app/stocks/" + pl.col("slug"))
        .alias("URL"),

        # Add standardized name
        pl.when(pl.col("name").str.contains("National Stock Exchange of India Ltd."))
        .then(pl.col("symbol").str.to_lowercase().str.replace(" ", "_"))
        .otherwise(pl.col("name"))
        .alias("Name")
    ])
    return df


def build_header_df(metadata_df: pl.DataFrame, ticker):
    """Build header row with benchmark index"""
    start_time = time.time()
    logger.debug(f"Building header DataFrame for ticker: {ticker}")
    
    # Create header row with benchmark index
    header_row = {
        "header_column1": ticker,
        "header_created_at": "",
        "header_symbol": ticker,
        "header_close_price": "",
        "header_security_code": "",
        "header_previous_close": "",
        "header_ticker": ticker,
        "header_name": ticker,
        "header_slug": ticker.lower().replace(" ", "-"),
        "header_index_name": ticker,
        "header_indices_slug": ticker.lower().replace(" ", "-"),
        "header_symbol_right": ticker
    }
    
    logger.debug(f"Header row data: {header_row}")
    
    # Create DataFrame with explicit schema
    schema = {col: pl.Utf8 for col in header_row.keys()}
    result = pl.DataFrame([header_row], schema=schema)
    
    logger.debug(f"Header DataFrame columns: {result.columns}")
    logger.debug(f"Header DataFrame sample: {result.head(1).to_dicts()}")
    
    logger.info(f"Completed header DataFrame build in {time.time() - start_time:.2f}s")
    return result


def transform_to_metadata_df(df: pl.DataFrame, ticker):
    """Transform DataFrame to metadata format."""
    metadata_columns = ["symbol", "Name", "URL"]
    metadata_df = df[metadata_columns]
    metadata_df = metadata_df.unique(subset=metadata_columns, keep='first').sort(by='symbol')
    metadata_df = metadata_df.rename({"symbol": "Ticker"})

    metadata_df = metadata_df.transpose(include_header=True, column_names=metadata_df["Ticker"])
    
    metadata_df_columns = metadata_df.columns
    columns_without_benchindex = [column for column in metadata_df_columns if (column != ticker and column != 'column')]
    metadata_df = metadata_df.select(["column"] + [ticker] + sorted(columns_without_benchindex))
    metadata_df = metadata_df.rename({"column": "column1"})

    return metadata_df


def transform_to_marketdata_df(df: pl.DataFrame, ticker):
    """Transform DataFrame to market data format with proper validation."""
    if df.is_empty():
        logger.error(f"No data available for ticker: {ticker}")
        return pl.DataFrame()

    # Validate required columns
    required_columns = ["symbol", "close_price", "created_at"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return pl.DataFrame()

    # Validate ticker exists
    valid_symbols = df['symbol'].unique().to_list()
    if ticker not in valid_symbols:
        logger.error(f"Ticker {ticker} not found in symbol list: {valid_symbols}")
        return pl.DataFrame()

    # Transform to market data format
    marketdata_df = df[required_columns]
    marketdata_df = marketdata_df.pivot(
        values="close_price",
        index="created_at",
        columns="symbol",
        aggregate_function="first",
    )

    # Sort columns with ticker first
    marketdata_df_columns = marketdata_df.columns
    columns_without_benchindex = [column for column in marketdata_df_columns if (column != ticker and column != 'created_at')]
    marketdata_df = marketdata_df.select(["created_at"] + [ticker] + sorted(columns_without_benchindex))
    marketdata_df = marketdata_df.rename({"created_at": "column1"})

    return marketdata_df


def backfill_marketdata_df(df: pl.DataFrame):
    """Backfill missing values in market data."""
    # Forward fill missing values
    df = df.fill_null(strategy="forward")
    
    # For any remaining nulls at the start, use the first non-null value
    for col in df.columns:
        if col != "column1":  # Skip the date column
            first_valid = df.select(pl.col(col)).drop_nulls().head(1)
            if not first_valid.is_empty():
                first_value = first_valid.item()
                df = df.with_columns(pl.col(col).fill_null(first_value))
    
    return df


def write_csv(df, rrg_csv_filepath, input_folder_path):
    """Write the DataFrame to a CSV file"""
    try:
        # Create the full file path
        full_path = os.path.join(input_folder_path, rrg_csv_filepath)
        logger.debug(f"Writing CSV to: {full_path}")
        logger.debug(f"DataFrame shape: {df.shape}")
        logger.debug(f"DataFrame columns: {df.columns}")
        logger.debug(f"DataFrame sample: {df.head(3).to_dicts()}")
        
        # Write CSV with correct Polars API
        df.write_csv(
            full_path,
            separator=",",
            quote='"',
            has_header=False,
            null_value="",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )
        logger.debug(f"CSV file written successfully")
        
        return full_path
    except Exception as e:
        logger.error(f"Error writing CSV file: {str(e)}", exc_info=True)
        raise


def generate_csv(df: pl.DataFrame, ticker, input_file_name, input_folder_path):
    print(f"[RRG][CSV GENERATOR] Initiate Generating Csv File.")
    
    try:
        # Get benchmark name from the input DataFrame
        benchmark_rows = df.filter(pl.col("symbol") == ticker)
        if len(benchmark_rows) == 0:
            raise ValueError(f"Benchmark symbol {ticker} not found in data")
        benchmark_name = benchmark_rows["name"].unique()[0]
        
        # Get all unique symbols and sort them with benchmark first
        all_symbols = df["symbol"].unique().sort()
        benchmark_first = [ticker] + [s for s in all_symbols if s != ticker]
        
        # Create header row with benchmark in first column
        header_row = {"column1": "Date"}
        for sym in benchmark_first:
            # Keep spaces in header as is
            header_row[sym] = sym
        header_df = pl.DataFrame([header_row])
        
        # Create market data rows
        marketdata_df = df.pivot(
            values="close_price",
            index="created_at",
            columns="symbol",
            aggregate_function="first"
        ).sort("created_at")
        
        # Rename created_at to column1
        marketdata_df = marketdata_df.rename({"created_at": "column1"})
        
        # Ensure columns are in correct order: column1, benchmark, other symbols
        market_cols = ["column1"] + benchmark_first
        marketdata_df = marketdata_df.select(market_cols)
        
        # Forward fill any missing values
        marketdata_df = marketdata_df.fill_null(strategy="forward")
        
        # Format dates to match expected format
        marketdata_df = marketdata_df.with_columns(
            pl.col("column1").dt.strftime("%Y-%m-%d %H:%M:%S").alias("column1")
        )
        
        # Combine all dataframes
        rrg_csv_df = pl.concat([header_df, marketdata_df], how="vertical_relaxed")
        
        # Create input directory if it doesn't exist
        os.makedirs(input_folder_path, exist_ok=True, mode=0o755)
        
        # Write to CSV with comma separation
        rrg_csv_filepath = os.path.join(input_folder_path, f"{input_file_name}.csv")
        
        # Write to CSV without header and with proper formatting
        rrg_csv_df.write_csv(
            rrg_csv_filepath,
            separator=",",
            quote='"',
            has_header=False,
            null_value="",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )
        
        # Verify the file was created and has content
        if not os.path.exists(rrg_csv_filepath):
            raise ValueError(f"Failed to create CSV file at {rrg_csv_filepath}")
            
        if os.path.getsize(rrg_csv_filepath) == 0:
            raise ValueError(f"Generated CSV file is empty at {rrg_csv_filepath}")
            
        # Set proper permissions
        os.chmod(rrg_csv_filepath, 0o644)
        
        # Verify CSV content
        with open(rrg_csv_filepath, 'r') as f:
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
        
        print(f"[RRG][CSV GENERATOR] Created csv file: {rrg_csv_filepath}")
        print(f"[DEBUG] CSV file contents preview:")
        with open(rrg_csv_filepath, 'r') as f:
            print(f.read()[:500])  # Print first 500 chars for debugging
            
        return rrg_csv_filepath
        
    except Exception as e:
        logger.error(f"Error generating CSV file: {str(e)}", exc_info=True)
        raise
