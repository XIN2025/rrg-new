import polars as pl
import duckdb
import time as t
from datetime import datetime, timedelta, timezone
import os
from src.utils.logger import get_logger
from src.modules.db.minute_data_loader import get_duckdb_connection
from src.modules.rrg.metadata_store import RRGMetadataStore
import pandas as pd
import numpy as np
import json
from typing import List, Tuple, Dict, Any, Optional

logger = get_logger(__name__)

def get_ticker_data(tickers: List[str], date_range: str, index_symbol: str = "CNX500") -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Get ticker data from DuckDB."""
    try:
        with get_duckdb_connection() as conn:
            # Get metadata
            metadata_df = RRGMetadataStore().get_market_metadata(tickers)
            
            # Get price data with proper filtering
            query = f"""
                WITH filtered_data AS (
                    SELECT 
                        ticker as symbol,
                        timestamp,
                        close_price,
                        security_code,
                        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY timestamp DESC) as rn
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
                WHERE rn <= 1
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
            
            return combined_df, metadata_df
            
    except Exception as e:
        logger.error(f"Error in get_ticker_data: {str(e)}")
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
            "dates": [d.strftime("%Y-%m-%d") for d in dates],
            "index": {
                "symbol": index_symbol,
                "prices": []
            },
            "stocks": {}
        }
        
        # Add index prices
        for date in dates:
            if date in index_data.index:
                price = float(index_data.loc[date, "close_price"])
                formatted_data["index"]["prices"].append(price)
            else:
                formatted_data["index"]["prices"].append(None)
        
        # Add stock prices
        for ticker in data["symbol"].unique():
            if ticker == index_symbol:
                continue
                
            stock_data = data[data["symbol"] == ticker].copy()
            stock_data.set_index("timestamp", inplace=True)
            
            formatted_data["stocks"][ticker] = {
                "symbol": ticker,
                "prices": []
            }
            
            for date in dates:
                if date in stock_data.index:
                    price = float(stock_data.loc[date, "close_price"])
                    formatted_data["stocks"][ticker]["prices"].append(price)
                else:
                    formatted_data["stocks"][ticker]["prices"].append(None)
        
        return formatted_data
        
    except Exception as e:
        logger.error(f"Error formatting RRG data: {str(e)}", exc_info=True)
        raise

def generate_csv(tickers: List[str], date_range: str, index_symbol: str, timeframe: str, channel_name: str, filename: str, cache_manager: Any) -> Dict[str, Any]:
    """Generate CSV file for RRG analysis."""
    try:
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
        
        return {
            "data": formatted_data,
            "filename": filename
        }
    except Exception as e:
        logger.error(f"Error generating CSV: {str(e)}")
        raise


