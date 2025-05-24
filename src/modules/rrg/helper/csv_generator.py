import os
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)

class CSVCenerator:
    def __init__(self, benchmark_ticker: str):
        self.benchmark_ticker = benchmark_ticker
        # Use absolute paths
        self.input_folder = os.path.abspath("src/modules/rrg/exports/input")
        self.output_folder = os.path.abspath("src/modules/rrg/exports/output")
        
    def _create_folders(self):
        """Create input and output folders if they don't exist"""
        os.makedirs(self.input_folder, exist_ok=True)
        os.makedirs(self.output_folder, exist_ok=True)
        
    def _generate_filename(self) -> str:
        """Generate a unique filename based on timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"rrg_data_{self.benchmark_ticker}_{timestamp}"
        
    def _aggregate_data(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """Aggregate data based on timeframe"""
        if timeframe == "daily":
            return df
        elif timeframe == "weekly":
            df['week'] = pd.to_datetime(df['date']).dt.isocalendar().week
            df['year'] = pd.to_datetime(df['date']).dt.isocalendar().year
            return df.groupby(['year', 'week', 'symbol'])['price'].last().reset_index()
        elif timeframe == "monthly":
            df['month'] = pd.to_datetime(df['date']).dt.month
            df['year'] = pd.to_datetime(df['date']).dt.year
            return df.groupby(['year', 'month', 'symbol'])['price'].last().reset_index()
        else:  # minute-based timeframes
            df['time'] = pd.to_datetime(df['date'])
            df = df[df['time'].dt.time.between(datetime.strptime('09:15', '%H:%M').time(),
                                             datetime.strptime('15:30', '%H:%M').time())]
            return df.groupby(pd.Grouper(key='time', freq=timeframe))['price'].last().reset_index()
        
    def generate_csv(self, rrg_data: Dict[str, Any], timeframe: str = "daily") -> tuple[str, str]:
        """
        Generate CSV file in the format required by RRG binary
        
        Args:
            rrg_data: Dictionary containing RRG data with datalists
            timeframe: Data timeframe (daily, weekly, monthly, or minute-based)
            
        Returns:
            tuple: (input_file_path, output_file_path)
        """
        try:
            self._create_folders()
            filename = self._generate_filename()
            input_file = os.path.join(self.input_folder, f"{filename}.csv")
            output_file = os.path.join(self.output_folder, f"{filename}.json")
            
            # Extract and transform data
            data_rows = []
            metadata_rows = []
            
            # Add benchmark header
            data_rows.append([self.benchmark_ticker])
            
            # Process each stock's data
            for stock in rrg_data["datalists"]:
                # Add metadata rows with URL
                url = f"https://www.moneycontrol.com/india/stockpricequote/{stock['slug']}"
                metadata_rows.append([
                    stock["symbol"],
                    stock["name"],
                    url,
                    stock["slug"]
                ])
                
                # Transform price data
                for point in stock["data"]:
                    date = point[0]  # created_at
                    price = point[3]  # close_price
                    data_rows.append([date, stock["symbol"], price])
            
            # Convert to DataFrame and pivot
            df = pd.DataFrame(data_rows[1:], columns=["date", "symbol", "price"])
            
            if not df.empty:
                # Deduplicate: keep only the last price for each (date, symbol)
                df = df.sort_values(["date"]).drop_duplicates(["date", "symbol"], keep="last")
                # Aggregate data based on timeframe
                df = self._aggregate_data(df, timeframe)
                # Pivot the data
                pivoted_df = df.pivot(index="date", columns="symbol", values="price")
            else:
                pivoted_df = pd.DataFrame()
            
            # Write to CSV
            with open(input_file, 'w', newline='') as f:
                # Write benchmark header
                f.write(f"{self.benchmark_ticker}\n")
                
                # Write metadata
                for meta in metadata_rows:
                    f.write("\t".join(meta) + "\n")
                
                # Write price data
                pivoted_df.to_csv(f, sep='\t', mode='a')
            
            logger.info(f"Generated CSV file: {input_file}")
            return input_file, output_file
            
        except Exception as e:
            logger.error(f"Error generating CSV: {str(e)}")
            raise 
