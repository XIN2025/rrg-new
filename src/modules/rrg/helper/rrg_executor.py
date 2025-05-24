import os
import subprocess
import json
import logging
from typing import Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class RRGExecutor:
    def __init__(self):
        self.rrg_binary = "src/modules/rrg/exports/rrgcsv_new"
        
    def _merge_momentum(self, rrg_data: Dict[str, Any], change_data: Dict[str, Any], timeframe: str) -> Dict[str, Any]:
        """Merge momentum data with change data"""
        try:
            # Process each stock's data
            for stock in rrg_data["datalists"]:
                # Find matching change data
                change = next((c for c in change_data if c["symbol"] == stock["ticker"]), None)
                if change:
                    # Update data points with change percentage
                    for point in stock["data"]:
                        if point[0].startswith(change["created_at"]):
                            point.append(str(change["change_percentage"]))
                            point.append(str(change["close_price"]))
                            break
            
            return rrg_data
            
        except Exception as e:
            logger.error(f"Error merging momentum data: {str(e)}")
            return rrg_data
        
    def execute(self, input_file: str, output_file: str, timeframe: str = "daily") -> Dict[str, Any]:
        """
        Execute RRG binary with given input file and return processed output
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output JSON file
            timeframe: Data timeframe (daily, weekly, monthly, or minute-based)
            
        Returns:
            Dict containing processed RRG data
        """
        try:
            # Ensure binary exists
            if not os.path.exists(self.rrg_binary):
                raise FileNotFoundError(f"RRG binary not found at {self.rrg_binary}")
            
            # Execute RRG binary
            cmd = [
                self.rrg_binary,
                "-csvpath", input_file,
                "-outputpath", output_file
            ]
            
            logger.info(f"Executing RRG binary: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                raise RuntimeError(f"RRG binary failed: {result.stderr}")
            
            # Read and parse output file
            if not os.path.exists(output_file):
                raise FileNotFoundError(f"Output file not generated: {output_file}")
            
            with open(output_file, 'r') as f:
                rrg_output = json.load(f)
            
            # Process date formats based on timeframe
            if timeframe == "daily":
                for stock in rrg_output["datalists"]:
                    stock["data"] = [[d[0].replace("00", "").replace(":", "").replace(" ", "")] + d[1:] 
                                   for d in stock["data"]]
            
            logger.info(f"Successfully processed RRG data: {output_file}")
            return rrg_output
            
        except Exception as e:
            logger.error(f"Error executing RRG binary: {str(e)}")
            raise 
