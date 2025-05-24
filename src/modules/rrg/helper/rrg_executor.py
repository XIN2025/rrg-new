import os
import subprocess
import json
import logging
import shutil
from typing import Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

def ensure_clean_output_path(output_file):
    """Ensure the output path is clean and ready for writing"""
    # Get the base name without extension
    base_name = os.path.splitext(os.path.basename(output_file))[0]

    # Always anchor to the correct exports/input and exports/output directories
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'exports'))
    input_dir = os.path.join(base_dir, 'input', base_name)
    output_dir = os.path.join(base_dir, 'output', base_name)

    # Create directories
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    # Return the new paths - use the same base name for both files
    input_path = os.path.join(input_dir, f"{base_name}.csv")
    output_path = os.path.join(output_dir, f"{base_name}.json")

    # Remove if output is a directory
    if os.path.isdir(output_path):
        logger.warning(f"Removing directory at: {output_path}")
        shutil.rmtree(output_path)

    # Remove if output is a file
    if os.path.isfile(output_path):
        logger.warning(f"Removing existing file at: {output_path}")
        os.remove(output_path)

    # Create an empty file to ensure the path is a file, not a directory
    with open(output_path, 'w') as f:
        pass

    return input_path, output_path

class RRGExecutor:
    def __init__(self):
        # Always resolve to src/modules/rrg/exports/rrgcsv_new
        self.rrg_binary = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "exports", "rrgcsv_new"))
        
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
            
            # Get new input and output paths with proper directory structure
            new_input_file, new_output_file = ensure_clean_output_path(output_file)
            
            # Ensure the input file exists
            if not os.path.exists(input_file):
                logger.error(f"Input CSV file does not exist: {input_file}")
                raise FileNotFoundError(f"Input CSV file does not exist: {input_file}")
            
            # Only copy if the input file is not already in the correct location
            if os.path.abspath(input_file) != os.path.abspath(new_input_file):
                os.makedirs(os.path.dirname(new_input_file), exist_ok=True)
                shutil.copy2(input_file, new_input_file)
            
            # Log input and output file paths
            logger.info(f"Input CSV path: {new_input_file}")
            logger.info(f"Output JSON path: {new_output_file}")

            # Check if input file exists and log details
            if not os.path.exists(new_input_file):
                logger.error(f"Input CSV file does not exist: {new_input_file}")
                raise FileNotFoundError(f"Input CSV file does not exist: {new_input_file}")
            else:
                file_size = os.path.getsize(new_input_file)
                logger.info(f"Input CSV file size: {file_size} bytes")
                try:
                    with open(new_input_file, 'r', encoding='utf-8') as f:
                        lines = [next(f) for _ in range(10)]
                    logger.info(f"First 10 lines of input CSV {new_input_file}:\n{''.join(lines)}")
                except Exception as e:
                    logger.warning(f"Could not read first lines of input CSV {new_input_file}: {e}")
            
            # Execute RRG binary
            cmd = [
                self.rrg_binary,
                "-csvpath", new_input_file,
                "-outputpath", new_output_file
            ]
            
            logger.info(f"Executing RRG binary: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            logger.info(f"RRG binary stdout: {result.stdout}")
            logger.info(f"RRG binary stderr: {result.stderr}")
            logger.info(f"RRG binary return code: {result.returncode}")
            
            if result.returncode != 0:
                logger.error(f"RRG binary failed: {result.stderr}")
                raise RuntimeError(f"RRG binary failed: {result.stderr}")
            
            # Read and parse output file
            if not os.path.exists(new_output_file):
                raise FileNotFoundError(f"Output file not generated: {new_output_file}")
            
            # Check if file is empty
            if os.path.getsize(new_output_file) == 0:
                logger.error(f"Output file is empty: {new_output_file}")
                with open(new_output_file, 'r') as f:
                    logger.error(f"Output file contents: '{f.read()}'")
                raise ValueError(f"RRG output file is empty: {new_output_file}")
            
            try:
                with open(new_output_file, 'r') as f:
                    rrg_output = json.load(f)
            except Exception as e:
                with open(new_output_file, 'r') as f:
                    file_contents = f.read()
                logger.error(f"Failed to parse RRG output file as JSON. Contents:\n{file_contents}", exc_info=True)
                raise
            
            # Process date formats based on timeframe
            if timeframe == "daily":
                for stock in rrg_output["datalists"]:
                    stock["data"] = [[d[0].replace("00", "").replace(":", "").replace(" ", "")] + d[1:] 
                                   for d in stock["data"]]
            
            logger.info(f"Successfully processed RRG data: {new_output_file}")
            return rrg_output
            
        except Exception as e:
            logger.error(f"Error executing RRG binary: {str(e)}")
            raise 
