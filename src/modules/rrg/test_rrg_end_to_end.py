import os
import sys
import json
import shutil
from datetime import datetime

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
sys.path.append(project_root)

from src.modules.rrg.helper.csv_generator import CSVCenerator
from src.modules.rrg.helper.rrg_executor import RRGExecutor

# Minimal test RRG data
rrg_data = {
    "benchmark": "CNX500",
    "indexdata": ["2400.50", "2410.00", "2420.00"],
    "datalists": [
        {
            "code": "RELIANCE",
            "name": "Reliance Industries",
            "meaningful_name": "Reliance Industries",
            "slug": "reliance-industries",
            "ticker": "RELIANCE",
            "symbol": "RELIANCE",
            "security_code": "500325",
            "security_type_code": 26.0,
            "data": [
                ["2025-05-24 00:00:00", "0", "0", "2400.50"],
                ["2025-05-25 00:00:00", "0", "0", "2410.00"],
                ["2025-05-26 00:00:00", "0", "0", "2420.00"]
            ]
        }
    ],
    "change_data": [
        {
            "symbol": "RELIANCE",
            "created_at": "2025-05-24",
            "change_percentage": 0,
            "close_price": 2400.50
        }
    ]
}

def ensure_clean_output_path(output_file):
    """Ensure the output path is clean and ready for writing"""
    # Remove if it's a directory
    if os.path.isdir(output_file):
        print(f"Removing directory at: {output_file}")
        shutil.rmtree(output_file)
    
    # Remove if it's a file
    if os.path.isfile(output_file):
        print(f"Removing existing file at: {output_file}")
        os.remove(output_file)
    
    # Ensure parent directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

def main():
    print("--- RRG End-to-End Test ---")
    
    # Initialize components
    csv_gen = CSVCenerator("CNX500")
    rrg_exec = RRGExecutor()
    
    try:
        # Generate file paths
        input_file, output_file = csv_gen.generate_csv(rrg_data, timeframe="daily")
        print(f"CSV generated at: {input_file}")
        print(f"Will output JSON to: {output_file}")
        
        # Ensure clean output path
        ensure_clean_output_path(output_file)
        
        # Debug: Check output_file path state before running binary
        if os.path.isdir(output_file):
            print(f"[DEBUG BEFORE] Output path is a directory: {output_file}")
        elif os.path.isfile(output_file):
            print(f"[DEBUG BEFORE] Output path is a file: {output_file}")
        else:
            print(f"[DEBUG BEFORE] Output path does not exist: {output_file}")
        
        # Execute RRG binary
        try:
            result = rrg_exec.execute(input_file, output_file, timeframe="daily")
        except Exception as e:
            print(f"[DEBUG] Exception during RRG binary execution: {e}")
        
        # Debug: Check output_file path state after running binary
        if os.path.isdir(output_file):
            print(f"[DEBUG AFTER] Output path is a directory: {output_file}")
        elif os.path.isfile(output_file):
            print(f"[DEBUG AFTER] Output path is a file: {output_file}")
        else:
            print(f"[DEBUG AFTER] Output path does not exist: {output_file}")
        
        # Verify output
        if os.path.exists(output_file):
            print(f"Success! RRG binary output written to: {output_file}")
            if 'result' in locals():
                print(f"Result keys: {list(result.keys())}")
                print(f"Number of datalists: {len(result.get('datalists', []))}")
                print(f"Number of index points: {len(result.get('indexdata', []))}")
        else:
            print(f"Error: Output file was not created at {output_file}")
            
    except Exception as e:
        print(f"Error during test execution: {str(e)}")
        raise

if __name__ == "__main__":
    main() 

