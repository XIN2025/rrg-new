import requests
import json
import time
import os
import random
from pathlib import Path
from typing import Dict, Any, List, Set, Tuple, Callable
from datetime import datetime
import logging
import polars as pl
from compare_debug_responses import JsonComparator

logger = logging.getLogger(__name__)

# Constants from test_random_configs.py
# INDEX_SYMBOLS = ["CNX500", "NSE Index", "NIFTYNXT50"]
INDEX_SYMBOLS = ["CNX500"]
VALID_COMBINATIONS = [
    {"timeframe": "60m", "date_range": "5 days"},
    {"timeframe": "daily", "date_range": "3 months"},
    # {"timeframe": "60m", "date_range": "3 months"},
    {"timeframe": "daily", "date_range": "1 year"},
    {"timeframe": "weekly", "date_range": "5 years"},
    {"timeframe": "monthly", "date_range": "5 years"},
]

# Fields to compare for each symbol
FIELDS_TO_COMPARE = ["momentum", "ratio", "close_price", "change_percentage"]

# Fields to ignore in comparison
IGNORE_FIELDS = [
    "filename", "cacheHit", "timestamp", "request_id", "status", 
    "execution_time", "metadata", "version", "hash", "cache_key", 
    "created_at", "updated_at", "processed_at", "query_time",
    "id", "correlation_id", "trace_id", "server_id", "environment",
    "latency", "source", "target", "build", "commit", "host"
]

def log_raw_responses(local_response: Dict[str, Any], prod_response: Dict[str, Any], timestamp: str):
    """Save raw responses for debugging"""
    debug_dir = Path("debug_responses")
    debug_dir.mkdir(exist_ok=True)
    with open(debug_dir / f"local_raw_{timestamp}.json", "w") as f:
        json.dump(local_response, f, indent=2)
    with open(debug_dir / f"prod_raw_{timestamp}.json", "w") as f:
        json.dump(prod_response, f, indent=2)
    print(f"Raw responses saved to debug_responses/[local|prod]_raw_{timestamp}.json")



def get_last_traded_time() -> str:
    """Fetch the last traded time from the API"""
    url = "https://api-prod.strike.money/v1/api/lasttradedtime"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-IN,en-US;q=0.9,en-GB;q=0.8,en;q=0.7",
        "Origin": "https://web.strike.money",
        "Priority": "u=1, i",
        "Sec-Ch-Ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Linux"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    }
    
    print("Fetching last traded time...")
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    last_traded_time = response.text.strip('"')  # Remove quotes if present
    print(f"Last traded time: {last_traded_time}")
    return last_traded_time

def create_payload(date_range: str, timeframe: str, index_symbol: str, last_traded_time: str) -> Dict[str, Any]:
    """Create the request payload with the given configuration"""
    return {
        "channel_name": "user/379f6c05-97e8-469e-9fab-c8bc3dd39d34",
        "date_range": date_range,
        "index_symbol": index_symbol,
        "is_custom_index": False,
        "last_traded_time": last_traded_time,
        "tickers": ["ABB","Adani Trans","Adani Green","Adani Power","Ambuja Cem","Bajaj Holdin","BankofBaroda","Bosch","Canara Bank","Chola.Invest","Dabur India","Divis Labora","DLF","Avenue Super","GAIL","GodrejConsum","HAL","HavellsIndia","ICICI LGICL","ICICIPruLife","InterGlo.Avi","IOC","IRFC","JindalStlPow","JSW Energy","LIC of India","Macrotec.Dev","LT Infotech","MothrsnSumiS","Info Edge","PFC","PidiliteInds","PNB","Rural Electr","Shree Cement","Siemens","TataPowerCom","Torrent Phar","TVS Motor","United Spiri","Varun Bevera","Sesa Goa","CadilaHealth"],
        "timeframe": timeframe,
        "skip_cache": True,
    }
def make_local_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Make a request to the localhost server"""
    url = "http://0.0.0.0:8000/rrg"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJodHRwczovL2hhc3VyYS5pby9qd3QvY2xhaW1zIjp7IngtaGFzdXJhLWFsbG93ZWQtcm9sZXMiOlsiY3VzdG9tZXIiXSwieC1oYXN1cmEtZGVmYXVsdC1yb2xlIjoiY3VzdG9tZXIiLCJ4LWhhc3VyYS11c2VyLWlkIjoiMmZjYjM4OWYtZWE3OC00ZGNjLThhYmYtNmQwNjYzNTQ5YjRjIiwieC1oYXN1cmEtdXNlci1pcy1hbm9ueW1vdXMiOiJmYWxzZSJ9LCJzdWIiOiIyZmNiMzg5Zi1lYTc4LTRkY2MtOGFiZi02ZDA2NjM1NDliNGMiLC",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-IN,en-US;q=0.9,en-GB;q=0.8,en;q=0.7",
        "Origin": "https://web.strike.money",
        "Priority": "u=1, i",
        "Sec-Ch-Ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Linux"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    }
    
    print("Making request to localhost...")
    start_time = time.time()
    response = requests.post(url, json=payload, headers=headers)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Local request completed in {elapsed_time:.2f} seconds")
    
    response.raise_for_status()
    return response.json()

def make_production_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Make a request to the production server"""
    url = "https://api-prod.strike.money/v1/pyapi/rrg/rrg"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJodHRwczovL2hhc3VyYS5pby9qd3QvY2xhaW1zIjp7IngtaGFzdXJhLWFsbG93ZWQtcm9sZXMiOlsiY3VzdG9tZXIiXSwieC1oYXN1cmEtZGVmYXVsdC1yb2xlIjoiY3VzdG9tZXIiLCJ4LWhhc3VyYS11c2VyLWlkIjoiMzc5ZjZjMDUtOTdlOC00NjllLTlmYWItYzhiYzNkZDM5ZDM0IiwieC1oYXN1cmEtdXNlci1pcy1hbm9ueW1vdXMiOiJmYWxzZSJ9LCJzdWIiOiIzNzlmNmMwNS05N2U4LTQ2OWUtOWZhYi1jOGJjM2RkMzlkMzQiLCJpYXQiOjE3NDYyMDQ3NjAsImV4cCI6MTc0NjM3NzU2MCwiaXNzIjoiaGFzdXJhLWF1dGgifQ.DeCR7zlArxJF7zpC_Emy9BkgHo4CWRaPPpu5XKJskpw",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-IN,en-US;q=0.9,en-GB;q=0.8,en;q=0.7",
        "Origin": "https://web.strike.money",
        "Priority": "u=1, i",
        "Sec-Ch-Ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Linux"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    }
    
    print("Making request to production API...")
    start_time = time.time()
    response = requests.post(url, json=payload, headers=headers)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Production request completed in {elapsed_time:.2f} seconds")
    
    response.raise_for_status()
    return response.json()

def save_responses(local_response: Dict[str, Any], prod_response: Dict[str, Any], last_traded_time: str) -> str:
    """Save the local and production responses to JSON files in a structured folder system"""
    # Create safe folder name from last_traded_time by replacing invalid characters
    folder_name = last_traded_time.replace(":", "-").replace("/", "-").replace(" ", "_")
    
    # Create the folder structure
    base_dir = Path("test_result_zip")
    response_dir = base_dir / folder_name
    
    # Create directories if they don't exist
    os.makedirs(response_dir, exist_ok=True)
    
    # Save the responses to JSON files
    with open(response_dir / "local.json", "w") as f:
        json.dump(local_response, f, indent=2)
    
    with open(response_dir / "prod.json", "w") as f:
        json.dump(prod_response, f, indent=2)
    
    # print(f"Responses saved to {response_dir}")
    return str(response_dir)


def compare_using_json_comparator(local_response: Dict[str, Any], prod_response: Dict[str, Any], timestamp: str) -> Tuple[bool, Dict[str, Any]]:
    """Compare responses using the JsonComparator class
    
    Args:
        local_response: The response from the local server
        prod_response: The response from the production server
        timestamp: Timestamp string for creating filenames
        
    Returns:
        Tuple containing a boolean (True if identical) and a detailed comparison result
    """
    try:
        # Save responses to temporary files for JsonComparator
        debug_dir = Path("debug_responses")
        debug_dir.mkdir(exist_ok=True)
        
        local_file = debug_dir / f"local_{timestamp}.json"
        prod_file = debug_dir / f"prod_{timestamp}.json"
        
        with open(local_file, "w") as f:
            json.dump(local_response, f, indent=2)
        
        with open(prod_file, "w") as f:
            json.dump(prod_response, f, indent=2)
        
        # Create JsonComparator instance
        comparator = JsonComparator(str(prod_file), str(local_file))
        
        # Run all comparisons
        result = comparator.compare_all()
        
        # Clean up temporary files (optional - you may want to keep them for debugging)
        # local_file.unlink()
        # prod_file.unlink()
        
        return result["overall_match"], result
    except Exception as e:
        # print(f"Error in JSON comparison: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # Return a dummy result with error information
        error_result = {
            "overall_match": False,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "sections": {
                "benchmark": {"match": False, "message": f"Error: {str(e)}"},
                "indexdata": {"match": False, "differences": [f"Error: {str(e)}"], "missing_in_local": [], "extra_in_local": []},
                "datalists": {"match": False, "differences": [f"Error: {str(e)}"], "missing_in_local": [], "extra_in_local": []},
                "change_data": {"match": False, "differences": [f"Error: {str(e)}"], "missing_in_local": [], "extra_in_local": []},
                "other_fields": {"match": False, "differences": [f"Error: {str(e)}"]}
            }
        }
        return False, error_result

def save_comparison_results(comparison_result: Dict[str, Any], timestamp: str, config: Dict[str, str], index_symbol: str) -> str:
    """Save the detailed comparison results to a JSON file in the comparison folder.
    
    Args:
        comparison_result: The detailed comparison result dictionary
        timestamp: Timestamp string for creating a unique filename
        config: The configuration used for the comparison (timeframe and date_range)
        index_symbol: The index symbol used for the comparison
        
    Returns:
        The path to the saved file
    """
    # Create the comparison directory if it doesn't exist
    comparison_dir = Path("comparison")
    comparison_dir.mkdir(exist_ok=True)
    
    # Extract only the differences that didn't match
    mismatch_data = {
        "metadata": {
            "timestamp": timestamp,
            "config": config,
            "index_symbol": index_symbol,
            "overall_match": comparison_result.get("overall_match", False)
        },
        "mismatches": {}
    }
    
    # Process benchmark mismatches
    benchmark = comparison_result.get("sections", {}).get("benchmark", {})
    if not benchmark.get("match", True):
        mismatch_data["mismatches"]["benchmark"] = {
            "message": benchmark.get("message", "")
        }
    
    # Process indexdata mismatches
    indexdata = comparison_result.get("sections", {}).get("indexdata", {})
    if not indexdata.get("match", True):
        mismatch_data["mismatches"]["indexdata"] = {
            "differences": indexdata.get("differences", []),
            "missing_in_local": indexdata.get("missing_in_local", []),
            "extra_in_local": indexdata.get("extra_in_local", [])
        }
    
    # Process datalists mismatches
    datalists = comparison_result.get("sections", {}).get("datalists", {})
    if not datalists.get("match", True):
        mismatch_data["mismatches"]["datalists"] = {
            "differences": datalists.get("differences", []),
            "missing_in_local": datalists.get("missing_in_local", []),
            "extra_in_local": datalists.get("extra_in_local", [])
        }
    
    # Process change_data mismatches
    change_data = comparison_result.get("sections", {}).get("change_data", {})
    if not change_data.get("match", True):
        mismatch_data["mismatches"]["change_data"] = {
            "differences": change_data.get("differences", []),
            "missing_in_local": change_data.get("missing_in_local", []),
            "extra_in_local": change_data.get("extra_in_local", [])
        }
    
    # Process other fields mismatches
    other_fields = comparison_result.get("sections", {}).get("other_fields", {})
    if not other_fields.get("match", True):
        mismatch_data["mismatches"]["other_fields"] = {
            "differences": other_fields.get("differences", [])
        }
    
    # Create the filename with timestamp
    filename = f"comparison_{timestamp}.json"
    filepath = comparison_dir / filename
    
    # Save the comparison results to a JSON file
    with open(filepath, "w") as f:
        json.dump(mismatch_data, f, indent=2)
    
    # print(f"Detailed comparison results saved to {filepath}")
    return str(filepath)

def run_detailed_comparison():
    """Run the detailed comparison logic"""
    config = random.choice(VALID_COMBINATIONS)
    index_symbol = random.choice(INDEX_SYMBOLS)
    
    print(f"\nUsing configuration:")
    print(f"Index Symbol: {index_symbol}")
    print(f"Timeframe: {config['timeframe']}")
    print(f"Date Range: {config['date_range']}\n")
    
    last_traded_time = get_last_traded_time()
    payload = create_payload(config['date_range'], config['timeframe'], index_symbol, last_traded_time)
    
    try:
        local_response = make_local_request(payload)
        prod_response = make_production_request(payload)
        
        # Log raw responses
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_raw_responses(local_response, prod_response, timestamp)
        
        # Compare using JsonComparator
        try:
            is_identical, comparison_result = compare_using_json_comparator(local_response, prod_response, timestamp)
            
            # Save detailed comparison results to a timestamped file in the comparison folder
            save_comparison_results(comparison_result, timestamp, config, index_symbol)
            
            if is_identical:
                print("Local and production responses are identical")
            else:
                print("Local and production responses are different")
                print("Differences found:")
                
                # Print section-specific differences
                sections = comparison_result.get("sections", {})
                
                if not sections.get("benchmark", {}).get("match", True):
                    print(f"- Benchmark: {sections.get('benchmark', {}).get('message', '')}")
                
                if not sections.get("indexdata", {}).get("match", True):
                    # print("- Indexdata differences:")
                    diff_count = len(sections.get("indexdata", {}).get("differences", []))
                    # print(f"  {diff_count} differences found")
                    
                if not sections.get("datalists", {}).get("match", True):
                    # print("- Datalists differences:")
                    missing = sections.get("datalists", {}).get("missing_in_local", [])
                    extra = sections.get("datalists", {}).get("extra_in_local", [])
                    # print(f"  {len(missing)} symbols missing in local")
                    # print(f"  {len(extra)} extra symbols in local")
                
                if not sections.get("change_data", {}).get("match", True):
                    print("- Change data differences:")
                    missing = sections.get("change_data", {}).get("missing_in_local", [])
                    extra = sections.get("change_data", {}).get("extra_in_local", [])
                    print(f"  {len(missing)} change data items missing in local")
                    print(f"  {len(extra)} extra change data items in local")
                
                if not sections.get("other_fields", {}).get("match", True):
                    print("- Other fields differences:")
                    for diff in sections.get("other_fields", {}).get("differences", []):
                        print(f"  {diff}")
                
                
                # Reference the detailed comparison results file
                print(f"\nDetailed comparison results saved to comparison/comparison_{timestamp}.json")
                
        except Exception as e:
            print(f"Error comparing responses: {str(e)}")
            import traceback
            traceback.print_exc()
        
    except Exception as e:
        print(f"Error occurred during comparison: {str(e)}")
        import traceback
        traceback.print_exc()
        with open("symbol_comparison_errors.log", "a") as f:
            f.write(f"{datetime.now().isoformat()} - Error: {str(e)}\n")
            f.write(traceback.format_exc() + "\n")

if __name__ == "__main__":
    while True:
        run_detailed_comparison() 
        time.sleep(3)