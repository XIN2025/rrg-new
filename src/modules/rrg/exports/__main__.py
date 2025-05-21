import subprocess
import boto3
from datetime import datetime
import os
import redis
import json
import paho.mqtt.client as mqtt
import shutil
import time
from config import REDIS_CONFIG
import logging

# Configure logging to only show errors
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Disable all other loggers
for name in logging.root.manager.loggerDict:
    if name != __name__:
        logging.getLogger(name).setLevel(logging.ERROR)

def main(args):
    st = time.time()
    input_folder_name = f"{args['input_folder_name']}"
    
    # Get absolute paths - use the correct project root
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, "../../../.."))  # Go up to rrg-rajesh root
    
    # Always use paths relative to project root
    input_folder_path = os.path.join(project_root, "src/modules/rrg/exports/input", input_folder_name)
    output_folder_path = os.path.join(project_root, "src/modules/rrg/exports/output", input_folder_name)
    
    # Create output directory
    os.makedirs(output_folder_path, exist_ok=True)

    # Verify input file exists
    input_file = os.path.join(input_folder_path, f"{input_folder_name}.csv")
    if not os.path.exists(input_file):
        return {"error": "Input file not found"}

    # Get RRG binary path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    rrgcsv_path = os.path.join(script_dir, "rrgcsv_new")
    
    if not os.path.exists(rrgcsv_path):
        return {"error": "RRG binary not found"}

    # Ensure the binary is executable
    try:
        os.chmod(rrgcsv_path, 0o755)
        if not os.access(rrgcsv_path, os.X_OK):
            return {"error": "RRG binary is not executable"}
    except Exception as e:
        return {"error": f"Permission error: {str(e)}"}

    # Create a copy of the input file with a simpler name
    simple_input_file = os.path.join(input_folder_path, "input.csv")
    try:
        # Read the original file
        with open(input_file, 'r') as f:
            content = f.read()
        
        # Write the content to the simple input file
        with open(simple_input_file, 'w') as f:
            f.write(content)
    except Exception as e:
        simple_input_file = input_file

    # Create a temporary config file that generates only two files
    temp_config_path = os.path.join(script_dir, "temp_rrgcsv.conf")
    try:
        # Create a minimal config that only generates two files
        config_content = """{
    "symbollists": {
        "Date": {
            "1 Day": "Date-1_Day.json"
        }
    },
    "indexfile": "rrg-index.json",
    "settings": {
        "decimal_places": 2,
        "price_format": "standard",
        "date_format": "YYYY-MM-DD HH:mm:ss",
        "timezone": "UTC",
        "process_all_symbols": true,
        "include_benchmark": true,
        "input_file": "input.csv",
        "output_format": "json",
        "data_points": 50,
        "calculate_relative_strength": true,
        "calculate_momentum": true,
        "calculate_roc": true,
        "calculate_ma": true,
        "calculate_std": true,
        "calculate_zscore": true,
        "calculate_rs_ratio": true,
        "calculate_rs_momentum": true,
        "benchmark_symbol": "CNX500",
        "use_benchmark_for_rs": true,
        "use_benchmark_for_momentum": true,
        "use_benchmark_for_roc": true,
        "use_benchmark_for_ma": true,
        "use_benchmark_for_std": true,
        "use_benchmark_for_zscore": true,
        "use_benchmark_for_rs_ratio": true,
        "use_benchmark_for_rs_momentum": true,
        "preserve_metadata": true,
        "metadata_fields": ["symbol", "ticker", "name", "slug", "security_code", "security_type_code"]
    }
}"""
        
        with open(temp_config_path, 'w') as f:
            f.write(config_content)
    except Exception as e:
        return {"error": f"Config error: {str(e)}"}
    
    command = f"'{rrgcsv_path}' -csvpath '{input_folder_path}' -outputpath '{output_folder_path}' -config '{temp_config_path}'"

    try:
        # Run the command with proper environment
        env = os.environ.copy()
        env["PATH"] = f"{script_dir}:{env.get('PATH', '')}"
        
        # First, change to the script directory
        original_dir = os.getcwd()
        os.chdir(script_dir)
        
        try:
            output = subprocess.run(
                command, 
                shell=True, 
                capture_output=True, 
                text=True,
                env=env
            )
            
            if output.returncode != 0:
                return {"error": f"RRG processing failed: {output.stderr}"}
                
            # Only log errors from the binary output
            if output.stderr:
                logger.error(f"RRG binary errors: {output.stderr}")
        finally:
            # Always change back to original directory
            os.chdir(original_dir)
            
        # Clean up temporary config
        try:
            os.remove(temp_config_path)
        except:
            pass
            
        # Check if output directory has files
        output_files = os.listdir(output_folder_path)
        
        if not output_files:
            return {"error": "No output files generated"}
            
        # Find the -1_Day file
        data_file = None
        for file in output_files:
            if file.endswith('.json') and '-1_Day.' in file:
                data_file = file
                break
                
        if not data_file:
            return {"error": "Could not find data file"}
            
        # Delete any other files except rrg-index.json and the data file
        for file in output_files:
            if file != data_file and file != "rrg-index.json":
                try:
                    os.remove(os.path.join(output_folder_path, file))
                except:
                    pass
        
        # Try to read the output file
        result = read_output_file(args, input_folder_path, output_folder_path, data_file)
        if not result:
            return {"error": "Failed to read output file"}

        # Ensure we have the correct structure
        if isinstance(result, dict) and "data" not in result:
            result = {"data": result}
            
        return result
    except Exception as e:
        return {"error": str(e)}

def download_file_from_s3(args, input_folder_path, file_name):
    client = boto3.client(
        "s3",
        region_name="sgp1",
        endpoint_url="https://sgp1.digitaloceanspaces.com",
        aws_access_key_id="DO00NP3RTNTFMEY9GLB9",
        aws_secret_access_key="69C8zFRea8XSspljp7cyS31mKBwrp3V1MuEu9dgV2tQ",
    )

    os.mkdir(input_folder_path)

    client.download_file(
        "iccharts",
        f"exports/{args['filename']}",
        f"{input_folder_path}/{file_name}.csv",
    )

    return

def read_output_file(args, input_folder_path, output_folder_path, file_name):
    """Read and format the output file from RRG binary."""
    try:
        # Read the JSON file with UTF-8-SIG encoding to handle BOM
        json_file_path = os.path.join(output_folder_path, file_name)
        with open(json_file_path, 'r', encoding='utf-8-sig') as f:
            json_data = json.load(f)

        # Read the original CSV to get metadata
        csv_metadata = {}
        try:
            with open(os.path.join(input_folder_path, f"{args['input_folder_name']}.csv"), 'r', encoding='utf-8-sig') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) >= 9:
                        symbol = parts[1]
                        csv_metadata[symbol] = {
                            "code": parts[1],
                            "name": parts[6],
                            "ticker": parts[5],
                            "slug": parts[7],
                            "security_code": parts[3],
                            "security_type_code": float(parts[8])
                        }
        except Exception as e:
            logger.error(f"Error reading metadata from CSV: {str(e)}")

        # Format the response
        formatted_response = {
            "indexdata": [],
            "datalists": []
        }

        # Process index data - ensure we get unique timestamps
        seen_timestamps = set()
        for point in json_data.get("indexdata", []):
            if len(point) >= 9:
                try:
                    timestamp = point[0]
                    if timestamp not in seen_timestamps:
                        seen_timestamps.add(timestamp)
                        formatted_point = [
                            timestamp,  # date
                            str(float(point[1])),  # ratio
                            str(float(point[2])),  # momentum
                            str(float(point[3])),  # additional metrics
                            str(float(point[4])),
                            str(float(point[5])),
                            str(float(point[6])),
                            str(float(point[7])),
                            str(float(point[8]))
                        ]
                        formatted_response["indexdata"].append(formatted_point)
                except (ValueError, TypeError) as e:
                    logger.error(f"Error formatting index data point: {str(e)}")
                    continue

        # Sort index data by timestamp
        formatted_response["indexdata"].sort(key=lambda x: x[0])

        # Process datalists - ensure we get unique timestamps per symbol
        for item in json_data.get("datalists", []):
            # Get the actual symbol from the data points
            actual_symbol = None
            if item.get("data") and len(item["data"]) > 0:
                first_point = item["data"][0]
                if len(first_point) >= 2:
                    # Try to find the actual symbol from the CSV metadata
                    for symbol, metadata in csv_metadata.items():
                        if symbol in first_point[1]:  # Check if symbol is part of the data point
                            actual_symbol = symbol
                            break

            # Get metadata for this symbol
            metadata = csv_metadata.get(actual_symbol, {}) if actual_symbol else {}

            # Create formatted item with proper metadata
            formatted_item = {
                "code": metadata.get("code", item.get("code", "")),
                "name": metadata.get("name", item.get("name", "")),
                "meaningful_name": metadata.get("name", item.get("meaningful_name", "")),
                "slug": metadata.get("slug", item.get("slug", "").lower().replace(" ", "-")),
                "ticker": metadata.get("ticker", item.get("ticker", "")),
                "symbol": actual_symbol or item.get("symbol", ""),
                "security_code": metadata.get("security_code", item.get("security_code", "")),
                "security_type_code": float(metadata.get("security_type_code", item.get("security_type_code", 26.0))),
                "data": []
            }

            # Track seen timestamps for this symbol
            symbol_seen_timestamps = set()

            # Format data points - ensure unique timestamps
            for point in item.get("data", []):
                if len(point) >= 9:
                    try:
                        timestamp = point[0]
                        if timestamp not in symbol_seen_timestamps:
                            symbol_seen_timestamps.add(timestamp)
                            formatted_point = [
                                timestamp,  # date
                                str(float(point[1])),  # ratio
                                str(float(point[2])),  # momentum
                                str(float(point[3])),  # additional metrics
                                str(float(point[4])),
                                str(float(point[5])),
                                str(float(point[6])),
                                str(float(point[7])),
                                str(float(point[8]))
                            ]
                            formatted_item["data"].append(formatted_point)
                    except (ValueError, TypeError) as e:
                        logger.error(f"Error formatting data point: {str(e)}")
                        continue

            # Sort data points by timestamp
            formatted_item["data"].sort(key=lambda x: x[0])
            formatted_response["datalists"].append(formatted_item)

        return formatted_response

    except Exception as e:
        logger.error(f"Error reading output file: {str(e)}")
        raise

def get_rrg_redis_client():
    r = redis.Redis(
        host=REDIS_CONFIG["rrg"]["host"],
        port=REDIS_CONFIG["rrg"]["port"],
        password=REDIS_CONFIG["rrg"]["password"],
        db=REDIS_CONFIG["rrg"]["db"]
    )
    return r

def get_redis_client():
    r = redis.Redis(
        host=REDIS_CONFIG["default"]["host"],
        port=REDIS_CONFIG["default"]["port"],
        password=REDIS_CONFIG["default"]["password"],
        db=REDIS_CONFIG["default"]["db"]
    )
    return r

def return_cleint():
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, transport="websockets")
    mqttc.ws_set_options(path="/")
    mqttc.tls_set()
    mqttc.connect(
        host="msgb-prod.strike.money",
        port=443,
    )
    return mqttc

def publish_msg(mqttc, json_data, channel_name):
    r = get_redis_client()
    if channel_name == "general-broadcast":
        key = "TC3vuAKrc0oTU1Ghd3AXsZ823wTcn1G6"
    else:
        r_key = r.get(f"emitterio:userchannel:{channel_name.split('/')[-1]}")
        key = json.loads(r_key.decode())["key"]

    mqttc.publish(
        topic=f"{key}/{channel_name}/",
        payload=json.dumps(json_data),
    )
    return
