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

def main(args):
    st = time.time()
    input_folder_name = f"{args['input_folder_name']}"
    
    # Get absolute paths - use the correct project root
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, "../../../.."))  # Go up to rrg-rajesh root
    
    # Always use paths relative to project root
    input_folder_path = os.path.join(project_root, "src/modules/rrg/exports/input", input_folder_name)
    output_folder_path = os.path.join(project_root, "src/modules/rrg/exports/output", input_folder_name)
    
    print(f"[DEBUG] Project root: {project_root}")
    print(f"[DEBUG] Input folder path: {input_folder_path}")
    print(f"[DEBUG] Output folder path: {output_folder_path}")
    
    # Create output directory
    os.makedirs(output_folder_path, exist_ok=True)

    # Verify input file exists
    input_file = os.path.join(input_folder_path, f"{input_folder_name}.csv")
    if not os.path.exists(input_file):
        print(f"Error: Input file not found at {input_file}")
        return {"error": "Input file not found"}

    # Get RRG binary path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    rrgcsv_path = os.path.join(script_dir, "rrgcsv_new")
    
    if not os.path.exists(rrgcsv_path):
        print(f"Error: RRG binary not found at {rrgcsv_path}")
        return {"error": "RRG binary not found"}

    # Ensure the binary is executable
    try:
        os.chmod(rrgcsv_path, 0o755)
        if not os.access(rrgcsv_path, os.X_OK):
            print(f"Error: RRG binary is not executable at {rrgcsv_path}")
            return {"error": "RRG binary is not executable"}
    except Exception as e:
        print(f"Error: Could not set executable permissions: {str(e)}")
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
            
        print(f"[DEBUG] Created simple input file at: {simple_input_file}")
        print(f"[DEBUG] Input file size: {os.path.getsize(simple_input_file)} bytes")
    except Exception as e:
        print(f"Warning: Could not create simple input file: {str(e)}")
        simple_input_file = input_file

    # Execute RRG binary with proper paths
    config_path = os.path.join(script_dir, "rrgcsv.conf")
    
    # Create a temporary config file that generates only two files
    temp_config_path = os.path.join(script_dir, "temp_rrgcsv.conf")
    try:
        with open(config_path, 'r') as f:
            config_content = f.read()
        
        # Create a minimal config that only generates two files
        config_content = """{
    "symbollists": {
        "Date": {
            "1 Day": "Date-1_Day.json"
        }
    },
    "indexfile": "rrg-index.json"
}"""
        
        with open(temp_config_path, 'w') as f:
            f.write(config_content)
            
        print(f"[DEBUG] Created temporary config file at: {temp_config_path}")
    except Exception as e:
        print(f"Error creating temporary config: {str(e)}")
        return {"error": f"Config error: {str(e)}"}
    
    command = f"'{rrgcsv_path}' -csvpath '{input_folder_path}' -outputpath '{output_folder_path}' -config '{temp_config_path}'"
    print(f"[DEBUG] Executing command: {command}")
    print(f"[DEBUG] Working directory: {script_dir}")
    print(f"[DEBUG] Config file exists: {os.path.exists(temp_config_path)}")
    print(f"[DEBUG] Config file contents: {open(temp_config_path).read()}")

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
            
            print(f"[DEBUG] Command return code: {output.returncode}")
            print(f"[DEBUG] Command stdout: {output.stdout}")
            print(f"[DEBUG] Command stderr: {output.stderr}")
            
            if output.returncode != 0:
                print(f"Error: RRG processing failed with return code {output.returncode}")
                print(f"STDOUT: {output.stdout}")
                print(f"STDERR: {output.stderr}")
                return {"error": f"RRG processing failed: {output.stderr}"}
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
        print(f"[DEBUG] Output directory contents: {output_files}")
        
        if not output_files:
            print("Error: No output files generated")
            return {"error": "No output files generated"}
            
        # Find the -1_Day file
        data_file = None
        for file in output_files:
            if file.endswith('.json') and '-1_Day.' in file:
                data_file = file
                print(f"[DEBUG] Found data file: {file}")
                break
                
        if not data_file:
            print("Error: Could not find data file")
            return {"error": "Could not find data file"}
            
        # Delete any other files except rrg-index.json and the data file
        for file in output_files:
            if file != data_file and file != "rrg-index.json":
                try:
                    os.remove(os.path.join(output_folder_path, file))
                    print(f"[DEBUG] Removed file: {file}")
                except Exception as e:
                    print(f"[DEBUG] Could not remove file {file}: {str(e)}")
        
        # Try to read the output file
        result = read_output_file(args, input_folder_path, output_folder_path, input_folder_name)
        if not result:
            print("Error: Failed to read output file")
            return {"error": "Failed to read output file"}

        print(f"RRG processing completed in {time.time() - st:.2f}s")
        print(f"Input files preserved in: {input_folder_path}")
        print(f"Input file contents: {os.listdir(input_folder_path)}")
        
        # Ensure we have the correct structure
        if isinstance(result, dict) and "data" not in result:
            result = {"data": result}
            
        return result
    except Exception as e:
        print(f"Error: {str(e)}")
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

    print(f"[RRG_{file_name}] Input file downloaded")
    print(f'[RRG_{file_name}] {os.listdir(input_folder_path)}')

    return

def read_output_file(args, input_folder_path, output_folder_path, file_name):
    os.makedirs(output_folder_path, exist_ok=True)
    
    if not os.path.exists(output_folder_path):
        print("Error: Output directory not found")
        return False
        
    try:
        files = os.listdir(output_folder_path)
        print(f"[DEBUG] Files in output directory: {files}")
    except FileNotFoundError:
        print("Error: Could not access output directory")
        return False

    if files:
        # Find the -1_Day file
        data_file = None
        for file in files:
            if file.endswith('.json') and '-1_Day.' in file:
                data_file = file
                print(f"[DEBUG] Found data file: {file}")
                break
        
        if not data_file:
            print("Error: Could not find data file")
            return False
            
        output_file = os.path.join(output_folder_path, data_file)
        print(f"[DEBUG] Reading data file: {output_file}")

        try:
            with open(output_file, "r", encoding="utf-8-sig") as file:
                content = file.read()
                if not content.strip():
                    print("Error: Data file is empty")
                    return False
                    
                try:
                    json_data = json.loads(content)
                    if not json_data:
                        print("Error: Invalid JSON data in file")
                        return False
                        
                    print(f"[DEBUG] Raw JSON data: {json.dumps(json_data, indent=2)}")
                    
                    # Return the data directly in the expected format
                    result = {
                        "data": {
                            "benchmark": json_data.get("benchmark", ""),
                            "indexdata": json_data.get("indexdata", []),
                            "datalists": json_data.get("datalists", [])
                        },
                        "change_data": None,
                        "filename": file_name,
                        "cacheHit": False
                    }
                    
                    # Verify we have the required data
                    if not result["data"]["benchmark"] or not result["data"]["indexdata"]:
                        print("Error: Missing required data fields")
                        return False
                        
                    print(f"[DEBUG] Final result: {json.dumps(result, indent=2)}")
                    return result
                    
                except json.JSONDecodeError as e:
                    print(f"Error: Invalid JSON in file: {str(e)}")
                    return False
                    
        except Exception as e:
            print(f"Error reading file: {str(e)}")
            return False
            
    return False


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
