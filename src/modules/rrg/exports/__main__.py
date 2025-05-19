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
    input_folder_path = args['input_folder_path']
    
    # Get absolute paths
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, "../../../.."))
    
    if not os.path.isabs(input_folder_path):
        input_folder_path = os.path.join(project_root, input_folder_path)
    
    output_folder_path = os.path.join(project_root, f"src/modules/rrg/exports/output/{input_folder_name}")
    os.makedirs(output_folder_path, exist_ok=True)

    # Verify input file exists
    input_file = os.path.join(input_folder_path, f"{input_folder_name}.csv")
    if not os.path.exists(input_file):
        print("Error: Input file not found")
        return {"error": "Input file not found"}

    script_dir = os.path.dirname(os.path.abspath(__file__))
    rrgcsv_path = os.path.join(script_dir, "rrgcsv_new")
    
    if not os.path.exists(rrgcsv_path):
        print("Error: RRG binary not found")
        return {"error": "RRG binary not found"}

    # Ensure the binary is executable
    try:
        os.chmod(rrgcsv_path, 0o755)
    except Exception as e:
        print(f"Warning: Could not set executable permissions: {str(e)}")

    # Create a copy of the input file with a simpler name
    simple_input_file = os.path.join(input_folder_path, "input.csv")
    try:
        shutil.copy2(input_file, simple_input_file)
    except Exception as e:
        print(f"Warning: Could not create simple input file: {str(e)}")
        simple_input_file = input_file

    command = f"{rrgcsv_path} -csvpath '{input_folder_path}' -outputpath '{output_folder_path}'"

    try:
        output = subprocess.run(command, shell=True, capture_output=True, text=True)
        
        if output.returncode != 0:
            print(f"Error: RRG processing failed with return code {output.returncode}")
            print(f"STDOUT: {output.stdout}")
            print(f"STDERR: {output.stderr}")
            return {"error": "RRG processing failed"}
            
        # Check if output directory has files
        if not os.listdir(output_folder_path):
            print("Error: No output files generated")
            return {"error": "No output files generated"}
            
        # Try to read the output file
        success = read_output_file(args, input_folder_path, output_folder_path, input_folder_name)
        if not success:
            print("Error: Failed to read output file")
            return {"error": "Failed to read output file"}

        print(f"RRG processing completed in {time.time() - st:.2f}s")
        return {
            "body": {
                "response_type": "in_channel",
                "text": "Processing completed successfully",
            }
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"error": str(e)}
    finally:
        # Clean up the simple input file if it was created
        if simple_input_file != input_file and os.path.exists(simple_input_file):
            try:
                os.remove(simple_input_file)
            except Exception as e:
                print(f"Warning: Could not remove simple input file: {str(e)}")

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
    except FileNotFoundError:
        print("Error: Could not access output directory")
        return False

    if files:
        # Filter out any files with "rrg-" prefix and get only JSON files
        files = [x for x in files if "rrg-" not in x and x.endswith('.json')]
        if not files:
            print("Error: No valid output files found")
            return False
            
        # Sort files by size to get the largest one
        files.sort(key=lambda x: os.path.getsize(os.path.join(output_folder_path, x)), reverse=True)
        output_file = os.path.join(output_folder_path, files[0])

        try:
            with open(output_file, "r", encoding="utf-8-sig") as file:
                content = file.read()
                if not content.strip():
                    print("Error: Output file is empty")
                    return False
                    
                try:
                    json_data = json.loads(content)
                    if not json_data:
                        print("Error: Invalid JSON data in output file")
                        return False
                        
                    # Validate the expected structure
                    if "benchmark" not in json_data or "indexdata" not in json_data or "datalists" not in json_data:
                        print("Error: Output file missing required fields")
                        return False
                        
                    # Format indexdata as strings with 2 decimal places
                    json_data["indexdata"] = [f"{float(x):.2f}" for x in json_data["indexdata"]]
                    
                    # Ensure datalists have the correct structure
                    for item in json_data["datalists"]:
                        if "data" in item:
                            # Format each data point to have 9 elements
                            formatted_data = []
                            for point in item["data"]:
                                if len(point) < 9:
                                    point = point + ["0"] * (9 - len(point))
                                formatted_data.append(point)
                            item["data"] = formatted_data
                            
                            # Add required fields if missing
                            if "slug" not in item:
                                item["slug"] = item["code"].lower().replace(" ", "-")
                            if "ticker" not in item:
                                item["ticker"] = item["code"]
                            if "security_code" not in item:
                                item["security_code"] = ""
                            if "security_type_code" not in item:
                                item["security_type_code"] = 26.0
                    
                    if args.get("do_publish", True):
                        json_data = {
                            "user": "server",
                            "date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                            "topic": "cacherefreshed",
                            "subtopic": "rrg",
                        }
                        publish_msg(return_cleint(), json_data, args["channel_name"])
                    
                    return True
                    
                except json.JSONDecodeError as e:
                    print(f"Error: Invalid JSON format in output file: {str(e)}")
                    return False
                    
        except Exception as e:
            print(f"Error: Could not read output file: {str(e)}")
            return False
    else:
        print("Error: No output files found")
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
