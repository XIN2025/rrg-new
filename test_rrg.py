import requests
import json

def test_rrg_endpoint():
    url = "http://localhost:8000/rrg"
    payload = {
        "index_symbol": "NIFTY50",
        "timeframe": "daily",
        "date_range": "365"
    }
    
    try:
        response = requests.post(url, json=payload)
        print(f"Status Code: {response.status_code}")
        print("Response:")
        print(json.dumps(response.json(), indent=2))
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    test_rrg_endpoint() 
