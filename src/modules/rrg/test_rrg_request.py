import asyncio
import json
import sys
import os

# Add the project root directory to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.modules.rrg.service import RRGService
from src.modules.rrg.schemas import RrgRequest

async def test_rrg_request():
    # Create a sample request
    request = RrgRequest(
        index_symbol="CNX500",
        tickers=["CNX Bank", "CNX 100", "CNX 200", "CNX500"],
        timeframe="daily",
        date_range="90"
    )
    
    # Initialize service and make request
    service = RRGService()
    response = await service.get_rrg_data(request)
    
    # Print response details
    print("\nRRG Response:")
    print(f"Status: {response.status}")
    print(f"Filename: {response.filename}")
    print(f"Cache Hit: {response.cacheHit}")
    
    if response.status == "success":
        print("\nResponse Data Structure:")
        data = response.data
        print(f"Benchmark: {data['benchmark']}")
        print(f"Number of datalists: {len(data['datalists'])}")
        print(f"Number of index points: {len(data['indexdata'])}")
        print(f"Number of change data entries: {len(data['change_data'])}")
        
        # Print first datalist as example
        if data['datalists']:
            print("\nFirst Datalist Example:")
            first_datalist = data['datalists'][0]
            print(json.dumps(first_datalist, indent=2))
        
        # Print first change data entry as example
        if data['change_data']:
            print("\nFirst Change Data Example:")
            first_change = data['change_data'][0]
            print(json.dumps(first_change, indent=2))
    else:
        print(f"Error: {response.error}")

if __name__ == "__main__":
    asyncio.run(test_rrg_request()) 
