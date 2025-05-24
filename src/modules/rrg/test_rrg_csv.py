import os
import sys
import json
from datetime import datetime, timedelta

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.append(project_root)

from src.modules.rrg.service import RRGService
from src.modules.rrg.schemas import RrgRequest

async def test_rrg_csv():
    """Test RRG CSV generation and processing"""
    try:
        # Create test request
        request = RrgRequest(
            index_symbol="CNX500",
            tickers=["RELIANCE", "TCS", "HDFCBANK"],
            timeframe="daily",
            date_range=30
        )
        
        # Initialize service
        service = RRGService()
        
        # Get RRG data
        response = await service.get_rrg_data(request)
        
        if response.status == "success":
            print("\nRRG Data Processing Successful!")
            print(f"Generated files:")
            print(f"- Input CSV: {os.path.join('rrg/rrg_export/input', os.listdir('rrg/rrg_export/input')[-1])}")
            print(f"- Output JSON: {os.path.join('rrg/rrg_export/output', os.listdir('rrg/rrg_export/output')[-1])}")
            
            print("\nResponse Summary:")
            print(f"- Status: {response.status}")
            print(f"- Cache Hit: {response.cacheHit}")
            print(f"- Filename: {response.filename}")
            print(f"- Number of datalists: {len(response.data['datalists'])}")
            print(f"- Number of index points: {len(response.data['indexdata'])}")
            print(f"- Number of change data entries: {len(response.data['change_data'])}")
        else:
            print(f"\nError: {response.error}")
            
    except Exception as e:
        print(f"\nTest failed with error: {str(e)}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_rrg_csv()) 
