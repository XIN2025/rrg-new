import asyncio
from src.modules.rrg.service import RRGService, RrgRequest
import logging

logging.basicConfig(level=logging.INFO)

async def main():
    service = RRGService()
    request = RrgRequest(
        index_symbol="CNX500",
        date_range="3 months",
        timeframe="daily",
        channel_name="user/379f6c05-97e8-469e-9fab-c8bc3dd39d34",
        is_custom_index=False,
        last_traded_time="2025-05-23T09:54:59.000000000Z",
        tickers=[
            "CNX Bank", "CNX 100", "CNX 200", "CNX500", "CNX Auto", "CNX Commo", "CNX Consum", "CNX Energy", "CNX FMCG",
            "CNX Infra", "CNX IT", "CNX Media", "CNX Metal", "CNX Midcap", "CNX MNC", "CNX Pharma", "CNX PSE", "CNX PSU Bank",
            "CNX Realty", "CNX Smallcap", "CPSE", "CNX Fin", "Nifty MCap50", "NIFMCSELECT", "NIFINDIAMANU", "NiftyMCap150",
            "NIFMICCAP250", "Nifty MSC400", "NiftySCap250", "Nifty SCap50", "NIFTOTALMAR", "Nifty CD", "Nifty Health",
            "Nifty LMC250", "NSEMID", "NIFTY OILGAS", "Nifty PBI", "NSE Index"
        ]
    )
    print("Sending RRG request...")
    response = await service.get_rrg_data(request)
    print("RRG response:")
    print(response)

if __name__ == "__main__":
    asyncio.run(main()) 
