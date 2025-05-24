from fastapi import APIRouter, HTTPException, Depends
from .schemas import RrgRequest, RrgResponse, StatusResponse
from .service import RRGService
from src.auth.jwt import get_current_user
from src.utils.metrics import (
    TimerMetric, RRGRequestMetrics, 
    record_rrg_error, record_rrg_data_points
)
from src.utils.logger import get_logger
from src.modules.rrg.metadata_store import get_metadata_status, refresh_metadata
import time
import logging
from datetime import datetime, timedelta
from src.utils.duck_pool import get_duckdb_connection

# Configure logger
logger = get_logger("rrg_router")

router = APIRouter(prefix="/rrg", tags=["RRG"])
service = RRGService()


@router.post("/", response_model=RrgResponse)
async def get_rrg_data(request: RrgRequest):
    """Get RRG data for the specified request"""
    try:
        logger.info(f"Processing RRG request for index: {request.index_symbol}, timeframe: {request.timeframe}")
        
        response = await service.get_rrg_data(request)
        
        if not response:
            raise HTTPException(status_code=500, detail="Failed to get RRG data")
            
        return response
        
    except Exception as e:
        logger.error(f"Error getting RRG data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# Metadata status and refresh endpoints
@router.get("/metadata/status")
async def metadata_status(
    current_user: str = Depends(get_current_user)
):
    """Get the current status of the RRG metadata cache"""
    with TimerMetric("metadata_status_endpoint", "rrg_router"):
        logger.info(f"Metadata status check requested by user: {current_user}")
        
        try:
            status = get_metadata_status()
            logger.debug(f"Metadata status: {status}")
            return status
        except Exception as e:
            record_rrg_error("metadata_status")
            logger.error(f"Error retrieving metadata status: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))


@router.post("/metadata/refresh", response_model=StatusResponse)
async def refresh_metadata_endpoint(
    current_user: str = Depends(get_current_user)
):
    """Manually refresh the RRG metadata cache"""
    with TimerMetric("refresh_metadata_endpoint", "rrg_router"):
        start_time = time.time()
        logger.info(f"Metadata refresh requested by user: {current_user}")
        
        try:
            success = refresh_metadata()
            elapsed_time = time.time() - start_time
            
            if success:
                status_msg = f"Metadata refreshed successfully in {elapsed_time:.2f}s"
                logger.info(status_msg)
                status = get_metadata_status()
                logger.debug(f"Updated metadata status: {status}")
                
                # Record metadata size in metrics
                if status and isinstance(status, dict):
                    for key, value in status.items():
                        if key in ["indices", "indices_stocks", "companies", "stocks"] and isinstance(value, int):
                            record_rrg_data_points(value, f"metadata_{key}")
                            
                return StatusResponse(status=status_msg)
            else:
                record_rrg_error("metadata_refresh_failed")
                error_msg = "Failed to refresh metadata"
                logger.error(error_msg)
                raise HTTPException(status_code=500, detail=error_msg)
        except Exception as e:
            record_rrg_error("metadata_refresh")
            logger.error(f"Error refreshing metadata: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))


# Purpose: Primarily responsible for loading End-of-Day (EOD) historical stock data and essential metadata tables into DuckDB.
      # Fetches company info -> public.companies (DuckDB)
      # Fetches stock info -> public.stocks (DuckDB)
      # Fetches EOD price history (indiacharts.stock_price_eod from PG for last 12 years) -> public.eod_stock_data (DuckDB)
      # Fetches indices info -> public.indices (DuckDB)
      # Fetches index constituents -> public.indices_stocks (DuckDB)
      # Fetches combined metadata -> public.market_metadata (DuckDB)
@router.get("/load_eod_data", response_model=StatusResponse)
async def load_eod_data(
    current_user: str = Depends(get_current_user)
):
    """Load EOD data from ClickHouse to DuckDB - Only call this manually when needed"""
    try:
        logger.info("Manual EOD data load requested")
        service = RRGService()
        
        # Load last 10 years of data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365*10)
        
        success = await service.load_data(
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d')
        )
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to load EOD data")
            
        return {"status": "success", "message": "EOD data loaded successfully"}
        
    except Exception as e:
        logger.error(f"Error loading EOD data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# Fetches intraday/hourly price history (indiacharts.stock_prices from PG for last 6 months) -> public.stock_prices (DuckDB)
@router.get("/load_hourly_data", response_model=StatusResponse)
async def load_hourly_data_endpoint(
    current_user: str = Depends(get_current_user)
):
    with TimerMetric("load_hourly_data_endpoint", "rrg_router"):
        start_time = time.time()
        logger.info(f"Hourly data load requested by user: {current_user}")

        try:
            await service.load_hourly_data()
            elapsed_time = time.time() - start_time
            logger.info(f"Hourly data loaded in {elapsed_time:.2f}s")
            return StatusResponse(status="Updated hourly stock data")
        except Exception as e:
            record_rrg_error("hourly_data_load_endpoint")
            logger.error(f"Error loading hourly data: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))


@router.post("/hourly-data")
async def update_hourly_data():
    """Update hourly stock data"""
    with TimerMetric("update_hourly_data_endpoint", "rrg_router"):
        start_time = time.time()
        logger.info("Hourly data update requested")

        try:
            success = await service.load_hourly_data()
            elapsed_time = time.time() - start_time
            
            if success:
                logger.info(f"Hourly data updated successfully in {elapsed_time:.2f}s")
                return {"status": "Updated hourly stock data"}
            else:
                raise HTTPException(status_code=500, detail="Failed to update hourly stock data")
                
        except Exception as e:
            record_rrg_error("hourly_data_update")
            logger.error(f"Error updating hourly data: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))


# generate_csv function
# This separation makes sense because your generate_csv function queries different DuckDB tables based on the requested timeframe:

# If timeframe ends in 'm' (minutes/hourly), it queries public.stock_prices.
# Otherwise (daily, weekly, monthly), it queries public.eod_stock_data.

@router.get("/metadata/check")
async def check_metadata():
    """Check metadata tables without loading data"""
    try:
        with get_duckdb_connection('data/pydb.duckdb') as conn:
            # Check if tables exist
            tables = conn.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """).fetchdf()
            
            # Test indices table
            indices = conn.execute("""
                SELECT * FROM public.indices LIMIT 5
            """).fetchdf()
            
            # Test stocks table
            stocks = conn.execute("""
                SELECT * FROM public.stocks LIMIT 5
            """).fetchdf()
            
            # Test indices_stocks table
            index_stocks = conn.execute("""
                SELECT * FROM public.indices_stocks LIMIT 5
            """).fetchdf()
            
            return {
                "status": "success",
                "tables": tables.to_dict('records'),
                "indices": indices.to_dict('records'),
                "stocks": stocks.to_dict('records'),
                "index_stocks": index_stocks.to_dict('records')
            }
    except Exception as e:
        logger.error(f"Error checking metadata: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

