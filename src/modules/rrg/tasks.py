from src.celery_app import celery_app
from .sync_worker import sync_stock_prices, sync_stock_price_eod
from .metadata_store import refresh_metadata, refresh_price_data
import logging
from celery.utils.log import get_task_logger
from src.tasks.data_tasks import update_reference_data, purge_old_stock_data, clean_rrg_export_directories

logger = get_task_logger("celery_tasks")

@celery_app.task(name="run_stock_prices_sync", bind=True)
def run_stock_prices_sync(self):
    """Celery task to sync stock prices"""
    try:
        logger.info(f"Starting stock prices sync task. Request: {self.request.id}")
        # The sync_stock_prices function now returns bool to indicate success/failure
        result = sync_stock_prices()
        if result is True:
            logger.info(f"Stock prices sync task {self.request.id} completed successfully")
        else:
            logger.info(f"Stock prices sync task {self.request.id} skipped (another process running or failed)")
        return result
    except Exception as e:
        logger.error(f"Error in stock prices sync task {self.request.id}: {str(e)}")
        # Retry with exponential backoff    


@celery_app.task(name="sync_stock_prices_eod", bind=True)
def sync_stock_prices_eod(self):
    """Celery task to sync stock prices from Postgres to DuckDB"""
    try:
        logger.info(f"Starting EOD stock prices sync task. Request: {self.request.id}")
        # The sync_stock_price_eod function now returns bool to indicate success/failure
        result = sync_stock_price_eod()
        if result is True:
            logger.info(f"EOD stock prices sync task {self.request.id} completed successfully")
        else:
            logger.info(f"EOD stock prices sync task {self.request.id} skipped (another process running or failed)")
        return result
    except Exception as e:
        logger.error(f"Error in EOD stock prices sync task {self.request.id}: {str(e)}")
        # Retry with exponential backoff    


@celery_app.task(name="refresh_rrg_metadata", bind=True)
def refresh_rrg_metadata(self):
    """Celery task to refresh RRG metadata cache"""
    try:
        logger.info(f"Starting RRG metadata refresh task. Request: {self.request.id}")
        
        result = refresh_metadata()
        
        if result is True:
            logger.info(f"RRG metadata refresh task {self.request.id} completed successfully")
        else:
            logger.error(f"RRG metadata refresh task {self.request.id} failed")
        
        return result
    except Exception as e:
        logger.error(f"Error in RRG metadata refresh task {self.request.id}: {str(e)}")
        # Could implement retry logic here if needed
        return False


@celery_app.task(name="refresh_rrg_price_data", bind=True)
def refresh_rrg_price_data(self):
    """Celery task to refresh RRG price data cache every 10 minutes"""
    try:
        logger.info(f"Starting RRG price data refresh task. Request: {self.request.id}")
        
        result = refresh_price_data()
        
        if result is True:
            logger.info(f"RRG price data refresh task {self.request.id} completed successfully")
        else:
            logger.error(f"RRG price data refresh task {self.request.id} failed")
        
        return result
    except Exception as e:
        logger.error(f"Error in RRG price data refresh task {self.request.id}: {str(e)}")
        return False


@celery_app.task(name="update_reference_data_task", bind=True)
def update_reference_data_task(self):
    """Celery task to update reference data"""
    try:
        logger.info(f"Starting reference data update task. Request: {self.request.id}")
        result = update_reference_data()
        if result is True:
            logger.info(f"Reference data update task {self.request.id} completed successfully")
        else:
            logger.error(f"Reference data update task {self.request.id} failed")
        return result
    except Exception as e:
        logger.error(f"Error in reference data update task {self.request.id}: {str(e)}")
        return False


@celery_app.task(name="purge_old_stock_data_task", bind=True)
def purge_old_stock_data_task(self):
    """Celery task to purge old stock data"""
    try:
        logger.info(f"Starting stock data purge task. Request: {self.request.id}")
        result = purge_old_stock_data()
        if result is True:
            logger.info(f"Stock data purge task {self.request.id} completed successfully")
        else:
            logger.error(f"Stock data purge task {self.request.id} failed")
        return result
    except Exception as e:
        logger.error(f"Error in stock data purge task {self.request.id}: {str(e)}")
        return False


@celery_app.task(name="clean_rrg_export_directories_task", bind=True)
def clean_rrg_export_directories_task(self):
    """Celery task to clean RRG export directories"""
    try:
        logger.info(f"Starting RRG export directories cleanup task. Request: {self.request.id}")
        result = clean_rrg_export_directories()
        if result is True:
            logger.info(f"RRG export directories cleanup task {self.request.id} completed successfully")
        else:
            logger.error(f"RRG export directories cleanup task {self.request.id} failed")
        return result
    except Exception as e:
        logger.error(f"Error in RRG export directories cleanup task {self.request.id}: {str(e)}")
        return False