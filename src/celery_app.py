from celery import Celery
from config import REDIS_CONFIG
from celery.schedules import crontab
import logging
import os
import time
from prometheus_client import Summary, Counter, Gauge

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("celery")

# Create Celery app
celery_app = Celery(
    'fastapi_rrg',
    broker=REDIS_CONFIG["celery"]["url"],
    backend=REDIS_CONFIG["celery"]["url"],
    broker_connection_retry_on_startup=True    #if redis connection fails, retry on startup
)

# Configure Celery
celery_app.conf.update(
    worker_concurrency=1,
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=70, 
    worker_max_tasks_per_child=100,
    broker_transport_options={'visibility_timeout': 70},
    beat_scheduler='django_celery_beat.schedulers:DatabaseScheduler',
    beat_schedule={
        'sync-stock-prices': {
            'task': 'run_stock_prices_sync',
            'schedule': 360.0,
            'options': {'expires': 60} 
        },
        "sync-stock-prices-eod": {
            "task": "sync_stock_prices_eod",
            "schedule": 660.0, 
            "options": {"expires": 60}  
        },
        'refresh-rrg-metadata-daily': {
            'task': 'refresh_rrg_metadata',
            'schedule': crontab(hour=3, minute=15),
            'options': {
                'queue': 'data_processing'
            }
        },
        'refresh-rrg-price-data': {
            'task': 'refresh_rrg_price_data',
            'schedule': 400.0,
            'options': {
                'queue': 'data_processing',
                'expires': 60
            }
        },
        'update-reference-data': {
            'task': 'update_reference_data_task',
            'schedule': crontab(hour=0, minute=15),
            'options': {
                'queue': 'data_processing'
            }
        },
        'purge-old-stock-data': {
            'task': 'purge_old_stock_data_task',
            'schedule': crontab(hour=0, minute=5),
            'options': {
                'queue': 'data_processing'
            }
        },
        'clean-rrg-export-directories': {
            'task': 'clean_rrg_export_directories_task',
            'schedule': crontab(hour=0, minute=0),
            'options': {
                'queue': 'data_processing'
            }
        }
    }
)

celery_app.autodiscover_tasks(['src.modules.rrg']) 

# Setup Celery signal handlers for metrics collection
from celery.signals import (
    task_prerun, task_postrun, task_retry, task_failure,
    worker_ready, worker_shutdown
)
from src.utils.metrics import (
    CeleryTaskMetrics, celery_task_execution_time,
    celery_task_success, celery_task_failure, 
    record_sync_worker_rows_loaded
)

_task_monitors = {}

@task_prerun.connect
def task_prerun_handler(task_id, task, *args, **kwargs):
    """Handler called before task execution"""
    queue = kwargs.get('delivery_info', {}).get('routing_key', 'default')
    logger.info(f"Starting task {task.name} with id {task_id} on queue {queue}")
    
@task_postrun.connect
def task_postrun_handler(task_id, task, retval, state, *args, **kwargs):
    """Handler called after task execution"""
    logger.info(f"Task {task.name} with id {task_id} finished with state {state}")

@task_retry.connect
def task_retry_handler(request, reason, einfo, *args, **kwargs):
    """Handler called when a task is retried"""
    logger.warning(f"Task {request.task} is being retried due to: {reason}")

@task_failure.connect
def task_failure_handler(task_id, exception, *args, **kwargs):
    """Handler called when a task fails"""
    logger.error(f"Task {task_id} failed with error: {str(exception)}")

@worker_ready.connect
def worker_ready_handler(sender, **kwargs):
    """Handler called when a worker is ready"""
    logger.info(f"Worker {sender.hostname} is ready")
    
@worker_shutdown.connect
def worker_shutdown_handler(sender, **kwargs):
    """Handler called when a worker shuts down"""
    logger.info(f"Worker {sender.hostname} is shutting down") 
