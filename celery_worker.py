#!/usr/bin/env python3
from src.celery_app import celery_app
import os
import logging
from src.utils.logger import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger("celery_worker")

# Import tasks to register them
from src.tasks.data_tasks import reload_duckdb

if __name__ == "__main__":
    logger.info("Starting Celery data processing worker")
    # Start Celery worker with command line arguments
    celery_app.worker_main(
        argv=[
            'worker',
            '--loglevel=INFO',
            '-Q', 'data_processing',  # Only process tasks from data_processing queue
            '--concurrency=1',        # Single concurrency for data processing worker
            '-n', 'data_worker@%h'    # Custom worker name
        ]
    ) 