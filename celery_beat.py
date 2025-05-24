#!/usr/bin/env python3
from src.celery_app import celery_app
import logging
from src.utils.logger import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger("celery_beat")

if __name__ == "__main__":
    logger.info("Starting Celery Beat scheduler")
    # Start Celery beat scheduler with command line arguments
    celery_app.start(
        argv=[
            'beat',
            '--loglevel=INFO',
            '--scheduler=django_celery_beat.schedulers:DatabaseScheduler'
        ]
    ) 