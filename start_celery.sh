#!/bin/bash

# Create logs directory if it doesn't exist
mkdir -p logs

# Set Redis URL
export CELERY_REDIS_URL="redis://localhost:6379/0"

# Start the Celery worker
echo "Starting Celery worker..."
python -m celery -A src.celery_app worker -Q celery,data_processing --loglevel=INFO > logs/celery_worker.log 2>&1 &
WORKER_PID=$!

# Start Celery beat scheduler
echo "Starting Celery beat scheduler..."
python -m celery -A src.celery_app beat --loglevel=INFO --scheduler=django_celery_beat.schedulers:DatabaseScheduler > logs/celery_beat.log 2>&1 &
BEAT_PID=$!

echo "Celery worker PID: $WORKER_PID"
echo "Celery beat PID: $BEAT_PID"
echo "Logs available at logs/celery_worker.log and logs/celery_beat.log"

cleanup() {
  echo "Stopping Celery processes..."
  if kill -0 $WORKER_PID > /dev/null 2>&1; then
    kill $WORKER_PID
  fi
  if kill -0 $BEAT_PID > /dev/null 2>&1; then
    kill $BEAT_PID
  fi
  exit 0
}

# Register the cleanup function for the SIGINT and SIGTERM signals
trap cleanup SIGINT SIGTERM

echo "Press Ctrl+C to stop Celery processes"
tail -f logs/celery_worker.log
