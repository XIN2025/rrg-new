#!/bin/bash

# Kill any existing processes
pkill -f "celery worker"
pkill -f "celery beat"
pkill -f "uvicorn"

# Start Redis if not running
if ! pgrep redis-server > /dev/null; then
    echo "Starting Redis..."
    redis-server &
    sleep 2
fi

# Start Celery worker
echo "Starting Celery worker..."
celery -A celery_worker worker --loglevel=info &

# Start Celery beat
echo "Starting Celery beat..."
celery -A celery_beat beat --loglevel=info &

# Start Uvicorn
echo "Starting Uvicorn..."
uvicorn main:app --host 0.0.0.0 --port 8000 --reload &

echo "All services started. Press Ctrl+C to stop all services."
wait 
