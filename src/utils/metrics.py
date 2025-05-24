from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
import time

# Define metrics
function_time = Histogram(
    'function_execution_time_seconds', 
    'Time spent in functions',
    ['function_name', 'module']
)

function_calls = Counter(
    'function_calls_total',
    'Number of calls to functions',
    ['function_name', 'module']
)

duck_db_query_time = Histogram(
    'duck_db_query_time_seconds',
    'Time spent executing DuckDB queries',
    ['query_name']
)

duck_db_query_count = Counter(
    'duck_db_query_count_total',
    'Number of DuckDB queries executed',
    ['query_name']
)

# RRG-specific metrics
rrg_request_duration = Histogram(
    'rrg_request_duration_seconds',
    'Duration of RRG data requests',
    ['timeframe', 'index_symbol', 'date_range']
)

rrg_cache_hits = Counter(
    'rrg_cache_hits_total',
    'Number of RRG cache hits',
    ['cache_type']  # 'rrg' or 'change_historical'
)

rrg_cache_misses = Counter(
    'rrg_cache_misses_total',
    'Number of RRG cache misses',
    ['cache_type']  # 'rrg' or 'change_historical'
)

rrg_data_points = Gauge(
    'rrg_data_points',
    'Number of data points processed in RRG calculations',
    ['phase']  # 'input', 'aggregated', 'final'
)

rrg_errors = Counter(
    'rrg_errors_total',
    'Number of errors in RRG processing',
    ['error_type']  # 'data_retrieval', 'calculation', 'momentum_merge', etc.
)

# Celery sync worker metrics
celery_task_execution_time = Histogram(
    'celery_task_execution_time_seconds',
    'Time spent executing Celery tasks',
    ['task_name', 'queue']
)

celery_task_success = Counter(
    'celery_task_success_total',
    'Number of successfully completed Celery tasks',
    ['task_name', 'queue']
)

celery_task_failure = Counter(
    'celery_task_failure_total',
    'Number of failed Celery tasks',
    ['task_name', 'queue', 'error_type']
)

sync_worker_rows_loaded = Counter(
    'sync_worker_rows_loaded_total',
    'Number of rows loaded by sync worker tasks',
    ['task_name', 'source', 'destination']
)

sync_worker_rows_deleted = Counter(
    'sync_worker_rows_deleted_total',
    'Number of rows deleted by sync worker tasks',
    ['task_name', 'destination']
)

sync_worker_timestamp_lag = Gauge(
    'sync_worker_timestamp_lag_seconds',
    'Time lag between current time and last sync timestamp',
    ['task_name', 'table_name']
)

sync_worker_timestamp_track = Gauge(
    'sync_worker_timestamp_track',
    'UNIX timestamp of last successful sync operation',
    ['task_name', 'table_name']
)

sync_worker_execution_status = Counter(
    'sync_worker_execution_status_total',
    'Execution status of sync worker operations',
    ['task_name', 'status']  # status: success, failure
)

sync_worker_query_execution_time = Histogram(
    'sync_worker_query_execution_time_seconds',
    'Time spent executing queries in sync worker',
    ['task_name', 'query_type', 'source']  # query_type: stock, index, etc.
)

# Utility class for timing functions
class TimerMetric:
    def __init__(self, function_name, module=None):
        self.function_name = function_name
        self.module = module or "unknown"
        self.start_time = None
        
    def __enter__(self):
        self.start_time = time.time()
        function_calls.labels(function_name=self.function_name, module=self.module).inc()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            elapsed = time.time() - self.start_time
            function_time.labels(function_name=self.function_name, module=self.module).observe(elapsed)

# Utility class for timing DuckDB queries
class DuckDBQueryTimer:
    def __init__(self, query_name):
        self.query_name = query_name
        self.start_time = None
        
    def __enter__(self):
        self.start_time = time.time()
        duck_db_query_count.labels(query_name=self.query_name).inc()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            elapsed = time.time() - self.start_time
            duck_db_query_time.labels(query_name=self.query_name).observe(elapsed)

# Utility class for tracking RRG request metrics
class RRGRequestMetrics:
    def __init__(self, timeframe, index_symbol, date_range):
        self.timeframe = timeframe
        self.index_symbol = index_symbol
        self.date_range = date_range
        self.start_time = None
        
    def __enter__(self):
        self.start_time = time.time()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            elapsed = time.time() - self.start_time
            rrg_request_duration.labels(
                timeframe=self.timeframe,
                index_symbol=self.index_symbol,
                date_range=self.date_range
            ).observe(elapsed)
            
            if exc_type:
                rrg_errors.labels(error_type="request").inc()

class CeleryTaskMetrics:
    def __init__(self, task_name, queue='default'):
        self.task_name = task_name
        self.queue = queue
        self.start_time = None
        
    def __enter__(self):
        self.start_time = time.time()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            elapsed = time.time() - self.start_time
            celery_task_execution_time.labels(task_name=self.task_name, queue=self.queue).observe(elapsed)
            
            if exc_type:
                error_type = exc_type.__name__ if exc_type else "unknown"
                celery_task_failure.labels(task_name=self.task_name, queue=self.queue, error_type=error_type).inc()
                sync_worker_execution_status.labels(task_name=self.task_name, status="failure").inc()
            else:
                celery_task_success.labels(task_name=self.task_name, queue=self.queue).inc()
                sync_worker_execution_status.labels(task_name=self.task_name, status="success").inc()

class SyncWorkerQueryMetrics:
    def __init__(self, task_name, query_type, source):
        self.task_name = task_name
        self.query_type = query_type
        self.source = source
        self.start_time = None
        
    def __enter__(self):
        self.start_time = time.time()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            elapsed = time.time() - self.start_time
            sync_worker_query_execution_time.labels(
                task_name=self.task_name,
                query_type=self.query_type,
                source=self.source
            ).observe(elapsed)

class classSyncWorkerTimer:
    def __init__(self, function_name, module=None, query_name=None):
        self.function_name = function_name
        self.module = module or "unknown"
        self.start_time = None
        self.query_name = query_name
        
    def __enter__(self):
        self.start_time = time.time()
        function_calls.labels(function_name=self.function_name, module=self.module).inc()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            elapsed = time.time() - self.start_time
            function_time.labels(function_name=self.function_name, module=self.module).observe(elapsed)
            if self.query_name:
                duck_db_query_time.labels(query_name=self.query_name).observe(elapsed)
            
            if exc_type:
                rrg_errors.labels(error_type=self.function_name).inc()
        
def get_metrics():
    """Return all metrics in the Prometheus exposition format"""
    return generate_latest() 

# Utility functions for RRG metrics
def record_rrg_cache_hit(cache_type):
    """Record a cache hit for RRG data"""
    rrg_cache_hits.labels(cache_type=cache_type).inc()

def record_rrg_cache_miss(cache_type):
    """Record a cache miss for RRG data"""
    rrg_cache_misses.labels(cache_type=cache_type).inc()

def record_rrg_data_points(count, phase):
    """Record the number of data points in an RRG processing phase"""
    rrg_data_points.labels(phase=phase).set(count)

def record_rrg_error(error_type):
    """Record an error during RRG processing"""
    rrg_errors.labels(error_type=error_type).inc()

# Utility functions for sync worker metrics
def record_sync_worker_rows_loaded(task_name, source, destination, count):
    """Record number of rows loaded by a sync worker task"""
    sync_worker_rows_loaded.labels(task_name=task_name, source=source, destination=destination).inc(count)

def record_sync_worker_rows_deleted(task_name, destination, count):
    """Record number of rows deleted by a sync worker task"""
    sync_worker_rows_deleted.labels(task_name=task_name, destination=destination).inc(count)

def record_sync_worker_timestamp_lag(task_name, table_name, last_sync_time):
    """Record time lag between current time and last sync timestamp"""
    if last_sync_time:
        current_time = time.time()
        last_sync_unix = time.mktime(last_sync_time.timetuple())
        lag_seconds = current_time - last_sync_unix
        sync_worker_timestamp_lag.labels(task_name=task_name, table_name=table_name).set(lag_seconds)

def record_sync_worker_timestamp(task_name, table_name, timestamp):
    """Record the timestamp of a successful sync operation"""
    if timestamp:
        unix_timestamp = time.mktime(timestamp.timetuple())
        sync_worker_timestamp_track.labels(task_name=task_name, table_name=table_name).set(unix_timestamp)

