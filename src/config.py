import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ClickHouse Configuration
CHDB_HOST = os.getenv('CHDB_HOST', 'api-uat-v2.strike.money')
CHDB_PORT = int(os.getenv('CHDB_PORT', '18123'))
CHDB_NAME = os.getenv('CHDB_NAME', 'strike')
CHDB_USER = os.getenv('CHDB_USER', 'open_gig')
CHDB_PASSWORD = os.getenv('CHDB_PASSWORD', 'open_gig_at_strike_IC')
CHDB_SECURE = False  # Force HTTP instead of HTTPS

# Redis Configuration
CELERY_REDIS_URL = os.getenv('CELERY_REDIS_URL', 'redis://localhost:6379')
REDDISHOST = os.getenv('REDDISHOST', 'localhost')
REDDISPORT = os.getenv('REDDISPORT', '6379')
REDDISPASS = os.getenv('REDDISPASS', '')
REDDISUSER = os.getenv('REDDISUSER', 'default')
REDDIS_SSL = os.getenv('REDDIS_SSL', 'False').lower() == 'true'
REDDIS_CERT_REQS = os.getenv('REDDIS_CERT_REQS', 'None')

# Server Configuration
SERVER_NAMES = os.getenv('SERVER_NAMES', 'localhost')
RRGREDISHOST = os.getenv('RRGREDISHOST', 'localhost')
RRGREDISPORT = os.getenv('RRGREDISPORT', '6379')
RRGREDISPASS = os.getenv('RRGREDISPASS', '')
RRGREDISUSER = os.getenv('RRGREDISUSER', 'default')
EXTERNAL_HOST = os.getenv('EXTERNAL_HOST', 'localhost')
DEVELOPMENT_MODE = os.getenv('DEVELOPMENT_MODE', 'True').lower() == 'true'
JWT = os.getenv('JWT', '2ek7Ae64oby40d9BqemHvGdbrZ9ZCNYEL4B1sFqHjDCiKkpP59FkP903qa1ZmOVMnPuwpaPP2TDwgJQIvFp6/A==')
URL = os.getenv('URL', 'http://142.93.212.144:9000/dagster/graphql')

# PostgreSQL Configuration
DB_NAME = os.getenv('DB_NAME', 'defaultdb')
DB_USER = os.getenv('DB_USER', 'open_gig')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'open_gig_at_strike_IC')
DB_HOST = os.getenv('DB_HOST', 'api-uat.strike.money')
DB_PORT = int(os.getenv('DB_PORT', '25061')) 
