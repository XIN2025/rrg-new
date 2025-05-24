import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Any
import ssl

# Load environment variables
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent

DEBUG = os.environ.get("DEVELOPMENT_MODE", "False") == "True"

ALLOWED_HOSTS = os.environ.get("SERVER_NAMES", "").split(",")

DB_HOST = os.getenv("DB_HOST")
DB_HOST_IP = "64.225.86.91" 
DB_SSL = os.getenv("DB_SSL", "True").lower() == "true"

DATABASE_CONFIG = {
    # Use hostname in the connection string instead of IP
    "url": f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{DB_HOST}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}",
    "options": {
        # For asyncpg, SSL is configured this way
        "ssl": "require" if DB_SSL else "disable"
    }
}

# Function to convert ssl_cert_reqs string to appropriate value
def get_ssl_cert_reqs(value):
    if value == "None" or value is None:
        return None
    elif value == "CERT_REQUIRED":
        return ssl.CERT_REQUIRED
    elif value == "CERT_OPTIONAL":
        return ssl.CERT_OPTIONAL
    else:
        return None

# Redis configuration
REDIS_CONFIG = {
    "default": {
        "host": os.getenv("REDDISHOST"),
        "port": os.getenv("REDDISPORT"),
        "password": os.getenv("REDDISPASS"),
        "username": os.getenv("REDDISUSER"),
        "ssl": os.getenv("REDDIS_SSL", "True").lower() == "true",
        "ssl_cert_reqs": get_ssl_cert_reqs(os.getenv("REDDIS_CERT_REQS")),
        "db": 2
    },
    "rrg": {
        "host": os.getenv("RRGREDISHOST"),
        "port": os.getenv("RRGREDISPORT"),
        "password": os.getenv("RRGREDISPASS"),
        "username": os.getenv("RRGREDISUSER"),
        "ssl": os.getenv("REDDIS_SSL", "True").lower() == "true",
        "ssl_cert_reqs": get_ssl_cert_reqs(os.getenv("REDDIS_CERT_REQS")),
        "db": 2
    },
    "celery": {
        "url": os.getenv("CELERY_REDIS_URL"),
        "db": 0
    }
}

# CORS configuration
CORS_CONFIG = {
    "allow_origins": ["*"],  # In production, replace with specific origins
    "allow_credentials": False,
    "allow_methods": ["*"],
    "allow_headers": ["*"],
}

# Timezone configuration
TIMEZONE = "UTC"

JWT = os.getenv("JWT")


# API configuration
API_CONFIG = {
    "title": "Strike API",
    "description": "API for Strike services",
    "version": "0.1.0",
    "docs_url": "/docs",
    "redoc_url": "/redoc",
}

# Security configuration
SECURITY_CONFIG = {
    "secret_key": os.environ.get("SECRET_KEY", "your-secret-key-here"),
    "algorithm": "HS256",
    "access_token_expire_minutes": 30,
}

# Logging Configuration
LOGGING_CONFIG = {
    "level": os.environ.get("LOG_LEVEL", "DEBUG"),
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
}

def get_database_url() -> str:
    """Get the database URL from configuration."""
    return DATABASE_CONFIG["url"]

def get_redis_config(alias: str = "default") -> Dict[str, Any]:
    """Get Redis configuration for the specified alias."""
    return REDIS_CONFIG.get(alias, REDIS_CONFIG["default"])
