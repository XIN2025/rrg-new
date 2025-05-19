import logging
import os
from typing import Optional

try:
    from config import LOGGING_CONFIG
except ImportError:
    LOGGING_CONFIG = {
        "level": os.environ.get("LOG_LEVEL", "INFO"),
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    }

LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}

# Configure root logger
def configure_logging(log_level: Optional[str] = None) -> None:
    """
    Configure the root logger with the specified log level.
    
    Args:
        log_level: Log level string (DEBUG, INFO, WARNING, ERROR, CRITICAL)
                  If None, uses LOGGING_CONFIG or LOG_LEVEL env var
    """
    if log_level is None:
        log_level = LOGGING_CONFIG.get("level", "INFO").upper()
    else:
        log_level = log_level.upper()
    
    level = LOG_LEVELS.get(log_level, logging.INFO)
    log_format = LOGGING_CONFIG.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Configure the root logger
    logging.basicConfig(
        level=level,
        format=log_format,
        force=True  # Override any existing configuration
    )
    
    # Log the configuration
    logging.info(f"Logging configured with level: {log_level}")

# Get a named logger
def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    
    Args:
        name: The name of the logger
        
    Returns:
        A configured logger instance
    """
    return logging.getLogger(name) 