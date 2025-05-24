import json
import redis
import pytz
import logging
from typing import Any, Optional, Dict
from datetime import datetime, timedelta, time
from config import REDIS_CONFIG, get_redis_config

TZ = 'Asia/Kolkata'

class CacheManager:
    _instances: Dict[str, 'CacheManager'] = {}
    
    def __new__(cls, alias: str = "default"):
        if alias not in cls._instances:
            instance = super(CacheManager, cls).__new__(cls)
            instance._initialize(alias)
            cls._instances[alias] = instance
        return cls._instances[alias]
        
    def _initialize(self, alias: str):
        """Initialize the cache manager with the specified Redis configuration."""
        logging.basicConfig(level=logging.WARNING)
        self.logger = logging.getLogger(f"{self.__class__.__name__}_{alias}")
        self.alias = alias
        
        try:
            redis_config = get_redis_config(alias)
            self._redis_client = redis.Redis(
                host=redis_config.get("host", "localhost"),
                port=int(redis_config.get("port", 6379)),
                password=redis_config.get("password", ""),
                username=redis_config.get("username", ""),
                ssl=redis_config.get("ssl", False),
                ssl_cert_reqs=redis_config.get("ssl_cert_reqs", None),
                db=redis_config.get("db", 0),
                decode_responses=True,
                socket_timeout=10,
                socket_connect_timeout=10
            )
            
            # Test connection
            self._redis_client.ping()
            self.logger.info(f'Successfully initialized Cache Manager for alias: {alias}')
        except redis.exceptions.ConnectionError as e:
            self.logger.error(f'Redis connection error for alias {alias}: {str(e)}')
            self._redis_client = None
        except Exception as e:
            self.logger.error(f'Unexpected error initializing Cache Manager for alias {alias}: {str(e)}')
            self._redis_client = None

    def close(self):
        """Close the Redis connection."""
        if self._redis_client:
            try:
                self._redis_client.close()
                self.logger.info(f'Closed Redis connection for alias: {self.alias}')
            except Exception as e:
                self.logger.error(f'Error closing Redis connection for alias {self.alias}: {str(e)}')
            
            # Remove this instance from the _instances dictionary
            if self.alias in self.__class__._instances:
                del self.__class__._instances[self.alias]

    @classmethod
    def close_all(cls):
        """Close all Redis connections."""
        for instance in cls._instances.values():
            if instance._redis_client:
                try:
                    instance._redis_client.close()
                except Exception as e:
                    instance.logger.error(f"Error closing Redis connection: {str(e)}")
        cls._instances.clear()

    @property
    def redis_client(self) -> Optional[redis.Redis]:
        return self._redis_client

    def getCache(self, key: str) -> Optional[Any]:
        """Get a value from the cache."""
        try:
            if not self._redis_client:
                self.logger.warning(f'Cannot get cache {key} - Redis client is not initialized')
                return None
                
            value = self.redis_client.get(key)
            self.logger.info(f'GET cache {key}')
            return value
        except Exception as e:
            self.logger.error('Issue on GET cache', exc_info=e)
            return None
        

    def setCache(self, key: str, value: Any, expiry_in_minutes: int = 10) -> bool:
        """Set a value in the cache with market-aware expiry."""
        try:
            if not self._redis_client:
                self.logger.warning(f'Cannot set cache {key} - Redis client is not initialized')
                return False

            # Handle different types of values
            if isinstance(value, (dict, list)):
                try:
                    value = json.dumps(value)
                except (TypeError, ValueError) as e:
                    self.logger.error(f'Failed to serialize value for key {key}: {str(e)}')
                    return False
            elif isinstance(value, (int, float, str, bool)):
                value = str(value)
            else:
                try:
                    value = str(value)
                except Exception as e:
                    self.logger.error(f'Failed to convert value to string for key {key}: {str(e)}')
                    return False
            
            expiry_at_unix_time = self._get_redis_expiry_unix_time(expiry_in_minutes)
            self.redis_client.set(key, value, exat=expiry_at_unix_time)
            self.logger.info(f'SET cache {key}')
            return True
        except redis.exceptions.OutOfMemoryError as e:
            self.logger.warning(f'Redis out of memory for key {key}: {str(e)}. Continuing without caching this data.')
            return False  # Continue without failing
        except redis.exceptions.ResponseError as e:
            self.logger.error(f'Redis response error for key {key}: {str(e)}')
            return False
        except Exception as e:
            self.logger.error('Issue on SET cache', exc_info=e)
            return False

    def setExpiry(self, key: str, expiry_in_minutes: int = 10) -> bool:
        """Set market-aware expiry for an existing key."""
        try:
            expiry_at_unix_time = self._get_redis_expiry_unix_time(expiry_in_minutes)
            self.redis_client.expireat(key, expiry_at_unix_time)
            self.logger.info(f'SET EXPIREAT cache {key}')
            return True
        except redis.exceptions.OutOfMemoryError as e:
            self.logger.warning(f'Redis out of memory when setting expiry for key {key}: {str(e)}. Continuing without setting expiry.')
            return False  # Continue without failing
        except Exception as e:
            self.logger.error('Issue on SET EXPIREAT cache', exc_info=e)
            return False

    def delete(self, key: str) -> bool:
        """Delete a key from the cache."""
        try:
            result = bool(self.redis_client.delete(key))
            self.logger.info(f'DELETE cache {key}')
            return result
        except Exception as e:
            self.logger.error('Issue on DELETE cache', exc_info=e)
            return False

    def clear(self) -> bool:
        """Clear all keys from the cache."""
        try:
            result = bool(self.redis_client.flushdb())
            self.logger.info('CLEAR cache')
            return result
        except Exception as e:
            self.logger.error('Issue on CLEAR cache', exc_info=e)
            return False

    def _get_redis_expiry_unix_time(self, expiry_in_minutes: int = 10) -> int:
        """Calculate expiry time based on market hours."""
        now = datetime.now(pytz.timezone(TZ))

        def set_to_market_start_time(dt: datetime) -> datetime:
            return dt.replace(hour=9, minute=21, second=59)

        week_day = now.weekday()
        
        if week_day in (5, 6):  # Saturday or Sunday
            expiry_at = set_to_market_start_time(now + timedelta(days=(7 - week_day)))
        elif now.time() < time(9, 21):  # Before market open
            expiry_at = set_to_market_start_time(now)
        elif now.time() > time(15, 30):  # After market close
            expiry_at = set_to_market_start_time(now + timedelta(days=1))
        else:  # During market hours
            expiry_at = self._get_nearest_nth_minute_datetime(now, expiry_in_minutes)

        return int(expiry_at.timestamp())

    def _get_nearest_nth_minute_datetime(self, now: datetime, expiry_in_minutes: int) -> datetime:
        """Calculate the nearest nth minute datetime for expiry."""
        current_minute = now.minute
        mod_current_minute = current_minute % expiry_in_minutes
        actual_mod_current_minute = expiry_in_minutes if mod_current_minute == 0 else mod_current_minute
        
        nth_minute_datetime = (
            now + timedelta(minutes=(expiry_in_minutes - actual_mod_current_minute))
        ).replace(second=59, microsecond=0)

        return nth_minute_datetime 

    def get_last_sync_timestamp(self, key: str) -> Optional[str]:
        """Get the last sync timestamp from Redis
        
        Args:
            key: The Redis key where the timestamp is stored
            
        Returns:
            The timestamp string if found, None otherwise
        """
        try:
            if not self._redis_client:
                self.logger.warning(f'Cannot get timestamp {key} - Redis client is not initialized')
                return None
                
            value = self.redis_client.get(key)
            self.logger.info(f'GET timestamp {key}')
            return value
        except Exception as e:
            self.logger.error('Issue getting timestamp', exc_info=e)
            return None
    
    def set_last_sync_timestamp(self, key: str, timestamp: str) -> bool:
        """Store a timestamp in Redis
        
        Args:
            key: The Redis key to store the timestamp
            timestamp: The timestamp string to store
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self._redis_client:
                self.logger.warning(f'Cannot set timestamp {key} - Redis client is not initialized')
                return False
                
            self.redis_client.set(key, timestamp)
            self.logger.info(f'SET timestamp {key} to {timestamp}')
            return True
        except Exception as e:
            self.logger.error('Issue setting timestamp', exc_info=e)
            return False 
