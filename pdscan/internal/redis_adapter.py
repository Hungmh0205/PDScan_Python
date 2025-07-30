"""
Redis adapter implementation
"""

from typing import List, Any, Dict, Optional
from urllib.parse import urlparse
import redis
from redis.exceptions import ConnectionError, TimeoutError
import time

from .data_store_adapter import Adapter
from .scan_opts import ScanOptions

class RedisAdapter(Adapter):
    """Adapter for Redis with connection pooling, SSL, retry"""
    
    def __init__(self, url: str, config: Optional[dict] = None):
        super().__init__(url)
        self.client = None
        self.config = config or {}
        self._retry_attempts = self.config.get('retry_attempts', 3)
        self._timeout = self.config.get('timeout', 30)
        self._ssl = self.config.get('ssl', False)
        self._max_connections = self.config.get('pool_size', 10)
        
    @classmethod
    def from_config(cls, db_config: dict):
        return cls(db_config['url'], db_config)
        
    def connect(self) -> None:
        """Connect to Redis with SSL, retry, connection pooling"""
        parsed = urlparse(self.url)
        if parsed.scheme not in ["redis", "rediss"]:
            raise ValueError("Invalid Redis URL scheme")
            
        attempt = 0
        while attempt < self._retry_attempts:
            try:
                # Tạo connection pool với SSL
                pool_options = {
                    'max_connections': self._max_connections,
                    'socket_timeout': self._timeout,
                    'socket_connect_timeout': 10,
                    'retry_on_timeout': True,
                }
                
                if self._ssl:
                    pool_options.update({
                        'ssl': True,
                        'ssl_cert_reqs': 'required',
                        'ssl_ca_certs': self.config.get('certificate_path'),
                    })
                
                # Tạo connection pool
                pool = redis.ConnectionPool.from_url(
                    self.url,
                    **pool_options
                )
                
                self.client = redis.Redis(connection_pool=pool)
                
                # Test connection
                self.client.ping()
                return
                
            except (ConnectionError, TimeoutError) as e:
                attempt += 1
                if attempt >= self._retry_attempts:
                    raise e
                print(f"Redis connection attempt {attempt} failed, retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
        
    def disconnect(self) -> None:
        """Disconnect from Redis"""
        if self.client:
            self.client.close()
            self.client = None
        
    def _get_items(self) -> List[str]:
        """Get key patterns to scan"""
        return self._get_key_patterns()
        
    def _get_values(self, pattern: str, options: ScanOptions) -> List[str]:
        """Get values for key pattern"""
        values = []
        count = 0
        
        for key in self.client.scan_iter():
            if count >= options.sample_size:
                break
                
            if self.client.type(key).decode() == pattern:
                value = self._get_value_for_key(key, pattern)
                if isinstance(value, str):
                    values.append(value)
                count += 1
                
        return values
        
    def _get_key_patterns(self) -> List[str]:
        """Get list of key patterns"""
        patterns = []
        for key in self.client.scan_iter():
            key_type = self.client.type(key).decode()
            if key_type not in patterns:
                patterns.append(key_type)
        return patterns
        
    def _get_value_for_key(self, key: bytes, key_type: str) -> Any:
        """Get value for key based on type"""
        if key_type == "string":
            return self.client.get(key).decode()
        elif key_type == "hash":
            return str(self.client.hgetall(key))
        elif key_type == "list":
            return str(self.client.lrange(key, 0, -1))
        elif key_type == "set":
            return str(list(self.client.smembers(key)))
        elif key_type == "zset":
            return str(self.client.zrange(key, 0, -1, withscores=True))
        return None
        
    def fetch_tables(self) -> List[str]:
        """Fetch list of key patterns"""
        return self._get_key_patterns()
        
    def fetch_table_data(self, pattern: str) -> List[dict]:
        """Fetch data for key pattern"""
        return self._get_data_for_pattern(pattern)
        
    def fetch_names(self) -> List[str]:
        """Fetch list of key patterns"""
        return self.fetch_tables()
        
    def _get_data_for_pattern(self, pattern: str) -> List[dict]:
        """Get data for key pattern"""
        data = []
        for key in self.client.scan_iter():
            if self.client.type(key).decode() == pattern:
                value = self._get_value_for_key(key, pattern)
                if value:
                    data.append({"key": key.decode(), "value": value})
        return data 