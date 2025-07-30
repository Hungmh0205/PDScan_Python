"""
S3 adapter implementation
"""

from typing import List, Any, Optional
from urllib.parse import urlparse
import boto3
from botocore.exceptions import ClientError, ConnectionError
from botocore.config import Config
import time

from .data_store_adapter import Adapter
from .scan_opts import ScanOptions

class S3Adapter(Adapter):
    """Adapter for S3 with connection pooling, SSL, retry"""
    
    def __init__(self, url: str, config: Optional[dict] = None):
        super().__init__(url)
        self.client = None
        self.bucket = None
        self.config = config or {}
        self._retry_attempts = self.config.get('retry_attempts', 3)
        self._timeout = self.config.get('timeout', 30)
        self._ssl = self.config.get('ssl', True)  # S3 default to SSL
        self._max_connections = self.config.get('pool_size', 10)
        
    @classmethod
    def from_config(cls, db_config: dict):
        return cls(db_config['url'], db_config)
        
    def connect(self) -> None:
        """Connect to S3 with SSL, retry, connection pooling"""
        parsed = urlparse(self.url)
        if parsed.scheme != "s3":
            raise ValueError("Invalid S3 URL scheme")
            
        attempt = 0
        while attempt < self._retry_attempts:
            try:
                # Tạo client với connection pooling và SSL
                session = boto3.Session()
                config = Config(
                    connect_timeout=self._timeout,
                    read_timeout=self._timeout,
                    max_pool_connections=self._max_connections,
                    retries={'max_attempts': 3}
                )
                
                # SSL configuration
                if self._ssl:
                    # S3 uses HTTPS by default
                    endpoint_url = None
                else:
                    # For testing with local S3-compatible services
                    endpoint_url = self.config.get('endpoint_url')
                
                self.client = session.client(
                    "s3",
                    config=config,
                    endpoint_url=endpoint_url,
                    aws_access_key_id=self.config.get('access_key_id'),
                    aws_secret_access_key=self.config.get('secret_access_key'),
                    region_name=self.config.get('region', 'us-east-1')
                )
                
                self.bucket = parsed.netloc
                
                # Test connection
                self.client.head_bucket(Bucket=self.bucket)
                return
                
            except (ClientError, ConnectionError) as e:
                attempt += 1
                if attempt >= self._retry_attempts:
                    raise e
                print(f"S3 connection attempt {attempt} failed, retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
        
    def disconnect(self) -> None:
        """Disconnect from S3"""
        if self.client:
            self.client.close()
            self.client = None
            self.bucket = None
        
    def _get_items(self) -> List[str]:
        """Get objects to scan"""
        return self.fetch_files()
        
    def _get_values(self, key: str, options: ScanOptions) -> List[str]:
        """Get values from S3 object"""
        values = []
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            content = response["Body"].read().decode("utf-8")
            
            for line in content.splitlines():
                values.append(line.strip())
        except Exception:
            # If we can't read the object, just return the key
            values.append(key)
            
        return values
        
    def fetch_files(self) -> List[str]:
        """Fetch list of objects"""
        return self._get_objects()
        
    def fetch_tables(self) -> List[str]:
        """Fetch list of objects (alias for fetch_files)"""
        return self.fetch_files()
        
    def fetch_table_data(self, key: str) -> List[dict]:
        """Fetch data from S3 object"""
        return [{"key": key, "content": self._get_values(key, ScanOptions())[:10]}]
        
    def fetch_names(self) -> List[str]:
        """Fetch list of object names"""
        return [key.split('/')[-1] for key in self.fetch_files()]
        
    def _get_objects(self) -> List[str]:
        """Get list of objects"""
        objects = []
        paginator = self.client.get_paginator("list_objects_v2")
        
        for page in paginator.paginate(Bucket=self.bucket):
            if "Contents" in page:
                for obj in page["Contents"]:
                    objects.append(obj["Key"])
                    
        return objects 