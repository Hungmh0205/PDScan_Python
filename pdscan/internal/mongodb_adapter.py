"""
MongoDB adapter implementation
"""

from typing import List, Any, Optional
from urllib.parse import urlparse
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import time

from .data_store_adapter import Adapter
from .scan_opts import ScanOptions

class MongodbAdapter(Adapter):
    """Adapter for MongoDB with connection pooling, SSL, retry"""
    
    def __init__(self, url: str, config: Optional[dict] = None):
        super().__init__(url)
        self.client = None
        self.db = None
        self.config = config or {}
        self._retry_attempts = self.config.get('retry_attempts', 3)
        self._timeout = self.config.get('timeout', 30)
        self._ssl = self.config.get('ssl', False)
        self._max_pool_size = self.config.get('pool_size', 10)
        
    @classmethod
    def from_config(cls, db_config: dict):
        return cls(db_config['url'], db_config)
        
    def connect(self) -> None:
        """Connect to MongoDB with SSL, retry, connection pooling"""
        parsed = urlparse(self.url)
        if parsed.scheme != "mongodb":
            raise ValueError("Invalid MongoDB URL scheme")
            
        attempt = 0
        while attempt < self._retry_attempts:
            try:
                # Tạo client với connection pooling và SSL
                client_options = {
                    'serverSelectionTimeoutMS': self._timeout * 1000,
                    'maxPoolSize': self._max_pool_size,
                    'minPoolSize': 1,
                    'maxIdleTimeMS': 30000,
                }
                
                if self._ssl:
                    client_options.update({
                        'ssl': True,
                        'ssl_cert_reqs': 'CERT_REQUIRED',
                        'ssl_ca_certs': self.config.get('certificate_path'),
                    })
                
                self.client = MongoClient(self.url, **client_options)
                
                # Test connection
                self.client.admin.command('ping')
                self.db = self.client.get_database()
                return
                
            except (ConnectionFailure, ServerSelectionTimeoutError) as e:
                attempt += 1
                if attempt >= self._retry_attempts:
                    raise e
                print(f"MongoDB connection attempt {attempt} failed, retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
        
    def disconnect(self) -> None:
        """Disconnect from MongoDB"""
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
        
    def _get_items(self) -> List[str]:
        """Get collections to scan"""
        return self.db.list_collection_names()
        
    def _get_values(self, collection: str, options: ScanOptions) -> List[str]:
        """Get values from collection"""
        values = []
        cursor = self.db[collection].find().limit(options.sample_size)
        
        for doc in cursor:
            values.extend(self._extract_string_values(doc))
            
        return values
        
    def _extract_string_values(self, doc: dict) -> List[str]:
        """Extract string values from document"""
        values = []
        for value in doc.values():
            if isinstance(value, str):
                values.append(value)
            elif isinstance(value, dict):
                values.extend(self._extract_string_values(value))
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, str):
                        values.append(item)
                    elif isinstance(item, dict):
                        values.extend(self._extract_string_values(item))
        return values
        
    def fetch_tables(self) -> List[str]:
        """Fetch list of collections"""
        return self.db.list_collection_names()
        
    def fetch_table_data(self, collection: str) -> List[dict]:
        """Fetch documents from collection"""
        return list(self.db[collection].find())
        
    def fetch_names(self) -> List[str]:
        """Fetch list of collection names"""
        return self.fetch_tables() 