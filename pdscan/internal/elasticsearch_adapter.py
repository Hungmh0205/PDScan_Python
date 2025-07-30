"""
Elasticsearch adapter implementation
"""

from typing import List, Any, Optional
from urllib.parse import urlparse
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, ConnectionTimeout
import time

from .data_store_adapter import Adapter
from .scan_opts import ScanOptions

class ElasticsearchAdapter(Adapter):
    """Adapter for Elasticsearch with connection pooling, SSL, retry"""
    
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
        """Connect to Elasticsearch with SSL, retry, connection pooling"""
        parsed = urlparse(self.url)
        if parsed.scheme not in ["elasticsearch", "https"]:
            raise ValueError("Invalid Elasticsearch URL scheme")
            
        attempt = 0
        while attempt < self._retry_attempts:
            try:
                # Tạo client với connection pooling và SSL
                client_options = {
                    'timeout': self._timeout,
                    'max_retries': 3,
                    'retry_on_timeout': True,
                    'maxsize': self._max_connections,
                }
                
                if self._ssl:
                    client_options.update({
                        'use_ssl': True,
                        'verify_certs': True,
                        'ca_certs': self.config.get('certificate_path'),
                        'ssl_assert_hostname': False,
                    })
                
                self.client = Elasticsearch([self.url], **client_options)
                
                # Test connection
                self.client.ping()
                return
                
            except (ConnectionError, ConnectionTimeout) as e:
                attempt += 1
                if attempt >= self._retry_attempts:
                    raise e
                print(f"Elasticsearch connection attempt {attempt} failed, retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
        
    def disconnect(self) -> None:
        """Disconnect from Elasticsearch"""
        if self.client:
            self.client.close()
            self.client = None
        
    def _get_items(self) -> List[str]:
        """Get indices to scan"""
        return self.fetch_tables()
        
    def _get_values(self, index: str, options: ScanOptions) -> List[str]:
        """Get values from index"""
        values = []
        try:
            response = self.client.search(
                index=index,
                body={
                    "query": {"match_all": {}},
                    "size": options.sample_size
                }
            )
            
            for hit in response["hits"]["hits"]:
                values.extend(self._extract_string_values(hit["_source"]))
        except Exception:
            # If we can't read the index, just return the index name
            values.append(index)
            
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
        """Fetch list of indices"""
        return list(self.client.indices.get_alias().keys())
        
    def fetch_table_data(self, index: str) -> List[dict]:
        """Fetch documents from index"""
        response = self.client.search(
            index=index,
            body={
                "query": {"match_all": {}},
                "size": 1000
            }
        )
        return [hit["_source"] for hit in response["hits"]["hits"]]
        
    def fetch_names(self) -> List[str]:
        """Fetch list of index names"""
        return self.fetch_tables() 