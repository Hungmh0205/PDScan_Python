"""
Module main: entrypoint cho quét dữ liệu, tích hợp các adapter và match finder.
"""

import sys
import time
import asyncio
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

from .helpers import pluralize, print_match_list
from .match_finder import MatchFinder
from .files import scan_files
from .local_file_adapter import LocalFileAdapter
from .mongodb_adapter import MongodbAdapter
from .redis_adapter import RedisAdapter
from .s3_adapter import S3Adapter
from .sql_adapter import SQLAdapter
from .elasticsearch_adapter import ElasticsearchAdapter
from .scan_opts import ScanOptions
from .exceptions import ScanError
from .mariadb_adapter import MariaDBAdapter
from .opensearch_adapter import OpenSearchAdapter
from .oracle_adapter_async import OracleAdapterAsync

# Import notification functions
try:
    from ..notification import notify_scan_complete, notify_scan_failed
    from ..notification import notify_scan_complete_email, notify_scan_failed_email
    from ..notification import notify_scan_complete_slack, notify_scan_failed_slack
    NOTIFICATION_AVAILABLE = True
except ImportError:
    NOTIFICATION_AVAILABLE = False

def get_adapter(url: str, config: Optional[Dict] = None, options: Optional[ScanOptions] = None) -> Any:
    """Get adapter for URL scheme with optional config."""
    scheme = urlparse(url).scheme
    
    # Tìm database config phù hợp
    db_config = None
    if config and 'database' in config:
        for conn in config['database']['connections']:
            if conn['url'] == url or conn.get('name', '').lower() in url.lower():
                db_config = conn
                break
    
    # Merge options into db_config for Oracle adapter
    if scheme == 'oracle' and options and hasattr(options, 'processes'):
        if db_config is None:
            db_config = {}
        # Map processes to max_concurrent_tables for Oracle
        db_config['max_concurrent_tables'] = options.processes
    
    if scheme == 'mongodb':
        return MongodbAdapter(url, db_config)
    elif scheme == 'redis':
        return RedisAdapter(url, db_config)
    elif scheme in ['postgresql', 'mysql', 'sqlite']:
        return SQLAdapter(url, db_config)
    elif scheme == 'mariadb':
        return MariaDBAdapter(url, db_config)
    elif scheme == 'oracle':
        return OracleAdapterAsync(url, db_config)
    elif scheme == 's3':
        return S3Adapter(url, db_config)
    elif scheme == 'elasticsearch':
        return ElasticsearchAdapter(url, db_config)
    elif scheme == 'opensearch':
        return OpenSearchAdapter(url, db_config)
    elif scheme == 'file':
        return LocalFileAdapter(url)
    else:
        raise ScanError(f"Unsupported URL scheme: {scheme}")

def scan(url: str, options: ScanOptions, config: Optional[Dict] = None) -> List[Dict[str, Any]]:
    """Scan data store for matches."""
    start_time = time.time()
    scan_id = f"scan_{int(start_time)}"
    user_id = getattr(options, 'user_id', 'cli_user')
    
    try:
        adapter = get_adapter(url, config, options)
        
        # Check if adapter is async and handle accordingly
        if hasattr(adapter, 'scan') and asyncio.iscoroutinefunction(adapter.scan):
            # Async adapter - run with asyncio
            matches = asyncio.run(adapter.scan(options))
        else:
            # Sync adapter - run normally
            matches = adapter.scan(options)
        
        # Gửi notification nếu có
        if NOTIFICATION_AVAILABLE and config:
            try:
                from ..config import PDScanConfig
                pdscan_config = PDScanConfig()
                notify_scan_complete(user_id, scan_id, len(matches), status="completed", config=pdscan_config)
                notify_scan_complete_email(user_id, scan_id, len(matches), status="completed", config=pdscan_config)
                notify_scan_complete_slack(user_id, scan_id, len(matches), status="completed", config=pdscan_config)
            except Exception as e:
                print(f"Warning: Failed to send notifications: {e}", file=sys.stderr)
        
        return matches
    except Exception as e:
        # Gửi notification nếu có
        if NOTIFICATION_AVAILABLE and config:
            try:
                from ..config import PDScanConfig
                pdscan_config = PDScanConfig()
                notify_scan_failed(user_id, scan_id, str(e), config=pdscan_config)
                notify_scan_failed_email(user_id, scan_id, str(e), config=pdscan_config)
                notify_scan_failed_slack(user_id, scan_id, str(e), config=pdscan_config)
            except Exception as notify_error:
                print(f"Warning: Failed to send notifications: {notify_error}", file=sys.stderr)
        
        if hasattr(options, 'debug') and options.debug:
            raise
        raise ScanError(f"Scan failed: {str(e)}")

def scan_data_sources(scan_opts: Any, config: Optional[Dict] = None) -> List[Any]:
    """Scan data sources"""
    match_list = []
    
    # Get adapter based on URL scheme
    adapter = get_adapter(scan_opts.url_str, config, scan_opts)
    if not adapter:
        print(f"Unsupported URL scheme: {urlparse(scan_opts.url_str).scheme}", file=sys.stderr)
        return []
        
    # Scan files
    if isinstance(adapter, (LocalFileAdapter, S3Adapter)):
        match_list.extend(scan_files(adapter, scan_opts))
        return match_list
        
    # Scan data stores
    adapter.init(scan_opts.url_str)
    tables = adapter.fetch_tables()
    
    if not tables:
        print(f"Found no {adapter.object_name()} to scan", file=sys.stderr)
        return []
        
    print(f"Found {pluralize(len(tables), adapter.object_name())} to scan...\n", file=sys.stderr)
    
    for table in tables:
        data = adapter.fetch_table_data(table)
        match_finder = MatchFinder(scan_opts.match_config)
        table_matches = match_finder.check_table_data(table, data)
        print_match_list(scan_opts.formatter, table_matches, scan_opts.show_data, scan_opts.show_all, "row")
        match_list.extend(table_matches)
        
    return match_list 