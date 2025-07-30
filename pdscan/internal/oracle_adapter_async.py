"""
Oracle adapter implementation - Async version with high performance optimizations
"""

import asyncio
import cx_Oracle
import time
from urllib.parse import urlparse
import re
import hashlib
import gc
import psutil
from typing import List, Any, Optional, Dict, AsyncGenerator, Set, Tuple
from contextlib import asynccontextmanager
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from .data_store_adapter import Adapter
from .scan_opts import ScanOptions
from .match_finder import MatchFinder

class OracleAdapterAsync(Adapter):
    """Async Oracle adapter with high performance optimizations"""
    
    def __init__(self, url: str, config: Optional[dict] = None):
        super().__init__(url)
        self.conn = None
        self.config = config or {}
        
        # --- Async & Concurrency ---
        self._max_concurrent_tables = self.config.get('max_concurrent_tables', 10)
        self._semaphore = asyncio.Semaphore(self._max_concurrent_tables)
        self._fetch_size = self.config.get('fetch_size', 10000)
        self._table_timeout = self.config.get('table_timeout', 120)
        
        # --- Connection Pool ---
        self._pool_min = self.config.get('pool_min', 5)
        self._pool_max = self.config.get('pool_max', 20)
        self._pool_increment = self.config.get('pool_increment', 2)
        self._pool = None
        
        # --- Schema & Skip ---
        self._skip_schemas = self.config.get('skip_schemas', {
            "SYS", "SYSTEM", "XDB", "OUTLN", "ORDSYS", "CTXSYS", "WMSYS",
            "MDSYS", "OLAPSYS", "WKSYS", "EXFSYS", "DBSNMP", "CTXOUT",
            "ANONYMOUS", "EXFSYS", "MDDATA", "ORACLE_OCM", "SPATIAL_CSW_ADMIN_USR",
            "SPATIAL_WFS_ADMIN_USR", "SI_INFORMTN_SCHEMA", "ORDDATA", "ORDPLUGINS"
        })
        self._target_schema = self.config.get('target_schema', '')
        
        # --- Algorithmic Optimization Flags ---
        self._early_termination = self.config.get('early_termination', True)
        self._value_caching = self.config.get('value_caching', True)
        self._column_optimization = self.config.get('column_optimization', True)
        self._batch_optimization = self.config.get('batch_optimization', True)
        self._pattern_optimization = self.config.get('pattern_optimization', True)
        self._adaptive_batch = self.config.get('adaptive_batch', True)
        self._min_batch_size = self.config.get('min_batch_size', 1000)
        self._max_batch_size = self.config.get('max_batch_size', 50000)
        
        # --- Retry & Resilience ---
        self._retry_attempts = self.config.get('retry_attempts', 3)
        self._retry_delay = self.config.get('retry_delay', 1)
        self._timeout = self.config.get('timeout', 30)
        
        # --- Caching & Metrics ---
        self._compiled_patterns = {}
        self._value_cache = {}
        self._column_stats = {}
        self._scan_progress = {'completed': 0, 'total': 0, 'start_time': None}
        self._metrics = {
            'total_rows_processed': 0,
            'total_matches_found': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'early_terminations': 0,
            'column_skips': 0,
            'memory_usage': [],
            'batch_times': [],
            'connection_errors': 0,
            'retry_count': 0,
            'tables_skipped': 0,
            'tables_completed': 0
        }
        
        # --- Connection Info ---
        parsed = urlparse(url)
        self._user = self.config.get('user') or parsed.username
        self._password = self.config.get('password') or parsed.password
        self._dsn = self.config.get('dsn')
        self._host = self.config.get('host') or parsed.hostname
        self._port = self.config.get('port', 1521) or (parsed.port or 1521)
        self._service_name = self.config.get('service_name') or parsed.path.lstrip('/')
        
        # --- Rich Console ---
        self.console = Console()

    @classmethod
    def from_config(cls, db_config: dict):
        return cls(db_config['url'], db_config)

    async def connect(self) -> None:
        """Connect to Oracle with connection pooling and retry."""
        attempt = 0
        dsn = self._build_dsn()
        
        while attempt < self._retry_attempts:
            try:
                # Create connection pool in executor to avoid blocking
                loop = asyncio.get_event_loop()
                self._pool = await loop.run_in_executor(
                    None,
                    lambda: cx_Oracle.SessionPool(
                        user=self._user,
                        password=self._password,
                        dsn=dsn,
                        min=self._pool_min,
                        max=self._pool_max,
                        increment=self._pool_increment,
                        encoding="UTF-8",
                        threaded=True
                    )
                )
                
                # Test connection
                async with self._get_connection() as conn:
                    await loop.run_in_executor(
                        None,
                        lambda: conn.cursor().execute("SELECT 1 FROM DUAL")
                    )
                
                self.console.print(f"✅ Connected to Oracle with async connection pool (min={self._pool_min}, max={self._pool_max})")
                return
                
            except cx_Oracle.Error as e:
                attempt += 1
                if attempt >= self._retry_attempts:
                    raise e
                self.console.print(f"⚠️ Oracle connection attempt {attempt} failed, retrying in {self._retry_delay}s...")
                await asyncio.sleep(self._retry_delay)
                self._retry_delay *= 2

    async def disconnect(self) -> None:
        """Disconnect from Oracle."""
        if self._pool:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._pool.close)
            self._pool = None
        if self.conn:
            await asyncio.get_event_loop().run_in_executor(None, self.conn.close)
            self.conn = None

    @asynccontextmanager
    async def _get_connection(self):
        """Get connection from pool with async context manager."""
        conn = None
        try:
            loop = asyncio.get_event_loop()
            conn = await loop.run_in_executor(None, self._pool.acquire)
            yield conn
        finally:
            if conn:
                await asyncio.get_event_loop().run_in_executor(None, self._pool.release, conn)

    def _build_dsn(self) -> str:
        """Build DSN string for Oracle connection."""
        if self._dsn:
            return self._dsn
        elif self._host and self._service_name:
            return cx_Oracle.makedsn(self._host, self._port, service_name=self._service_name)
        else:
            parsed = urlparse(self.url)
            host = parsed.hostname
            port = parsed.port or 1521
            service_name = parsed.path.lstrip('/')
            return cx_Oracle.makedsn(host, port, service_name=service_name)

    async def fetch_tables(self) -> List[str]:
        """Fetch tables with SELECT privilege only - async version."""
        async with self._get_connection() as conn:
            loop = asyncio.get_event_loop()
            cursor = conn.cursor()
            
            try:
                if self._target_schema:
                    await loop.run_in_executor(
                        None,
                        lambda: cursor.execute("""
                            SELECT DISTINCT c.owner, c.table_name
                            FROM all_tab_columns c
                            JOIN user_tab_privs p
                              ON c.owner = p.owner AND c.table_name = p.table_name
                            WHERE p.privilege = 'SELECT' AND c.owner = :schema
                        """, schema=self._target_schema.upper())
                    )
                else:
                    skip_schemas_list = ",".join(f"'{s}'" for s in self._skip_schemas)
                    await loop.run_in_executor(
                        None,
                        lambda: cursor.execute(f"""
                            SELECT DISTINCT c.owner, c.table_name
                            FROM all_tab_columns c
                            JOIN user_tab_privs p
                              ON c.owner = p.owner AND c.table_name = p.table_name
                            WHERE p.privilege = 'SELECT' AND c.owner NOT IN ({skip_schemas_list})
                        """)
                    )
                
                tables = await loop.run_in_executor(None, cursor.fetchall)
                return [f'"{owner}"."{table}"' for owner, table in tables]
                
            finally:
                await loop.run_in_executor(None, cursor.close)

    async def fetch_names(self) -> List[str]:
        return await self.fetch_tables()

    def _get_items(self) -> List[str]:
        """Get tables to scan - sync wrapper for compatibility."""
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If we're in an async context, we should use fetch_tables directly
            return []
        else:
            return loop.run_until_complete(self.fetch_tables())

    async def _get_values(self, table: str, options: ScanOptions) -> List[str]:
        """Get values from table - async version."""
        values = []
        try:
            data = await self.fetch_table_data(table, options)
            for item in data:
                values.append(item['value'])
        except Exception:
            values.append(table)
        return values

    async def fetch_table_data(self, table: str, options: ScanOptions = None) -> List[dict]:
        """Fetch data from table - async version."""
        if options is None:
            options = ScanOptions()
        return [match async for match in self._scan_table_streaming(table, self.match_finder.get_patterns(options), options)]

    async def scan(self, options: ScanOptions) -> List[Dict[str, Any]]:
        """Main async scan method with streaming, pooling, progress, metrics."""
        matches = []
        scan_start_time = time.time()
        
        try:
            await self.connect()
            tables = await self.fetch_tables()
            self.console.print(f"🔍 Found {len(tables)} tables to scan")
            
            # Initialize progress tracking
            self._scan_progress = {
                'completed': 0,
                'total': len(tables),
                'start_time': scan_start_time
            }
            
            patterns = self.match_finder.get_patterns(options)
            self.console.print(f"🎯 Using {len(patterns)} patterns for scanning")
            
            # Create progress bar
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeRemainingColumn(),
                console=self.console
            ) as progress:
                
                # Create main task
                main_task = progress.add_task(
                    f"[cyan]Scanning {len(tables)} tables...", 
                    total=len(tables)
                )
                
                # Scan tables concurrently with semaphore
                tasks = []
                for table in tables:
                    task = asyncio.create_task(
                        self._scan_table_with_progress(table, patterns, options, progress, main_task)
                    )
                    tasks.append(task)
                
                # Wait for all tasks to complete
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Collect results
                for result in results:
                    if isinstance(result, Exception):
                        self.console.print(f"❌ Error in scan task: {result}")
                        self._metrics['connection_errors'] += 1
                    else:
                        matches.extend(result)
                        
        except Exception as e:
            error_msg = str(e)
            # Only exit on critical errors
            if any(code in error_msg for code in ['ORA-01017', 'ORA-12514', 'TNS-12541', 'ORA-12547', 'ORA-12170']):
                self.console.print(f"❌ Critical error during scan: {e}")
                import sys
                sys.exit(1)
            else:
                self.console.print(f"⚠️ Minor error during scan, continuing: {e}")
        finally:
            await self.disconnect()
            
        # Final progress report with comprehensive metrics
        scan_end_time = time.time()
        total_time = scan_end_time - scan_start_time
        
        self.console.print(f"🎉 Scan completed in {total_time:.1f}s - Found {len(matches)} total matches")
        
        # Print performance metrics
        await self._print_performance_metrics()
        
        return matches

    async def _scan_table_with_progress(self, table: str, patterns: List[Any], options: ScanOptions, 
                                      progress: Progress, main_task) -> List[Dict[str, Any]]:
        """Scan a single table with progress tracking."""
        async with self._semaphore:
            try:
                matches = []
                async for match in self._scan_table_streaming(table, patterns, options):
                    matches.append(match)
                
                # Update progress
                progress.advance(main_task)
                self._scan_progress['completed'] += 1
                self._metrics['tables_completed'] += 1
                
                # Update description with current progress
                progress.update(
                    main_task,
                    description=f"[cyan]Scanned {self._scan_progress['completed']}/{self._scan_progress['total']} tables - {table}: {len(matches)} matches"
                )
                
                return matches
                
            except Exception as e:
                self.console.print(f"⚠️ Skipping {table}: {e}")
                self._metrics['connection_errors'] += 1
                self._metrics['tables_skipped'] += 1
                progress.advance(main_task)
                return []

    def _compile_patterns(self, patterns: List[Any]) -> Dict[str, re.Pattern]:
        """Compile regex patterns for optimal performance."""
        compiled = {}
        for pattern in patterns:
            if pattern.name not in self._compiled_patterns:
                self._compiled_patterns[pattern.name] = re.compile(pattern.regex, re.IGNORECASE)
            compiled[pattern.name] = self._compiled_patterns[pattern.name]
        return compiled

    def _batch_match_patterns(self, value: str, compiled_patterns: Dict[str, re.Pattern]) -> List[Dict[str, Any]]:
        """Match patterns against a value."""
        matches = []
        for pattern_name, compiled_regex in compiled_patterns.items():
            if compiled_regex.search(value):
                matches.append({
                    'pattern_name': pattern_name,
                    'matched_value': value
                })
        return matches

    def _optimized_pattern_matching(self, value: str, compiled_patterns: Dict[str, re.Pattern]) -> List[Dict[str, Any]]:
        """Optimized pattern matching with caching and early termination."""
        if not self._pattern_optimization:
            return self._batch_match_patterns(value, compiled_patterns)
            
        matches = []
        
        # Early termination check
        if self._early_termination_check(value, compiled_patterns):
            self._metrics['early_terminations'] += 1
            return matches
            
        # Value caching
        value_hash = self._hash_value(value)
        if self._is_value_cached(value_hash):
            cached_result = self._get_cached_result(value_hash)
            if cached_result is not None:
                self._metrics['cache_hits'] += 1
                return cached_result
        else:
            self._metrics['cache_misses'] += 1
            
        # Pattern matching with optimizations
        for pattern_name, compiled_regex in compiled_patterns.items():
            # Pattern-specific optimizations
            if pattern_name == 'credit_card':
                if len(value) < 13 or len(value) > 19:
                    continue
                if not any(c.isdigit() for c in value):
                    continue
            elif pattern_name == 'email':
                if '@' not in value or '.' not in value:
                    continue
                if len(value) < 5 or len(value) > 254:
                    continue
            elif pattern_name == 'ssn':
                if len(value) < 9 or len(value) > 11:
                    continue
                if not any(c.isdigit() for c in value):
                    continue
                    
            if compiled_regex.search(value):
                matches.append({
                    'pattern_name': pattern_name,
                    'matched_value': value
                })
                if not self.config.get('show_all', False):
                    break
                    
        self._cache_value_result(value_hash, matches)
        return matches

    async def _optimized_batch_processing(self, rows: List[Tuple], cols: List[Tuple[str, str]], 
                                        compiled_patterns: Dict[str, re.Pattern], table: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Optimized batch processing with async generator."""
        if not self._batch_optimization:
            for row in rows:
                for col, val in zip(cols, row):
                    if val is None:
                        continue
                    str_val = str(val)
                    matches = self._batch_match_patterns(str_val, compiled_patterns)
                    for match in matches:
                        yield {
                            'path': f"{table}.{col[0]}",
                            'value': str_val,
                            'table': table,
                            'column': col[0],
                            'full_value': str_val,
                            'rule': match['pattern_name'],
                            'data_type': 'text'
                        }
            return
            
        processed_values = set()
        for row in rows:
            for col_name, data_type in cols:
                if self._should_skip_column(col_name, data_type, table):
                    self._metrics['column_skips'] += 1
                    continue
                    
                val = row[cols.index((col_name, data_type))]
                if val is None:
                    continue
                    
                str_val = str(val)
                if str_val in processed_values:
                    continue
                    
                processed_values.add(str_val)
                matches = self._optimized_pattern_matching(str_val, compiled_patterns)
                
                for match in matches:
                    self._metrics['total_matches_found'] += 1
                    yield {
                        'path': f"{table}.{col_name}",
                        'value': str_val,
                        'table': table,
                        'column': col_name,
                        'full_value': str_val,
                        'rule': match['pattern_name'],
                        'data_type': 'text'
                    }

    async def _get_valid_columns(self, owner: str, table_name: str, options: ScanOptions = None) -> List[Tuple[str, str]]:
        """Get valid columns for scanning - async version."""
        async with self._get_connection() as conn:
            loop = asyncio.get_event_loop()
            cursor = conn.cursor()
            
            try:
                is_credit_card_scan = False
                if options and options.pattern:
                    is_credit_card_scan = options.pattern.lower() in ['creditcard', 'credit-card', 'credit_card']
                elif options and options.only_patterns:
                    is_credit_card_scan = any('credit' in p.lower() for p in options.only_patterns)
                    
                if is_credit_card_scan:
                    await loop.run_in_executor(
                        None,
                        lambda: cursor.execute("""
                            SELECT column_name, data_type
                            FROM   all_tab_columns
                            WHERE  owner = :o AND table_name = :t
                              AND  data_type IN (
                                  'CHAR','VARCHAR','VARCHAR2','CLOB','NCHAR','NVARCHAR2','NCLOB'
                              )
                        """, o=owner, t=table_name)
                    )
                else:
                    await loop.run_in_executor(
                        None,
                        lambda: cursor.execute("""
                            SELECT column_name, data_type
                            FROM   all_tab_columns
                            WHERE  owner = :o AND table_name = :t
                              AND  data_type IN (
                                  'CHAR','VARCHAR','VARCHAR2','CLOB','NCHAR','NVARCHAR2','NCLOB',
                                  'NUMBER','FLOAT','DECIMAL','NUMERIC'
                              )
                        """, o=owner, t=table_name)
                    )
                    
                columns = await loop.run_in_executor(None, cursor.fetchall)
                columns = [(row[0], row[1]) for row in columns]
                
                if self._column_optimization:
                    columns = self._optimize_column_order(columns)
                    
                return columns
                
            finally:
                await loop.run_in_executor(None, cursor.close)

    def _should_skip_column(self, column_name: str, data_type: str, table_name: str) -> bool:
        """Check if column should be skipped based on optimization rules."""
        if not self._column_optimization:
            return False
            
        skip_patterns = [
            r'^ID$', r'^PK_', r'^FK_', r'_ID$',
            r'^CREATED_', r'^UPDATED_', r'^MODIFIED_',
            r'^VERSION$', r'^STATUS$', r'^FLAG$',
            r'^DELETED$', r'^ACTIVE$', r'^ENABLED$',
            r'^SORT_', r'^ORDER_', r'^SEQ_',
            r'^TEMP_', r'^TMP_', r'^BKP_',
        ]
        
        for pattern in skip_patterns:
            if re.match(pattern, column_name, re.IGNORECASE):
                return True
                
        if data_type in ['NUMBER', 'FLOAT', 'DECIMAL', 'NUMERIC']:
            return True
            
        return False

    def _optimize_column_order(self, columns: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
        """Optimize column order based on likelihood of containing sensitive data."""
        if not self._column_optimization:
            return columns
            
        column_scores = []
        for col_name, data_type in columns:
            score = 0
            
            # High probability patterns
            high_prob_patterns = [
                r'CARD', r'CREDIT', r'DEBIT', r'PAYMENT',
                r'SSN', r'SOCIAL', r'TAX',
                r'EMAIL', r'MAIL', r'ADDRESS',
                r'PHONE', r'TEL', r'MOBILE',
                r'PASSWORD', r'PASS', r'SECRET', r'KEY',
                r'NAME', r'FIRST', r'LAST', r'FULL'
            ]
            for pattern in high_prob_patterns:
                if re.search(pattern, col_name, re.IGNORECASE):
                    score += 10
                    
            # Medium probability patterns
            medium_prob_patterns = [
                r'USER', r'CUSTOMER', r'CLIENT',
                r'ACCOUNT', r'BANK', r'FINANCIAL',
                r'PERSONAL', r'PRIVATE', r'CONFIDENTIAL'
            ]
            for pattern in medium_prob_patterns:
                if re.search(pattern, col_name, re.IGNORECASE):
                    score += 5
                    
            if data_type in ['VARCHAR2', 'VARCHAR', 'CHAR', 'CLOB']:
                score += 3
                
            column_scores.append((col_name, data_type, score))
            
        column_scores.sort(key=lambda x: x[2], reverse=True)
        return [(col_name, data_type) for col_name, data_type, _ in column_scores]

    def _early_termination_check(self, value: str, patterns: Dict[str, re.Pattern]) -> bool:
        """Check if value should be skipped early based on characteristics."""
        if not self._early_termination:
            return False
            
        value_len = len(value)
        
        # Keep values that look like emails or SSNs
        if ('@' in value and '.' in value) or ('-' in value and value_len in (9, 11)):
            return False
            
        # Skip very short or very long values
        if value_len < 10:
            return True
        if value_len > 1000:
            return True
            
        # Skip short numeric values
        if value.isdigit() and value_len < 13:
            return True
            
        # Skip values without digits
        if not any(c.isdigit() for c in value):
            return True
            
        return False

    def _hash_value(self, value: str) -> str:
        """Hash value for caching."""
        return hashlib.md5(value.encode('utf-8')).hexdigest()

    def _is_value_cached(self, value_hash: str) -> bool:
        """Check if value is cached."""
        if not self._value_caching:
            return False
        return value_hash in self._value_cache

    def _cache_value_result(self, value_hash: str, matches: List[Dict[str, Any]]):
        """Cache value matching result."""
        if not self._value_caching:
            return
        self._value_cache[value_hash] = matches

    def _get_cached_result(self, value_hash: str) -> Optional[List[Dict[str, Any]]]:
        """Get cached result for value."""
        if not self._value_caching:
            return None
        return self._value_cache.get(value_hash)

    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024

    def _adjust_batch_size(self, current_batch_size: int, batch_time: float) -> int:
        """Adjust batch size based on performance."""
        if not self._adaptive_batch:
            return current_batch_size
            
        target_time = 0.5
        if batch_time > target_time * 1.5:
            new_size = max(self._min_batch_size, int(current_batch_size * 0.8))
        elif batch_time < target_time * 0.5:
            new_size = min(self._max_batch_size, int(current_batch_size * 1.2))
        else:
            new_size = current_batch_size
        return new_size

    async def _monitor_memory(self):
        """Monitor memory usage and trigger garbage collection if needed."""
        memory_mb = self._get_memory_usage()
        self._metrics['memory_usage'].append(memory_mb)
        
        if memory_mb > 1024:
            self.console.print(f"🧹 High memory usage detected ({memory_mb:.1f}MB), triggering garbage collection...")
            gc.collect()
            new_memory_mb = self._get_memory_usage()
            self.console.print(f"🧹 Memory after GC: {new_memory_mb:.1f}MB (freed {memory_mb - new_memory_mb:.1f}MB)")

    def _should_retry(self, exception: Exception) -> bool:
        """Check if exception should trigger a retry."""
        error_msg = str(exception)
        retryable_errors = [
            'ORA-12541', 'ORA-12547', 'ORA-12170',
            'ORA-12514', 'ORA-12505',
            'ORA-03113', 'ORA-03114',
            'ORA-00028', 'ORA-00068'
        ]
        return any(error in error_msg for error in retryable_errors)

    async def _scan_table_streaming(self, table: str, patterns: List[Any], options: ScanOptions = None) -> AsyncGenerator[Dict[str, Any], None]:
        """Stream scan results from a single table - async version."""
        # Pre-compile patterns for optimal performance
        compiled_patterns = self._compile_patterns(patterns)
        
        try:
            # Parse table name
            if '"' in table:
                parts = table.split('"."')
                owner = parts[0].strip('"')
                table_name = parts[1].strip('"')
            else:
                owner = self._user.upper()
                table_name = table
            
            # Get valid columns with optimization
            cols = await self._get_valid_columns(owner, table_name, options)
            if not cols:
                return
            
            # Use connection from pool
            async with self._get_connection() as conn:
                loop = asyncio.get_event_loop()
                col_names = [col[0] for col in cols]
                col_list = ", ".join(f'"{c}"' for c in col_names)
                data_cur = conn.cursor()
                
                try:
                    # Execute query
                    await loop.run_in_executor(
                        None,
                        lambda: data_cur.execute(f"SELECT {col_list} FROM {table}")
                    )
                    
                    # Algorithmically optimized batch processing
                    batch_count = 0
                    current_batch_size = self._fetch_size
                    
                    while True:
                        batch_start_time = time.time()
                        
                        # Fetch batch of rows
                        rows = await loop.run_in_executor(
                            None,
                            lambda: data_cur.fetchmany(current_batch_size)
                        )
                        
                        if not rows:
                            break
                        
                        batch_count += 1
                        rows_count = len(rows)
                        self._metrics['total_rows_processed'] += rows_count
                        
                        # Process batch with optimized matching
                        batch_matches = 0
                        async for match in self._optimized_batch_processing(rows, cols, compiled_patterns, table):
                            batch_matches += 1
                            yield match
                        
                        # Calculate batch performance
                        batch_time = time.time() - batch_start_time
                        self._metrics['batch_times'].append(batch_time)
                        
                        # Adjust batch size based on performance
                        if self._adaptive_batch and batch_count % 5 == 0:
                            current_batch_size = self._adjust_batch_size(current_batch_size, batch_time)
                        
                        # Memory monitoring
                        if batch_count % 10 == 0:
                            await self._monitor_memory()
                
                finally:
                    await loop.run_in_executor(None, data_cur.close)
                    
        except Exception as exc:
            error_msg = str(exc)
            
            # Skip specific Oracle errors
            if any(code in error_msg for code in ['ORA-00942', 'ORA-01031', 'ORA-03113']):
                self.console.print(f"⚠️ Skipping {table}: {error_msg}")
                self._metrics['connection_errors'] += 1
                return
            
            # Retry logic for transient errors
            if self._should_retry(exc):
                self.console.print(f"🔄 Retrying {table} due to transient error...")
                self._metrics['retry_count'] += 1
                await asyncio.sleep(self._retry_delay)
                async for match in self._scan_table_streaming(table, patterns, options):
                    yield match
            else:
                self.console.print(f"❌ Error scanning {table}: {exc}")
                self._metrics['connection_errors'] += 1

    async def _print_performance_metrics(self):
        """Print comprehensive performance metrics."""
        metrics = self._get_performance_metrics()
        
        # Create metrics table
        table = Table(title="📊 Performance Metrics")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Tables Completed", f"{metrics['tables_completed']:,}")
        table.add_row("Tables Skipped", f"{metrics['tables_skipped']:,}")
        table.add_row("Rows Processed", f"{metrics['total_rows_processed']:,}")
        table.add_row("Matches Found", f"{metrics['total_matches_found']:,}")
        
        if metrics.get('rows_per_second'):
            table.add_row("Rows/Second", f"{metrics['rows_per_second']:,.0f}")
        if metrics.get('matches_per_second'):
            table.add_row("Matches/Second", f"{metrics.get('matches_per_second', 0):,.0f}")
            
        table.add_row("Memory Usage", f"{metrics.get('current_memory_usage', 0):.1f}MB")
        if metrics.get('max_memory_usage'):
            table.add_row("Max Memory", f"{metrics['max_memory_usage']:.1f}MB")
            
        if metrics.get('cache_hit_rate'):
            table.add_row("Cache Hit Rate", f"{metrics['cache_hit_rate']:.1%}")
        if metrics.get('early_termination_rate'):
            table.add_row("Early Termination Rate", f"{metrics['early_termination_rate']:.1%}")
        if metrics.get('column_skip_rate'):
            table.add_row("Column Skip Rate", f"{metrics['column_skip_rate']:.1%}")
            
        table.add_row("Connection Errors", str(metrics['connection_errors']))
        table.add_row("Retry Attempts", str(metrics['retry_count']))
        
        if metrics.get('avg_batch_time'):
            table.add_row("Avg Batch Time", f"{metrics['avg_batch_time']:.3f}s")
            
        self.console.print(table)

    def _get_performance_metrics(self) -> Dict[str, Any]:
        """Get comprehensive performance metrics."""
        metrics = self._metrics.copy()
        
        # Calculate averages
        if metrics['batch_times']:
            metrics['avg_batch_time'] = sum(metrics['batch_times']) / len(metrics['batch_times'])
            metrics['max_batch_time'] = max(metrics['batch_times'])
            metrics['min_batch_time'] = min(metrics['batch_times'])
        
        if metrics['memory_usage']:
            metrics['avg_memory_usage'] = sum(metrics['memory_usage']) / len(metrics['memory_usage'])
            metrics['max_memory_usage'] = max(metrics['memory_usage'])
            metrics['current_memory_usage'] = self._get_memory_usage()
        
        # Calculate algorithmic optimization metrics
        total_cache_operations = metrics['cache_hits'] + metrics['cache_misses']
        if total_cache_operations > 0:
            metrics['cache_hit_rate'] = metrics['cache_hits'] / total_cache_operations
        
        total_values_processed = metrics['total_rows_processed'] * 10  # Estimate
        if total_values_processed > 0:
            metrics['early_termination_rate'] = metrics['early_terminations'] / total_values_processed
            metrics['column_skip_rate'] = metrics['column_skips'] / total_values_processed
        
        # Calculate throughput
        if self._scan_progress['start_time']:
            total_time = time.time() - self._scan_progress['start_time']
            metrics['rows_per_second'] = metrics['total_rows_processed'] / total_time if total_time > 0 else 0
            metrics['matches_per_second'] = metrics['total_matches_found'] / total_time if total_time > 0 else 0
        
        return metrics

    # Compatibility methods for sync interface
    def connect_sync(self) -> None:
        """Sync wrapper for connect method."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.connect())
        finally:
            loop.close()

    def disconnect_sync(self) -> None:
        """Sync wrapper for disconnect method."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.disconnect())
        finally:
            loop.close()

    def scan_sync(self, options: ScanOptions) -> List[Dict[str, Any]]:
        """Sync wrapper for scan method."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self.scan(options))
        finally:
            loop.close()

    def init(self, url: str) -> None:
        """Initialize adapter with URL - compatibility method."""
        # URL is already set in __init__, this is for compatibility
        pass

    def object_name(self) -> str:
        """Get object name for this adapter - compatibility method."""
        return "table" 