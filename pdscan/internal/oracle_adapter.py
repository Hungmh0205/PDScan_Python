"""
Oracle adapter implementation - Algorithmically optimized version (cleaned)
"""

from typing import List, Any, Optional, Dict, Generator, Iterator, Set, Tuple
import cx_Oracle
import time
from urllib.parse import urlparse
import re
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from contextlib import contextmanager
import threading
import psutil
import gc
import hashlib

from .data_store_adapter import Adapter
from .scan_opts import ScanOptions
from .match_finder import MatchFinder

class OracleAdapter(Adapter):
    """Algorithmically optimized Oracle adapter (cleaned)"""
    
    def __init__(self, url: str, config: Optional[dict] = None):
        super().__init__(url)
        self.conn = None
        self.config = config or {}
        # --- Pool & parallelism ---
        self._max_workers = self.config.get('max_workers', 8)
        self._fetch_size = self.config.get('fetch_size', 10000)
        self._table_timeout = self.config.get('table_timeout', 120)
        self._pool_min = self.config.get('pool_min', 2)
        self._pool_max = self.config.get('pool_max', 10)
        self._pool_increment = self.config.get('pool_increment', 1)
        self._pool = None
        # --- Schema & skip ---
        self._skip_schemas = self.config.get('skip_schemas', {"SYS", "SYSTEM", "XDB", "OUTLN", "ORDSYS", "CTXSYS", "WMSYS"})
        self._target_schema = self.config.get('target_schema', '')
        # --- Algorithmic optimization flags ---
        self._early_termination = self.config.get('early_termination', True)
        self._value_caching = self.config.get('value_caching', True)
        self._column_optimization = self.config.get('column_optimization', True)
        self._batch_optimization = self.config.get('batch_optimization', True)
        self._pattern_optimization = self.config.get('pattern_optimization', True)
        self._adaptive_batch = self.config.get('adaptive_batch', True)
        self._min_batch_size = self.config.get('min_batch_size', 1000)
        self._max_batch_size = self.config.get('max_batch_size', 50000)
        # --- Retry & resilience ---
        self._retry_attempts = self.config.get('retry_attempts', 3)
        self._retry_delay = self.config.get('retry_delay', 1)
        self._timeout = self.config.get('timeout', 30)
        # --- Caching & metrics ---
        self._compiled_patterns = {}
        self._pattern_cache_lock = threading.Lock()
        self._value_cache = {}
        self._value_cache_lock = threading.Lock()
        self._column_stats = {}
        self._column_stats_lock = threading.Lock()
        self._scan_progress = {'completed': 0, 'total': 0, 'start_time': None}
        self._progress_lock = threading.Lock()
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
            'retry_count': 0
        }
        self._metrics_lock = threading.Lock()
        # --- Connection info ---
        parsed = urlparse(url)
        self._user = self.config.get('user') or parsed.username
        self._password = self.config.get('password') or parsed.password
        self._dsn = self.config.get('dsn')
        self._host = self.config.get('host') or parsed.hostname
        self._port = self.config.get('port', 1521) or (parsed.port or 1521)
        self._service_name = self.config.get('service_name') or parsed.path.lstrip('/')

    @classmethod
    def from_config(cls, db_config: dict):
        return cls(db_config['url'], db_config)

    def connect(self) -> None:
        """Connect to Oracle with connection pooling and retry."""
        attempt = 0
        dsn = self._build_dsn()
        while attempt < self._retry_attempts:
            try:
                self._pool = cx_Oracle.SessionPool(
                    user=self._user,
                    password=self._password,
                    dsn=dsn,
                    min=self._pool_min,
                    max=self._pool_max,
                    increment=self._pool_increment,
                    encoding="UTF-8"
                )
                with self._get_connection() as conn:
                    conn.cursor().execute("SELECT 1 FROM DUAL")
                print(f"✅ Connected to Oracle with connection pool (min={self._pool_min}, max={self._pool_max})")
                return
            except cx_Oracle.Error as e:
                attempt += 1
                if attempt >= self._retry_attempts:
                    raise e
                print(f"Oracle connection attempt {attempt} failed, retrying in {self._retry_delay}s...")
                time.sleep(self._retry_delay)
                self._retry_delay *= 2

    def disconnect(self) -> None:
        if self._pool:
            self._pool.close()
            self._pool = None
        if self.conn:
            self.conn.close()
            self.conn = None

    @contextmanager
    def _get_connection(self):
        conn = None
        try:
            conn = self._pool.acquire()
            yield conn
        finally:
            if conn:
                self._pool.release(conn)

    def fetch_tables(self) -> List[str]:
        """Fetch tables with SELECT privilege only."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                if self._target_schema:
                    cursor.execute("""
                        SELECT DISTINCT c.owner, c.table_name
                        FROM all_tab_columns c
                        JOIN user_tab_privs p
                          ON c.owner = p.owner AND c.table_name = p.table_name
                        WHERE p.privilege = 'SELECT' AND c.owner = :schema
                    """, schema=self._target_schema.upper())
                else:
                    skip_schemas_list = ",".join(f"'{s}'" for s in self._skip_schemas)
                    cursor.execute(f"""
                        SELECT DISTINCT c.owner, c.table_name
                        FROM all_tab_columns c
                        JOIN user_tab_privs p
                          ON c.owner = p.owner AND c.table_name = p.table_name
                        WHERE p.privilege = 'SELECT' AND c.owner NOT IN ({skip_schemas_list})
                    """)
                tables = cursor.fetchall()
                return [f'"{owner}"."{table}"' for owner, table in tables]
            finally:
                cursor.close()

    def fetch_names(self) -> List[str]:
        return self.fetch_tables()

    def _get_items(self) -> List[str]:
        return self.fetch_tables()

    def _get_values(self, table: str, options: ScanOptions) -> List[str]:
        values = []
        try:
            data = self.fetch_table_data(table, options)
            for item in data:
                values.append(item['value'])
        except Exception:
            values.append(table)
        return values

    def fetch_table_data(self, table: str, options: ScanOptions = None) -> List[dict]:
        if options is None:
            options = ScanOptions()
        return list(self._scan_table_streaming(table, self.match_finder.get_patterns(options), options))

    def scan(self, options: ScanOptions) -> List[Dict[str, Any]]:
        """Main scan method with streaming, pooling, progress, metrics."""
        matches = []
        scan_start_time = time.time()
        try:
            self.connect()
            tables = self.fetch_tables()
            print(f"Found {len(tables)} tables to scan")
            with self._progress_lock:
                self._scan_progress = {
                    'completed': 0,
                    'total': len(tables),
                    'start_time': scan_start_time
                }
            patterns = self.match_finder.get_patterns(options)
            print(f"Using {len(patterns)} patterns for scanning")
            with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
                futures = {
                    executor.submit(self._scan_table_streaming, table, patterns, options): table 
                    for table in tables
                }
                for future in as_completed(futures):
                    table = futures[future]
                    try:
                        table_matches = []
                        for match in future.result(timeout=self._table_timeout):
                            matches.append(match)
                            table_matches.append(match)
                        self._update_progress(table, len(table_matches))
                    except TimeoutError:
                        print(f"⏱️  Timeout >{self._table_timeout}s: Skipping {table}")
                        self._update_progress(table, 0)
                    except Exception as e:
                        print(f"⚠️  Error processing {table}: {e}")
                        self._update_progress(table, 0)
        except Exception as e:
            error_msg = str(e)
            if any(code in error_msg for code in ['ORA-01017', 'ORA-12514', 'TNS-12541', 'ORA-12547', 'ORA-12170']):
                print(f"❌ Critical error during scan: {e}")
                import sys
                sys.exit(1)
            else:
                print(f"⚠️  Minor error during scan, continuing: {e}")
        finally:
            self.disconnect()
        scan_end_time = time.time()
        total_time = scan_end_time - scan_start_time
        print(f"🎉 Scan completed in {total_time:.1f}s - Found {len(matches)} total matches")
        metrics = self.get_performance_metrics()
        print(f"📈 Performance Metrics:")
        print(f"   • Rows processed: {metrics['total_rows_processed']:,}")
        print(f"   • Matches found: {metrics['total_matches_found']:,}")
        print(f"   • Rows/second: {metrics.get('rows_per_second', 0):,.0f}")
        print(f"   • Matches/second: {metrics.get('matches_per_second', 0):,.0f}")
        print(f"   • Memory usage: {metrics.get('current_memory_usage', 0):.1f}MB (max: {metrics.get('max_memory_usage', 0):.1f}MB)")
        print(f"   • Cache hit rate: {metrics.get('cache_hit_rate', 0):.1%}")
        print(f"   • Early termination rate: {metrics.get('early_termination_rate', 0):.1%}")
        print(f"   • Column skip rate: {metrics.get('column_skip_rate', 0):.1%}")
        print(f"   • Connection errors: {metrics['connection_errors']}")
        print(f"   • Retry attempts: {metrics['retry_count']}")
        if metrics.get('avg_batch_time'):
            print(f"   • Avg batch time: {metrics['avg_batch_time']:.3f}s")
        return matches

    # ========== PATTERN & BATCH OPTIMIZATION ========== #
    def _compile_patterns(self, patterns: List[Any]) -> Dict[str, re.Pattern]:
        with self._pattern_cache_lock:
            compiled = {}
            for pattern in patterns:
                if pattern.name not in self._compiled_patterns:
                    self._compiled_patterns[pattern.name] = re.compile(pattern.regex, re.IGNORECASE)
                compiled[pattern.name] = self._compiled_patterns[pattern.name]
            return compiled

    def _batch_match_patterns(self, value: str, compiled_patterns: Dict[str, re.Pattern]) -> List[Dict[str, Any]]:
        matches = []
        for pattern_name, compiled_regex in compiled_patterns.items():
            if compiled_regex.search(value):
                matches.append({
                    'pattern_name': pattern_name,
                    'matched_value': value
                })
        return matches

    def _optimized_pattern_matching(self, value: str, compiled_patterns: Dict[str, re.Pattern]) -> List[Dict[str, Any]]:
        if not self._pattern_optimization:
            return self._batch_match_patterns(value, compiled_patterns)
        matches = []
        if self._early_termination_check(value, compiled_patterns):
            self._update_metrics(early_terminations=1)
            return matches
        value_hash = self._hash_value(value)
        if self._is_value_cached(value_hash):
            cached_result = self._get_cached_result(value_hash)
            if cached_result is not None:
                self._update_metrics(cache_hits=1)
                return cached_result
        else:
            self._update_metrics(cache_misses=1)
        for pattern_name, compiled_regex in compiled_patterns.items():
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

    def _optimized_batch_processing(self, rows: List[Tuple], cols: List[Tuple[str, str]], 
                                  compiled_patterns: Dict[str, re.Pattern], table: str) -> Generator[Dict[str, Any], None, None]:
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
                    self._update_metrics(column_skips=1)
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
                    self._update_metrics(total_matches_found=1)
                    yield {
                        'path': f"{table}.{col_name}",
                        'value': str_val,
                        'table': table,
                        'column': col_name,
                        'full_value': str_val,
                        'rule': match['pattern_name'],
                        'data_type': 'text'
                    }

    # ========== COLUMN, MEMORY, METRICS, ETC ========== #
    def _get_valid_columns(self, owner: str, table_name: str, options: ScanOptions = None) -> List[Tuple[str, str]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                is_credit_card_scan = False
                if options and options.pattern:
                    is_credit_card_scan = options.pattern.lower() in ['creditcard', 'credit-card', 'credit_card']
                elif options and options.only_patterns:
                    is_credit_card_scan = any('credit' in p.lower() for p in options.only_patterns)
                if is_credit_card_scan:
                    cursor.execute("""
                        SELECT column_name, data_type
                        FROM   all_tab_columns
                        WHERE  owner = :o AND table_name = :t
                          AND  data_type IN (
                              'CHAR','VARCHAR','VARCHAR2','CLOB','NCHAR','NVARCHAR2','NCLOB'
                          )
                    """, o=owner, t=table_name)
                    print(f"  💳 Credit card scan: Only scanning string columns for {owner}.{table_name}")
                else:
                    cursor.execute("""
                        SELECT column_name, data_type
                        FROM   all_tab_columns
                        WHERE  owner = :o AND table_name = :t
                          AND  data_type IN (
                              'CHAR','VARCHAR','VARCHAR2','CLOB','NCHAR','NVARCHAR2','NCLOB',
                              'NUMBER','FLOAT','DECIMAL','NUMERIC'
                          )
                    """, o=owner, t=table_name)
                columns = [(row[0], row[1]) for row in cursor.fetchall()]
                if self._column_optimization:
                    columns = self._optimize_column_order(columns)
                return columns
            finally:
                cursor.close()

    def _should_skip_column(self, column_name: str, data_type: str, table_name: str) -> bool:
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
        if not self._column_optimization:
            return columns
        column_scores = []
        for col_name, data_type in columns:
            score = 0
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
        if not self._early_termination:
            return False
        value_len = len(value)
        if ('@' in value and '.' in value) or ('-' in value and value_len in (9, 11)):
            return False
        if value_len < 10:
            return True
        if value_len > 1000:
            return True
        if value.isdigit() and value_len < 13:
            return True
        if not any(c.isdigit() for c in value):
            return True
        return False

    # ========== CACHING, MEMORY, METRICS ========== #
    def _hash_value(self, value: str) -> str:
        return hashlib.md5(value.encode('utf-8')).hexdigest()

    def _is_value_cached(self, value_hash: str) -> bool:
        if not self._value_caching:
            return False
        with self._value_cache_lock:
            return value_hash in self._value_cache

    def _cache_value_result(self, value_hash: str, matches: List[Dict[str, Any]]):
        if not self._value_caching:
            return
        with self._value_cache_lock:
            self._value_cache[value_hash] = matches

    def _get_cached_result(self, value_hash: str) -> Optional[List[Dict[str, Any]]]:
        if not self._value_caching:
            return None
        with self._value_cache_lock:
            return self._value_cache.get(value_hash)

    def _get_memory_usage(self) -> float:
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024

    def _update_metrics(self, **kwargs):
        with self._metrics_lock:
            for key, value in kwargs.items():
                if key in self._metrics:
                    if isinstance(self._metrics[key], list):
                        self._metrics[key].append(value)
                    else:
                        self._metrics[key] += value

    def _adjust_batch_size(self, current_batch_size: int, batch_time: float) -> int:
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

    def _monitor_memory(self):
        memory_mb = self._get_memory_usage()
        self._update_metrics(memory_usage=memory_mb)
        if memory_mb > 1024:
            print(f"🧹 High memory usage detected ({memory_mb:.1f}MB), triggering garbage collection...")
            gc.collect()
            new_memory_mb = self._get_memory_usage()
            print(f"🧹 Memory after GC: {new_memory_mb:.1f}MB (freed {memory_mb - new_memory_mb:.1f}MB)")

    def _should_retry(self, exception: Exception) -> bool:
        error_msg = str(exception)
        retryable_errors = [
            'ORA-12541', 'ORA-12547', 'ORA-12170',
            'ORA-12514', 'ORA-12505',
            'ORA-03113', 'ORA-03114',
            'ORA-00028', 'ORA-00068'
        ]
        return any(error in error_msg for error in retryable_errors)

    def _update_progress(self, table: str, matches_count: int):
        with self._progress_lock:
            self._scan_progress['completed'] += 1
            if self._scan_progress['start_time']:
                elapsed = time.time() - self._scan_progress['start_time']
                avg_time = elapsed / self._scan_progress['completed']
                remaining = (self._scan_progress['total'] - self._scan_progress['completed']) * avg_time
                progress = (self._scan_progress['completed'] / self._scan_progress['total']) * 100
                memory_mb = self._get_memory_usage()
                print(f"📊 Progress: {progress:.1f}% - {self._scan_progress['completed']}/{self._scan_progress['total']} tables - ETA: {remaining:.0f}s - {table}: {matches_count} matches - Memory: {memory_mb:.1f}MB")

    def get_performance_metrics(self) -> Dict[str, Any]:
        with self._metrics_lock:
            metrics = self._metrics.copy()
            if metrics['batch_times']:
                metrics['avg_batch_time'] = sum(metrics['batch_times']) / len(metrics['batch_times'])
                metrics['max_batch_time'] = max(metrics['batch_times'])
                metrics['min_batch_time'] = min(metrics['batch_times'])
            if metrics['memory_usage']:
                metrics['avg_memory_usage'] = sum(metrics['memory_usage']) / len(metrics['memory_usage'])
                metrics['max_memory_usage'] = max(metrics['memory_usage'])
                metrics['current_memory_usage'] = self._get_memory_usage()
            total_cache_operations = metrics['cache_hits'] + metrics['cache_misses']
            if total_cache_operations > 0:
                metrics['cache_hit_rate'] = metrics['cache_hits'] / total_cache_operations
            total_values_processed = metrics['total_rows_processed'] * 10
            if total_values_processed > 0:
                metrics['early_termination_rate'] = metrics['early_terminations'] / total_values_processed
                metrics['column_skip_rate'] = metrics['column_skips'] / total_values_processed
            if self._scan_progress['start_time']:
                total_time = time.time() - self._scan_progress['start_time']
                metrics['rows_per_second'] = metrics['total_rows_processed'] / total_time if total_time > 0 else 0
                metrics['matches_per_second'] = metrics['total_matches_found'] / total_time if total_time > 0 else 0
            return metrics

    def _build_dsn(self) -> str:
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

    def _scan_table_streaming(self, table: str, patterns: List[Any], options: ScanOptions = None) -> Generator[Dict[str, Any], None, None]:
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
            cols = self._get_valid_columns(owner, table_name, options)
            if not cols:
                return
            
            # Use connection from pool
            with self._get_connection() as conn:
                col_names = [col[0] for col in cols]
                col_list = ", ".join(f'"{c}"' for c in col_names)
                data_cur = conn.cursor()
                
                try:
                    data_cur.execute(f"SELECT {col_list} FROM {table}")
                    
                    # Algorithmically optimized batch processing
                    batch_count = 0
                    current_batch_size = self._fetch_size
                    
                    while True:
                        batch_start_time = time.time()
                        rows = data_cur.fetchmany(current_batch_size)
                        if not rows:
                            break
                        
                        batch_count += 1
                        rows_count = len(rows)
                        self._update_metrics(total_rows_processed=rows_count)
                        
                        # Use optimized batch processing
                        batch_matches = 0
                        for match in self._optimized_batch_processing(rows, cols, compiled_patterns, table):
                            batch_matches += 1
                            yield match
                        
                        # Calculate batch performance
                        batch_time = time.time() - batch_start_time
                        self._update_metrics(batch_times=batch_time)
                        
                        # Adjust batch size based on performance
                        if self._adaptive_batch and batch_count % 5 == 0:
                            current_batch_size = self._adjust_batch_size(current_batch_size, batch_time)
                        
                        # Memory monitoring
                        if batch_count % 10 == 0:
                            self._monitor_memory()
                
                finally:
                    data_cur.close()
                    
        except Exception as exc:
            print(f"⚠️  Skipping {table}: {exc}")
            self._update_metrics(connection_errors=1)
            
            # Retry logic for transient errors
            if self._should_retry(exc):
                print(f"🔄 Retrying {table} due to transient error...")
                self._update_metrics(retry_count=1)
                time.sleep(self._retry_delay)
                yield from self._scan_table_streaming(table, patterns, options)
    
    def _update_progress(self, table: str, matches_count: int):
        """Update scan progress with thread safety and enhanced metrics"""
        with self._progress_lock:
            self._scan_progress['completed'] += 1
            if self._scan_progress['start_time']:
                elapsed = time.time() - self._scan_progress['start_time']
                avg_time = elapsed / self._scan_progress['completed']
                remaining = (self._scan_progress['total'] - self._scan_progress['completed']) * avg_time
                progress = (self._scan_progress['completed'] / self._scan_progress['total']) * 100
                
                # Get current memory usage
                memory_mb = self._get_memory_usage()
                
                print(f"📊 Progress: {progress:.1f}% - {self._scan_progress['completed']}/{self._scan_progress['total']} tables - ETA: {remaining:.0f}s - {table}: {matches_count} matches - Memory: {memory_mb:.1f}MB")
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get comprehensive performance metrics with algorithmic optimizations"""
        with self._metrics_lock:
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
    
    def fetch_table_data(self, table: str, options: ScanOptions = None) -> List[dict]:
        """Fetch data from table - compatibility method"""
        if options is None:
            options = ScanOptions()
        return list(self._scan_table_streaming(table, self.match_finder.get_patterns(options), options))
        
    def fetch_names(self) -> List[str]:
        return self.fetch_tables()
        
    def _get_items(self) -> List[str]:
        """Get tables to scan"""
        return self.fetch_tables()
        
    def _get_values(self, table: str, options: ScanOptions) -> List[str]:
        """Get values from table - optimized version"""
        values = []
        try:
            data = self.fetch_table_data(table, options)
            for item in data:
                values.append(item['value'])
        except Exception:
            values.append(table)
        return values
    
    def scan(self, options: ScanOptions) -> List[Dict[str, Any]]:
        """Algorithmically optimized scan method with advanced optimizations"""
        matches = []
        scan_start_time = time.time()
        
        try:
            # Connect to database with pooling
            self.connect()
            
            # Get all tables
            tables = self.fetch_tables()
            print(f"Found {len(tables)} tables to scan")
            
            # Initialize progress tracking
            with self._progress_lock:
                self._scan_progress = {
                    'completed': 0,
                    'total': len(tables),
                    'start_time': scan_start_time
                }
            
            # Get patterns for matching
            patterns = self.match_finder.get_patterns(options)
            print(f"Using {len(patterns)} patterns for scanning")
            
            # Algorithmically optimized parallel scanning with streaming
            with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
                # Submit all table scanning tasks
                futures = {
                    executor.submit(self._scan_table_streaming, table, patterns, options): table 
                    for table in tables
                }
                
                # Process completed tasks with streaming
                for future in as_completed(futures):
                    table = futures[future]
                    try:
                        # Stream results from each table
                        table_matches = []
                        for match in future.result(timeout=self._table_timeout):
                            matches.append(match)  # Add to main results
                            table_matches.append(match)  # Count for progress
                        
                        # Update progress
                        self._update_progress(table, len(table_matches))
                        
                    except TimeoutError:
                        print(f"⏱️  Timeout >{self._table_timeout}s: Skipping {table}")
                        self._update_progress(table, 0)
                    except Exception as e:
                        print(f"⚠️  Error processing {table}: {e}")
                        self._update_progress(table, 0)
                        
        except Exception as e:
            error_msg = str(e)
            # Only exit on critical errors
            if any(code in error_msg for code in ['ORA-01017', 'ORA-12514', 'TNS-12541', 'ORA-12547', 'ORA-12170']):
                print(f"❌ Critical error during scan: {e}")
                import sys
                sys.exit(1)
            else:
                print(f"⚠️  Minor error during scan, continuing: {e}")
        finally:
            self.disconnect()
            
        # Final progress report with comprehensive metrics
        scan_end_time = time.time()
        total_time = scan_end_time - scan_start_time
        
        print(f"🎉 Scan completed in {total_time:.1f}s - Found {len(matches)} total matches")
        
        # Print performance metrics with algorithmic optimizations
        metrics = self.get_performance_metrics()
        print(f"📈 Performance Metrics:")
        print(f"   • Rows processed: {metrics['total_rows_processed']:,}")
        print(f"   • Matches found: {metrics['total_matches_found']:,}")
        print(f"   • Rows/second: {metrics.get('rows_per_second', 0):,.0f}")
        print(f"   • Matches/second: {metrics.get('matches_per_second', 0):,.0f}")
        print(f"   • Memory usage: {metrics.get('current_memory_usage', 0):.1f}MB (max: {metrics.get('max_memory_usage', 0):.1f}MB)")
        print(f"   • Cache hit rate: {metrics.get('cache_hit_rate', 0):.1%}")
        print(f"   • Early termination rate: {metrics.get('early_termination_rate', 0):.1%}")
        print(f"   • Column skip rate: {metrics.get('column_skip_rate', 0):.1%}")
        print(f"   • Connection errors: {metrics['connection_errors']}")
        print(f"   • Retry attempts: {metrics['retry_count']}")
        if metrics.get('avg_batch_time'):
            print(f"   • Avg batch time: {metrics['avg_batch_time']:.3f}s")
            
        return matches 