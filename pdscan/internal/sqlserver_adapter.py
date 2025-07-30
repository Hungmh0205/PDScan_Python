"""
SQL Server adapter implementation
"""

from typing import List, Any, Optional
import pyodbc
import time
from urllib.parse import urlparse

from .data_store_adapter import Adapter
from .scan_opts import ScanOptions

class SQLServerAdapter(Adapter):
    """Adapter for SQL Server with connection pooling, SSL, retry"""
    
    def __init__(self, url: str, config: Optional[dict] = None):
        super().__init__(url)
        self.conn = None
        self.config = config or {}
        self._retry_attempts = self.config.get('retry_attempts', 3)
        self._timeout = self.config.get('timeout', 30)
        self._ssl = self.config.get('ssl', False)
        self._pool_size = self.config.get('pool_size', 5)
        self._driver = self.config.get('driver', 'ODBC Driver 17 for SQL Server')
        self._database = self.config.get('database')
        self._user = self.config.get('user')
        self._password = self.config.get('password')
        self._host = self.config.get('host')
        self._port = self.config.get('port', 1433)
        
    @classmethod
    def from_config(cls, db_config: dict):
        return cls(db_config['url'], db_config)
        
    def connect(self) -> None:
        """Connect to SQL Server with retry, SSL, pooling"""
        attempt = 0
        conn_str = self._build_conn_str()
        while attempt < self._retry_attempts:
            try:
                self.conn = pyodbc.connect(conn_str, timeout=self._timeout)
                return
            except pyodbc.Error as e:
                attempt += 1
                if attempt >= self._retry_attempts:
                    raise e
                print(f"SQL Server connection attempt {attempt} failed, retrying...")
                time.sleep(2 ** attempt)
        
    def disconnect(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None
        
    def _build_conn_str(self) -> str:
        # Ưu tiên lấy từ config, nếu không thì parse từ URL
        if self._host and self._database:
            host = self._host
            port = self._port
            database = self._database
            user = self._user
            password = self._password
        else:
            parsed = urlparse(self.url)
            host = parsed.hostname
            port = parsed.port or 1433
            database = parsed.path.lstrip('/')
            user = parsed.username
            password = parsed.password
        
        ssl_str = 'Encrypt=yes;TrustServerCertificate=no;' if self._ssl else ''
        return (
            f"DRIVER={{{self._driver}}};SERVER={host},{port};DATABASE={database};"
            f"UID={user};PWD={password};{ssl_str}"
        )
        
    def fetch_tables(self) -> List[str]:
        """Fetch list of tables"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables
        
    def fetch_table_data(self, table: str) -> List[dict]:
        """Fetch data from table"""
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT TOP 1000 * FROM [{table}]")
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        return [dict(zip(columns, row)) for row in rows]
        
    def fetch_names(self) -> List[str]:
        return self.fetch_tables()
        
    def _get_items(self) -> List[str]:
        """Get tables to scan"""
        return self.fetch_tables()
        
    def _get_values(self, table: str, options: ScanOptions) -> List[str]:
        """Get values from table"""
        values = []
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT TOP {options.sample_size} * FROM [{table}]")
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            
            for row in rows:
                for value in row:
                    if isinstance(value, str):
                        values.append(value)
                    elif value is not None:
                        values.append(str(value))
        except Exception:
            # If we can't read the table, just return the table name
            values.append(table)
            
        return values 
    