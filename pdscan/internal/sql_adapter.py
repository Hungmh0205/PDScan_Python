"""
SQL adapter implementation
"""

from typing import List, Any, Optional
from urllib.parse import urlparse
import sqlite3
import psycopg2
import psycopg2.pool
import mysql.connector
import time

from .data_store_adapter import Adapter
from .scan_opts import ScanOptions

class SQLAdapter(Adapter):
    """Adapter for SQL databases (PostgreSQL, MySQL, SQLite)"""
    
    def __init__(self, url: str, config: Optional[dict] = None):
        super().__init__(url)
        self.conn = None
        self.cursor = None
        self.pool = None
        self.config = config or {}
        self._pool_params = {}
        self._retry_attempts = self.config.get('retry_attempts', 2)
        self._timeout = self.config.get('timeout', 30)
        self._ssl = self.config.get('ssl', False)
        self._pool_size = self.config.get('pool_size', 5)

    @classmethod
    def from_config(cls, db_config: dict):
        return cls(db_config['url'], db_config)

    def connect(self) -> None:
        """Connect to SQL database with pooling, SSL, retry"""
        parsed = urlparse(self.url)
        if parsed.scheme not in ["postgresql", "mysql", "sqlite", "mariadb"]:
            raise ValueError("Invalid SQL URL scheme")
        attempt = 0
        while attempt < self._retry_attempts:
            try:
                if parsed.scheme == "postgresql":
                    if not self.pool:
                        self.pool = psycopg2.pool.SimpleConnectionPool(
                            1, self._pool_size,
                            dsn=self.url,
                            connect_timeout=self._timeout,
                            sslmode='require' if self._ssl else 'prefer',
                        )
                    self.conn = self.pool.getconn()
                elif parsed.scheme in ["mysql", "mariadb"]:
                    self.conn = mysql.connector.connect(
                        host=parsed.hostname,
                        port=parsed.port or 3306,
                        user=parsed.username,
                        password=parsed.password,
                        database=parsed.path.lstrip('/'),
                    )
                elif parsed.scheme == "sqlite":
                    self.conn = sqlite3.connect(parsed.path)
                self.cursor = self.conn.cursor()
                return
            except Exception as e:
                attempt += 1
                if attempt >= self._retry_attempts:
                    raise e
                time.sleep(2)

    def disconnect(self) -> None:
        """Disconnect from SQL database"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            if self.pool and hasattr(self.pool, 'putconn'):
                self.pool.putconn(self.conn)
            else:
                self.conn.close()
        self.conn = None
        self.cursor = None
        
    def _get_items(self) -> List[str]:
        """Get tables to scan"""
        return self.fetch_tables()
        
    def _get_values(self, table: str, options: ScanOptions) -> List[str]:
        """Get values from table"""
        values = []
        self.cursor.execute(f"SELECT * FROM {table} LIMIT {options.sample_size}")
        columns = [desc[0] for desc in self.cursor.description]
        
        for row in self.cursor.fetchall():
            for value in row:
                if isinstance(value, str):
                    values.append(value)
                    
        return values
        
    def fetch_tables(self) -> List[str]:
        """Fetch list of tables"""
        if hasattr(self.conn, 'get_dsn_parameters') or 'psycopg2' in str(type(self.conn)):
            self.cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
        elif isinstance(self.conn, mysql.connector.connection.MySQLConnection):
            self.cursor.execute("SHOW TABLES")
        elif isinstance(self.conn, sqlite3.Connection):
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        else:
            raise ValueError("Unsupported database")
            
        return [row[0] for row in self.cursor.fetchall()]
        
    def fetch_table_data(self, table: str) -> List[dict]:
        """Fetch rows from table"""
        self.cursor.execute(f"SELECT * FROM {table}")
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        
    def fetch_names(self) -> List[str]:
        """Fetch list of table names"""
        return self.fetch_tables() 