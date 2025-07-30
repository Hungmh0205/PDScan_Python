"""Output formatters for PDScan."""

from typing import List, Dict, Any
from .format import Formatter
import sqlite3

def get_formatter(format_type: str, output_path=None) -> Formatter:
    """Get formatter for specified format type."""
    if format_type == 'json':
        return JSONFormatter()
    elif format_type == 'csv':
        return CSVFormatter()
    elif format_type == 'sqlite':
        return SQLiteFormatter(output_path)
    return TextFormatter()

class TextFormatter(Formatter):
    def format(self, matches: List[Dict[str, Any]]) -> str:
        if not matches:
            return "No matches found."
        return "\n".join(f"{m['pattern']}: {m['value']}" for m in matches)

class JSONFormatter(Formatter):
    def format(self, matches: List[Dict[str, Any]]) -> str:
        import json
        return json.dumps(matches, indent=2)

class CSVFormatter(Formatter):
    def format(self, matches: List[Dict[str, Any]]) -> str:
        import csv
        from io import StringIO
        output = StringIO()
        
        # Enhanced fieldnames with more details
        fieldnames = ['table', 'column', 'value', 'rule', 'data_type', 'path']
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        
        for match in matches:
            # Extract table and column from path if available
            table = ''
            column = ''
            if 'path' in match:
                path_parts = match['path'].split('.')
                if len(path_parts) >= 2:
                    table = '.'.join(path_parts[:-1])  # Everything except last part
                    column = path_parts[-1]  # Last part is column
            
            writer.writerow({
                'table': table,
                'column': column,
                'value': match.get('value', '')[:200],  # Truncate long values
                'rule': match.get('rule', ''),
                'data_type': match.get('data_type', 'text'),
                'path': match.get('path', '')
            })
        
        return output.getvalue()

    def print_match(self, match, show_data=False, row_name=None):
        # In ra một dòng CSV đơn giản cho 1 match (hoặc chỉ pass nếu không dùng)
        import csv
        import sys
        fieldnames = ['table', 'column', 'value', 'rule', 'data_type', 'path']
        table = ''
        column = ''
        if hasattr(match, 'path'):
            path_parts = match.path.split('.')
            if len(path_parts) >= 2:
                table = '.'.join(path_parts[:-1])
                column = path_parts[-1]
        row = {
            'table': table,
            'column': column,
            'value': getattr(match, 'value', '')[:200],
            'rule': getattr(match, 'rule', ''),
            'data_type': getattr(match, 'data_type', 'text'),
            'path': getattr(match, 'path', '')
        }
        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        writer.writerow(row)

class SQLiteFormatter(Formatter):
    def __init__(self, output_path=None):
        self.output_path = output_path or "pdscan_results.sqlite"

    def format(self, matches: List[Dict[str, Any]]) -> str:
        conn = sqlite3.connect(self.output_path)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT,
                column_name TEXT,
                value TEXT,
                rule TEXT,
                data_type TEXT,
                path TEXT
            )
        """)
        for match in matches:
            table = ''
            column = ''
            if 'path' in match:
                path_parts = match['path'].split('.')
                if len(path_parts) >= 2:
                    table = '.'.join(path_parts[:-1])
                    column = path_parts[-1]
            c.execute("""
                INSERT INTO results (table_name, column_name, value, rule, data_type, path)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                table,
                column,
                match.get('value', '')[:200],
                match.get('rule', ''),
                match.get('data_type', 'text'),
                match.get('path', '')
            ))
        conn.commit()
        conn.close()
        return f"Results saved to SQLite DB: {self.output_path}"

    def print_match(self, match, show_data=False, row_name=None):
        pass 