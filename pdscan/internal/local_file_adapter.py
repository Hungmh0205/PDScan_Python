"""
Local file adapter implementation
"""

import os
import magic
from typing import List, Any
from urllib.parse import urlparse
from urllib.request import url2pathname

from .data_store_adapter import Adapter
from .scan_opts import ScanOptions
from .files import scan_text_file, scan_excel_file, scan_csv_file

class LocalFileAdapter(Adapter):
    """Adapter for local files"""
    
    def __init__(self, url: str):
        super().__init__(url)
        self.path = ""
        
    def connect(self) -> None:
        """Connect to local file system"""
        parsed = urlparse(self.url)
        if parsed.scheme != "file":
            raise ValueError("Invalid file URL scheme")
        if parsed.netloc and parsed.path:
            # Windows: file:///C:/path
            self.path = url2pathname(f"{parsed.netloc}{parsed.path}")
        else:
            self.path = url2pathname(parsed.path)
        if not os.path.exists(self.path):
            raise ValueError(f"Path does not exist: {self.path}")
            
    def disconnect(self) -> None:
        """Disconnect from local file system"""
        pass
        
    def _get_items(self) -> List[str]:
        """Get files to scan"""
        return self.fetch_files()
        
    def _get_values(self, file: str, options: ScanOptions) -> List[str]:
        """Get values from file"""
        values = []
        try:
            mime = magic.Magic(mime=True)
            file_type = mime.from_file(file)
            
            if file_type.startswith("text/"):
                with open(file, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        values.append(line.strip())
            elif file_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
                # For Excel files, we'll just return the filename for now
                values.append(file)
            elif file_type == "text/csv":
                with open(file, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        values.append(line.strip())
        except Exception:
            # If we can't read the file, just return the filename
            values.append(file)
            
        return values
        
    def fetch_files(self) -> List[str]:
        """Fetch list of files"""
        files = []
        for root, _, filenames in os.walk(self.path):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files
        
    def fetch_tables(self) -> List[str]:
        """Fetch list of files (alias for fetch_files)"""
        return self.fetch_files()
        
    def fetch_table_data(self, file: str) -> List[dict]:
        """Fetch data from file"""
        return [{"file": file, "content": self._get_values(file, ScanOptions())[:10]}]
        
    def fetch_names(self) -> List[str]:
        """Fetch list of file names"""
        return [os.path.basename(f) for f in self.fetch_files()]

    def find_file_matches(self, file: str, match_finder: Any) -> None:
        """Find matches in file"""
        mime = magic.Magic(mime=True)
        file_type = mime.from_file(file)
        
        if file_type.startswith("text/"):
            scan_text_file(file, match_finder)
        elif file_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
            scan_excel_file(file, match_finder)
        elif file_type == "text/csv":
            scan_csv_file(file, match_finder)
            
    def object_name(self) -> str:
        """Get object name for files"""
        return "file" 