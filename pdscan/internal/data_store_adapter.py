"""
Module data_store_adapter: Định nghĩa interface Adapter cho các nguồn dữ liệu.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from concurrent.futures import ProcessPoolExecutor
from .scan_opts import ScanOptions
from .match_finder import MatchFinder
from .rules import MatchConfig

class Adapter(ABC):
    """Base class cho mọi adapter nguồn dữ liệu."""
    
    def __init__(self, url: str):
        self.url = url
        self.match_finder = MatchFinder(MatchConfig())

    @abstractmethod
    def connect(self) -> None:
        """Connect to the data store."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Disconnect from the data store."""
        pass

    def scan(self, options: ScanOptions) -> List[Dict[str, Any]]:
        """Scan the data store for matches."""
        try:
            self.connect()
            if options.processes and options.processes > 1:
                return self._scan_concurrent(options)
            return self._scan_sequential(options)
        finally:
            self.disconnect()

    def _scan_concurrent(self, options: ScanOptions) -> List[Dict[str, Any]]:
        """Scan using multiple processes."""
        with ProcessPoolExecutor(max_workers=options.processes) as executor:
            futures = []
            for item in self._get_items():
                futures.append(executor.submit(self._scan_item, item, options))
            return [match for future in futures for match in future.result()]

    def _scan_sequential(self, options: ScanOptions) -> List[Dict[str, Any]]:
        """Scan using a single process."""
        matches = []
        for item in self._get_items():
            matches.extend(self._scan_item(item, options))
        return matches

    @abstractmethod
    def _get_items(self) -> List[Any]:
        """Get items to scan (tables, collections, etc.)."""
        pass

    def _scan_item(self, item: Any, options: ScanOptions) -> List[Dict[str, Any]]:
        """Scan a single item for matches."""
        matches = []
        patterns = self.match_finder.get_patterns(options)
        
        for value in self._get_values(item, options):
            if not isinstance(value, str):
                continue
                
            for pattern in patterns:
                if pattern.match(value):
                    match = {
                        'pattern': pattern.name,
                        'value': value,
                        'item': str(item)
                    }
                    if options.show_data:
                        match['context'] = self._get_context(item, value)
                    matches.append(match)
                    
                    if not options.show_all:
                        break

        return matches

    @abstractmethod
    def _get_values(self, item: Any, options: ScanOptions) -> List[str]:
        """Get values to scan from an item."""
        pass

    def _get_context(self, item: Any, value: str) -> Optional[Dict[str, Any]]:
        """Get context for a matched value."""
        return None

    @abstractmethod
    def fetch_tables(self) -> List[Any]:
        """Fetch list of tables"""
        pass
        
    @abstractmethod
    def fetch_table_data(self, table: Any) -> List[dict]:
        """Fetch data from table"""
        pass
        
    @abstractmethod
    def fetch_names(self) -> List[str]:
        """Fetch list of names"""
        pass 