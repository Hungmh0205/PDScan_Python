"""
Base adapter for file systems
"""

from abc import ABC, abstractmethod
from typing import List, Any

class FileAdapter(ABC):
    """Base adapter for file systems"""
    
    @abstractmethod
    def fetch_files(self) -> List[Any]:
        """Fetch list of files"""
        pass
        
    @abstractmethod
    def find_file_matches(self, file: Any, match_finder: Any) -> None:
        """Find matches in file"""
        pass
        
    @abstractmethod
    def object_name(self) -> str:
        """Get object name for files"""
        pass 