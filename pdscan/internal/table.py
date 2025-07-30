"""
Table interface
"""

from abc import ABC, abstractmethod
from typing import Any

class Table(ABC):
    """Table interface"""
    
    @abstractmethod
    def display_name(self) -> str:
        """Get display name of table"""
        pass 