"""
Format functions
"""

from abc import ABC, abstractmethod
from typing import Any, List

class Formatter(ABC):
    """Base formatter interface"""
    
    @abstractmethod
    def print_match(self, match: Any, show_data: bool, row_name: str) -> None:
        """Print a match"""
        pass

class TextFormatter(Formatter):
    """Text formatter"""
    
    def print_match(self, match: Any, show_data: bool, row_name: str) -> None:
        """Print a match in text format"""
        print(f"\n{match.display_name} ({match.confidence} confidence)")
        print(f"Found in {match.location}")
        
        if show_data:
            print("\nSample data:")
            for value in match.values[:50]:
                print(f"  {value}")

class JsonFormatter(Formatter):
    """JSON formatter"""
    
    def print_match(self, match: Any, show_data: bool, row_name: str) -> None:
        """Print a match in JSON format"""
        import json
        
        data = {
            "name": match.display_name,
            "confidence": match.confidence,
            "location": match.location,
        }
        
        if show_data:
            data["values"] = match.values[:50]
            
        print(json.dumps(data))

# Available formatters
Formatters = {
    "text": TextFormatter(),
    "json": JsonFormatter(),
} 