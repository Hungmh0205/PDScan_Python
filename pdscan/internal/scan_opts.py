"""Scan options for PDScan."""

from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class ScanOptions:
    """Options for scanning data stores."""
    show_data: bool = False
    show_all: bool = False
    sample_size: Optional[int] = 1000
    processes: Optional[int] = 1
    only_patterns: List[str] = field(default_factory=list)
    except_patterns: List[str] = field(default_factory=list)
    min_count: Optional[int] = 1
    pattern: Optional[str] = None
    debug: bool = False
    format: str = 'text'

    def __init__(self, show_data=False, show_all=False, sample_size=1000, processes=1, only=None, except_=None, min_count=1, pattern=None, debug=False, format='text', only_patterns=None, **kwargs):
        self.show_data = show_data
        self.show_all = show_all
        self.sample_size = sample_size
        self.processes = processes
        self.only = only
        self.except_ = except_
        self.min_count = min_count
        self.pattern = pattern
        self.debug = debug
        self.format = format
        self.only_patterns = only_patterns
        self.except_patterns = None
        for k, v in kwargs.items():
            setattr(self, k, v) 