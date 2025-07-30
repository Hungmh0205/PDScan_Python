"""
Rules for matching sensitive data - Enhanced version
"""

from dataclasses import dataclass
from typing import List, Dict, Any
import re

@dataclass
class RuleMatch:
    """A match found by a rule"""
    name: str
    display_name: str
    confidence: str
    location: str
    values: List[str]

class MatchConfig:
    """Configuration for matching - Enhanced with better patterns"""
    
    def __init__(self):
        self.name_rules = [
            {
                "name": "email",
                "display_name": "Email Address",
                "confidence": "high",
                "pattern": r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b"
            },
            {
                "name": "credit-card",
                "display_name": "Credit Card Number",
                "confidence": "high",
                "pattern": (
                    r'\b('
                    r'4\d{3}(?:[\s\.\-_–]?\d{4}){3}'      # Visa  xxxx-xxxx-xxxx-xxxx
                    r'|4\d{15}'                            # Visa  16 số liền
                    r'|5[1-5]\d{2}(?:[\s\.\-_–]?\d{4}){3}' # MasterCard xxxx-xxxx-xxxx-xxxx
                    r'|5[1-5]\d{14}'                       # MasterCard 16 số liền
                    r'|3[47]\d{2}[\s\.\-_–]?\d{6}[\s\.\-_–]?\d{5}'  # AmEx 4-6-5
                    r'|3[47]\d{13}'                        # AmEx 15 số liền
                    r'|6011(?:[\s\.\-_–]?\d{4}){3}'        # Discover xxxx-xxxx-xxxx-xxxx
                    r'|6011\d{12}'                         # Discover 16 số liền
                    r')\b'
                )
            },
            {
                "name": "credit-card-masked",
                "display_name": "Masked Credit Card Number",
                "confidence": "high",
                "pattern": r"\b\d{4}[-\s]?[\*X]{4,8}[-\s]?\d{4}\b"
            },
            {
                "name": "ssn",
                "display_name": "Social Security Number",
                "confidence": "high",
                "pattern": r"\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b"
            },
            {
                "name": "phone",
                "display_name": "Phone Number",
                "confidence": "medium",
                "pattern": r"\b(?:\+\d{1,3}\s?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}\b"
            },
            {
                "name": "ip-address",
                "display_name": "IP Address",
                "confidence": "medium",
                "pattern": r"\b(?:\d{1,3}\.){3}\d{1,3}\b"
            },
            {
                "name": "ipv6",
                "display_name": "IPv6 Address",
                "confidence": "medium",
                "pattern": r"\b(?:[A-F0-9]{1,4}:){7}[A-F0-9]{1,4}\b"
            },
            {
                "name": "url",
                "display_name": "URL",
                "confidence": "medium",
                "pattern": r"https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+"
            },
            {
                "name": "mac-address",
                "display_name": "MAC Address",
                "confidence": "medium",
                "pattern": r"\b(?:[0-9A-Fa-f]{2}[:-]){5}(?:[0-9A-Fa-f]{2})\b"
            },
            {
                "name": "date",
                "display_name": "Date",
                "confidence": "low",
                "pattern": r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b"
            },
            {
                "name": "time",
                "display_name": "Time",
                "confidence": "low",
                "pattern": r"\b\d{1,2}:\d{2}(?::\d{2})?(?:\s?[AP]M)?\b"
            }
        ]
        
        # Enhanced person name patterns
        self.multi_name_rules = [
            {
                "name": "person-name",
                "display_name": "Person Name",
                "confidence": "medium",
                "patterns": [
                    # Vietnamese names
                    r"\b[A-Z][a-zàáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]+ [A-Z][a-zàáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]+\b",
                    # English names - First Last
                    r"\b[A-Z][a-z]+ [A-Z][a-z]+\b",
                    # English names - First M. Last
                    r"\b[A-Z][a-z]+ [A-Z]\. [A-Z][a-z]+\b",
                    # English names - First Middle Last
                    r"\b[A-Z][a-z]+ [A-Z][a-z]+ [A-Z][a-z]+\b",
                    # Names with titles
                    r"\b(?:Mr\.|Mrs\.|Ms\.|Dr\.|Prof\.)\s+[A-Z][a-zàáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]+ [A-Z][a-zàáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]+\b",
                    # Names with suffixes
                    r"\b[A-Z][a-zàáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]+ [A-Z][a-zàáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]+ (?:Jr\.|Sr\.|III|IV)\b"
                ]
            },
            {
                "name": "company-name",
                "display_name": "Company Name",
                "confidence": "low",
                "patterns": [
                    r"\b[A-Z][a-zA-Z\s&.,'-]+(?:Inc\.|Corp\.|LLC|Ltd\.|Company|Co\.)\b",
                    r"\b[A-Z][a-zA-Z\s&.,'-]+(?:Technologies|Technology|Systems|Solutions|Services)\b"
                ]
            }
        ]
        
        # Enhanced token rules
        self.token_rules = [
            {
                "name": "api-key",
                "display_name": "API Key",
                "confidence": "high",
                "tokens": ["api", "key", "secret", "token", "auth", "password", "pwd"]
            },
            {
                "name": "database-connection",
                "display_name": "Database Connection String",
                "confidence": "high",
                "tokens": ["jdbc:", "mysql://", "postgresql://", "mongodb://", "redis://", "oracle://"]
            },
            {
                "name": "file-path",
                "display_name": "File Path",
                "confidence": "low",
                "tokens": ["/home/", "/var/", "C:\\", "D:\\", "/tmp/", "/usr/"]
            }
        ]
        
        # Custom regex rules (can be added dynamically)
        self.regex_rules = []
        
        # Configuration
        self.min_count = 1
        
        # Custom patterns support
        self.custom_patterns = {}
    
    def add_custom_pattern(self, name: str, pattern: str, display_name: str = None, confidence: str = "medium"):
        """Add a custom pattern dynamically"""
        self.custom_patterns[name] = {
            "name": name,
            "display_name": display_name or name.title(),
            "confidence": confidence,
            "pattern": pattern
        }
    
    def remove_custom_pattern(self, name: str):
        """Remove a custom pattern"""
        if name in self.custom_patterns:
            del self.custom_patterns[name]
    
    def get_all_patterns(self) -> List[Dict[str, Any]]:
        """Get all patterns including custom ones"""
        all_patterns = []
        
        # Add name rules
        all_patterns.extend(self.name_rules)
        
        # Add multi-name rules as individual patterns
        for rule in self.multi_name_rules:
            for i, pattern in enumerate(rule["patterns"]):
                all_patterns.append({
                    "name": f"{rule['name']}-{i+1}",
                    "display_name": f"{rule['display_name']} (Pattern {i+1})",
                    "confidence": rule["confidence"],
                    "pattern": pattern
                })
        
        # Add custom patterns
        all_patterns.extend(self.custom_patterns.values())
        
        return all_patterns
    
    def validate_pattern(self, pattern: str) -> bool:
        """Validate if a regex pattern is valid"""
        try:
            re.compile(pattern)
            return True
        except re.error:
            return False 