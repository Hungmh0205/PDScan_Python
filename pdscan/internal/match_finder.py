"""
Module match_finder: logic tìm kiếm pattern, quét dữ liệu theo rule - Enhanced version
"""

import re
from typing import List, Dict, Any, Optional, Pattern
from dataclasses import dataclass
from .scan_opts import ScanOptions

from .rules import RuleMatch, MatchConfig

@dataclass
class Pattern:
    """Pattern for matching sensitive data."""
    name: str
    regex: str
    description: str

    def match(self, value: str) -> bool:
        """Check if value matches pattern."""
        return bool(re.search(self.regex, value))

class MatchFinder:
    """Find matches in data - Enhanced with custom patterns support"""
    
    def __init__(self, match_config: MatchConfig):
        self.match_config = match_config
        self.matches: Dict[str, RuleMatch] = {}
        
        # Compile regex patterns
        for rule in self.match_config.name_rules:
            rule["regex"] = re.compile(rule["pattern"], re.IGNORECASE)
            
        for rule in self.match_config.multi_name_rules:
            rule["regexes"] = [re.compile(p, re.IGNORECASE) for p in rule["patterns"]]
            
        for rule in self.match_config.regex_rules:
            if isinstance(rule["regex"], str):
                rule["regex"] = re.compile(rule["regex"], re.IGNORECASE)
        
        # Compile custom patterns
        for rule in self.match_config.custom_patterns.values():
            rule["regex"] = re.compile(rule["pattern"], re.IGNORECASE)
    
    def check_table_data(self, table: Any, data: List[dict]) -> List[RuleMatch]:
        """Check table data for matches"""
        for row in data:
            for key, value in row.items():
                if not isinstance(value, str):
                    continue
                    
                self.check_line(value, f"{table}.{key}")
                
        return list(self.matches.values())
    
    def check_matches(self, file: str, is_file: bool) -> List[RuleMatch]:
        """Get matches for a file"""
        matches = []
        for match in self.matches.values():
            if match.location.startswith(file):
                matches.append(match)
        return matches
    
    def check_line(self, line: str, location: str) -> None:
        """Check a line for matches - Enhanced with better pattern matching"""
        # Check name rules
        for rule in self.match_config.name_rules:
            for match in rule["regex"].finditer(line):
                self._add_match(rule, match.group(), location)
        
        # Check multi-name rules
        for rule in self.match_config.multi_name_rules:
            for regex in rule["regexes"]:
                for match in regex.finditer(line):
                    self._add_match(rule, match.group(), location)
        
        # Check token rules with improved matching
        for rule in self.match_config.token_rules:
            for token in rule["tokens"]:
                # Improved token matching - look for whole words or patterns
                if self._token_match(line, token):
                    self._add_match(rule, line, location)
        
        # Check regex rules
        for rule in self.match_config.regex_rules:
            for match in rule["regex"].finditer(line):
                self._add_match(rule, match.group(), location)
        
        # Check custom patterns
        for rule in self.match_config.custom_patterns.values():
            for match in rule["regex"].finditer(line):
                self._add_match(rule, match.group(), location)
    
    def _token_match(self, line: str, token: str) -> bool:
        """Improved token matching - look for whole words or patterns"""
        line_lower = line.lower()
        token_lower = token.lower()
        
        # Exact word match
        if f" {token_lower} " in f" {line_lower} ":
            return True
        
        # Pattern match (for connection strings, etc.)
        if token_lower in line_lower:
            # Additional validation for certain tokens
            if token_lower in ["jdbc:", "mysql://", "postgresql://", "mongodb://", "redis://", "oracle://"]:
                return "://" in line_lower or "jdbc:" in line_lower
            elif token_lower in ["/home/", "/var/", "c:\\", "d:\\", "/tmp/", "/usr/"]:
                return any(path in line_lower for path in ["/", "\\"])
            else:
                return True
        
        return False
    
    def _add_match(self, rule: Dict[str, Any], value: str, location: str) -> None:
        """Add a match with improved deduplication"""
        key = f"{rule['name']}:{location}"
        
        if key not in self.matches:
            self.matches[key] = RuleMatch(
                name=rule["name"],
                display_name=rule["display_name"],
                confidence=rule["confidence"],
                location=location,
                values=[]
            )
            
        # Improved deduplication - normalize values
        normalized_value = self._normalize_value(value)
        if normalized_value not in [self._normalize_value(v) for v in self.matches[key].values]:
            self.matches[key].values.append(value)
    
    def _normalize_value(self, value: str) -> str:
        """Normalize value for better deduplication"""
        # Remove extra whitespace
        normalized = re.sub(r'\s+', ' ', value.strip())
        # Convert to lowercase for comparison
        return normalized.lower()
    
    def add_custom_pattern(self, name: str, pattern: str, display_name: str = None, confidence: str = "medium"):
        """Add a custom pattern dynamically"""
        if self.match_config.validate_pattern(pattern):
            self.match_config.add_custom_pattern(name, pattern, display_name, confidence)
            # Recompile the new pattern
            rule = self.match_config.custom_patterns[name]
            rule["regex"] = re.compile(pattern, re.IGNORECASE)
            return True
        else:
            raise ValueError(f"Invalid regex pattern: {pattern}")
    
    def remove_custom_pattern(self, name: str):
        """Remove a custom pattern"""
        self.match_config.remove_custom_pattern(name)
        # Remove any existing matches for this pattern
        keys_to_remove = [k for k in self.matches.keys() if k.startswith(f"{name}:")]
        for key in keys_to_remove:
            del self.matches[key]

    def get_patterns(self, options: ScanOptions) -> List[Pattern]:
        """Get patterns to use for scanning - Enhanced with custom patterns"""
        patterns = self._get_default_patterns()

        if options.only_patterns:
            patterns = [p for p in patterns if p.name in options.only_patterns]
        elif options.except_patterns:
            patterns = [p for p in patterns if p.name not in options.except_patterns]

        if options.pattern:
            patterns.append(Pattern(
                name='custom',
                regex=options.pattern,
                description='Custom pattern'
            ))

        return patterns

    def _get_default_patterns(self) -> List[Pattern]:
        """Get default patterns for matching - Enhanced version"""
        return [
            Pattern(
                name='email',
                regex=r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b',
                description='Email address'
            ),
            Pattern(
                name='phone',
                regex=r'\b(?:\+\d{1,3}\s?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}\b',
                description='Phone number'
            ),
            Pattern(
                name='ssn',
                regex=r'\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b',
                description='Social Security Number'
            ),
            Pattern(
                name='credit_card',
                regex=(
                    r'(?:'
                    r'4\d{3}(?:[\-\.\s_\u2013]\d{4}){3}'      # Visa xxxx-xxxx-xxxx-xxxx
                    r'|5[1-5]\d{2}(?:[\-\.\s_\u2013]\d{4}){3}' # MasterCard xxxx-xxxx-xxxx-xxxx
                    r'|3[47]\d{2}[\-\.\s_\u2013]\d{6}[\-\.\s_\u2013]\d{5}' # AmEx 4-6-5
                    r'|6011[\-\.\s_\u2013]\d{4}[\-\.\s_\u2013]\d{4}[\-\.\s_\u2013]\d{4}' # Discover xxxx-xxxx-xxxx-xxxx
                    r')'
                ),
                description='Credit card number (Visa, MasterCard, AmEx, Discover)'
            ),
            Pattern(
                name='credit_card_masked',
                regex=r'\b\d{4}[-\s]?[\*X]{4,8}[-\s]?\d{4}\b',
                description='Masked credit card number'
            ),
            Pattern(
                name='ipv4',
                regex=r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b',
                description='IPv4 address'
            ),
            Pattern(
                name='ipv6',
                regex=r'\b(?:[A-F0-9]{1,4}:){7}[A-F0-9]{1,4}\b',
                description='IPv6 address'
            ),
            Pattern(
                name='url',
                regex=r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+',
                description='URL'
            ),
            Pattern(
                name='mac',
                regex=r'\b(?:[0-9A-Fa-f]{2}[:-]){5}(?:[0-9A-Fa-f]{2})\b',
                description='MAC address'
            ),
            Pattern(
                name='date',
                regex=r'\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b',
                description='Date (YYYY-MM-DD or YYYY/MM/DD)'
            ),
            Pattern(
                name='time',
                regex=r'\b\d{1,2}:\d{2}(?::\d{2})?(?:\s?[AP]M)?\b',
                description='Time (HH:MM:SS or HH:MM AM/PM)'
            ),
            Pattern(
                name='person_name',
                regex=r'\b[A-Z][a-zàáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]+ [A-Z][a-zàáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]+\b',
                description='Person name (Vietnamese and English)'
            ),
            Pattern(
                name='company_name',
                regex=r'\b[A-Z][a-zA-Z\s&.,\'-]+(?:Inc\.|Corp\.|LLC|Ltd\.|Company|Co\.|Technologies|Technology|Systems|Solutions|Services)\b',
                description='Company name'
            )
        ] 

def scan(adapter, rules):
    """Scan adapter với rules (dùng cho test)."""
    results = []
    for item in adapter.fetch():
        for field, rule in rules.items():
            value = item.get(field)
            if value and rule.get("pattern"):
                import re
                if re.match(rule["pattern"], value):
                    results.append({"path": field, "value": value})
    return results 