"""
Helper functions
"""

import sys
from typing import List, Any
from urllib.parse import urlparse

def pluralize(count: int, word: str) -> str:
    """Pluralize a word based on count"""
    if count == 1:
        return f"{count} {word}"
    return f"{count} {word}s"

def show_low_confidence_match_help() -> None:
    """Show help for low confidence matches"""
    print("\nLow confidence matches may be false positives.", file=sys.stderr)
    print("Use --show-all to see all matches.", file=sys.stderr)

def print_match_list(formatter: Any, match_list: List[Any], show_data: bool, show_all: bool, row_name: str) -> None:
    """Print list of matches"""
    if not match_list:
        return
        
    for match in match_list:
        if match.confidence == "low" and not show_all:
            continue
        formatter.print_match(match, show_data, row_name)
        
    if not show_all and any(m.confidence == "low" for m in match_list):
        show_low_confidence_match_help()

def update_rules(match_config: Any, rules: List[dict]) -> None:
    """Update rules in match config"""
    for rule in rules:
        if "name" in rule and "pattern" in rule:
            match_config.name_rules.append(rule)
        elif "name" in rule and "patterns" in rule:
            match_config.multi_name_rules.append(rule)
        elif "name" in rule and "tokens" in rule:
            match_config.token_rules.append(rule)
        elif "regex" in rule:
            match_config.regex_rules.append(rule)

def make_valid_names(names: List[str]) -> List[str]:
    """Make valid names from list"""
    valid_names = []
    for name in names:
        if name and name.strip():
            valid_names.append(name.strip())
    return valid_names

def get_adapter_type(url: str) -> str:
    """Get adapter type from URL."""
    scheme = urlparse(url).scheme
    if scheme == "mongodb":
        return "mongodb"
    elif scheme == "redis":
        return "redis"
    elif scheme in ["postgresql", "mysql", "sqlite", "sql", "mariadb"]:
        return "sql"
    return "unknown" 