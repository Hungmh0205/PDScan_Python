#!/usr/bin/env python3

"""
PDScan - Scan for sensitive data
"""

import sys
from .cmd.root import execute

def main() -> int:
    """Main entry point for the CLI"""
    return execute() or 0

if __name__ == "__main__":
    sys.exit(main()) 