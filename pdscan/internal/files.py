"""
File handling functions
"""

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Any
import magic
import pandas as pd

from .helpers import pluralize, print_match_list
from .match_finder import MatchFinder

def scan_files(adapter: Any, scan_opts: Any) -> List[Any]:
    """Scan files using adapter"""
    adapter.init(scan_opts.url_str)
    files = adapter.fetch_files()

    if not files:
        print(f"Found no {adapter.object_name()} to scan", file=sys.stderr)
        return []

    print(f"Found {pluralize(len(files), adapter.object_name())} to scan...\n", file=sys.stderr)

    match_list = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for file in files:
            futures.append(executor.submit(
                scan_file,
                adapter,
                file,
                scan_opts
            ))

        for future in futures:
            try:
                file_matches = future.result()
                match_list.extend(file_matches)
            except Exception as e:
                print(f"Error scanning file: {str(e)}", file=sys.stderr)

    return match_list

def scan_file(adapter: Any, file: Any, scan_opts: Any) -> List[Any]:
    """Scan a single file"""
    start = time.time()
    match_finder = MatchFinder(scan_opts.match_config)
    adapter.find_file_matches(file, match_finder)

    if scan_opts.debug:
        duration = (time.time() - start) * 1000
        print(f"Scanned {file} ({int(duration)} ms)", file=sys.stderr)

    file_match_list = match_finder.check_matches(file, True)
    print_match_list(scan_opts.formatter, file_match_list, scan_opts.show_data, scan_opts.show_all, "line")

    return file_match_list

def scan_text_file(filepath: str, match_finder: Any) -> None:
    """Scan a text file"""
    with open(filepath, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            match_finder.check_line(line.strip(), line_num)

def scan_excel_file(filepath: str, match_finder: Any) -> None:
    """Scan an Excel file"""
    df = pd.read_excel(filepath)
    for col in df.columns:
        for row_num, value in enumerate(df[col], 1):
            if pd.notna(value):
                match_finder.check_line(str(value), row_num)

def scan_csv_file(filepath: str, match_finder: Any) -> None:
    """Scan a CSV file"""
    df = pd.read_csv(filepath)
    for col in df.columns:
        for row_num, value in enumerate(df[col], 1):
            if pd.notna(value):
                match_finder.check_line(str(value), row_num) 