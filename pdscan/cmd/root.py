"""Root command for PDScan."""

import argparse
import sys
import os
from typing import List, Optional
from ..internal.main import scan
from ..internal.scan_opts import ScanOptions
from ..internal.exceptions import ScanError
from ..config import PDScanConfig, ConfigError
from ..rbac import RBACManager
from ..logging import AuditLogManager
from ..metrics import MetricsCollector
from ..security import SecurityManager, Authenticator

def new_root_cmd() -> argparse.ArgumentParser:
    """Create root command."""
    parser = argparse.ArgumentParser(
        description="Scan data stores for unencrypted personal data (PII)"
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to YAML config file (default: config/pdscan.yaml)",
    )
    parser.add_argument(
        "--show-data",
        action="store_true",
        help="Show matched data values",
    )
    parser.add_argument(
        "--show-all",
        action="store_true",
        help="Show all fields in matched documents",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=1000,
        help="Number of documents to sample (default: 1000)",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=1,
        help="Number of processes to use (default: 1)",
    )
    parser.add_argument(
        "--only",
        type=str,
        help="Only scan these collections/tables (comma-separated)",
    )
    parser.add_argument(
        "--except",
        type=str,
        dest="except_",
        help="Skip these collections/tables (comma-separated)",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=1,
        help="Minimum number of matches to report (default: 1)",
    )
    parser.add_argument(
        "--pattern",
        type=str,
        help="Custom regex pattern to match",
    )
    parser.add_argument(
        "--only-patterns",
        type=str,
        help="Only scan these patterns (comma-separated, e.g., credit-card,email,ssn)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )
    parser.add_argument(
        "--format",
        type=str,
        choices=["text", "json", "csv", "sqlite"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file path (if not specified, prints to stdout)",
    )
    parser.add_argument(
        "--distributed",
        action="store_true",
        help="Enable distributed scanning via Celery",
    )
    parser.add_argument(
        "url",
        type=str,
        help="Data store URL to scan",
    )
    return parser

def execute(args: Optional[List[str]] = None) -> int:
    """Execute root command."""
    try:
        parser = new_root_cmd()
        parsed_args = parser.parse_args(args)

        # Đọc config YAML (nếu có)
        try:
            config = PDScanConfig(parsed_args.config)
        except ConfigError as ce:
            print(f"[Config Error] {ce}", file=sys.stderr)
            return 2

        # --- Tích hợp bảo mật, RBAC, logging, metrics ---
        # Xác thực user (user_id hoặc api_key)
        user_id = os.getenv('PDSCAN_USER_ID') or getattr(parsed_args, 'user_id', None) or 'cli-user'
        api_key = os.getenv('PDSCAN_API_KEY') or getattr(parsed_args, 'api_key', None)
        security = SecurityManager()
        authenticator = Authenticator(security)
        rbac = RBACManager()
        audit = AuditLogManager(log_dir="logs")
        metrics = MetricsCollector(metrics_file="logs/metrics.json")

        # Gán role mặc định cho user nếu chưa có (demo)
        if user_id not in rbac.user_roles:
            rbac.assign_role(user_id, 'admin')

        # Kiểm tra quyền scan
        if not rbac.check_permission(user_id, 'scan'):
            print(f"Permission denied: user '{user_id}' does not have scan permission.", file=sys.stderr)
            audit.log_permission_denied(user_id, 'scan', parsed_args.url)
            return 3

        # Create scan options (ưu tiên CLI, fallback config)
        options = ScanOptions(
            show_data=parsed_args.show_data,
            show_all=parsed_args.show_all,
            sample_size=parsed_args.sample_size or config.get('scanning', {}).get('batch_size', 1000),
            processes=parsed_args.processes or config.get('scanning', {}).get('max_workers', 1),
            only=parsed_args.only.split(",") if parsed_args.only else None,
            except_=parsed_args.except_.split(",") if parsed_args.except_ else None,
            min_count=parsed_args.min_count,
            pattern=parsed_args.pattern,
            only_patterns=parsed_args.only_patterns.split(",") if parsed_args.only_patterns else None,
            debug=parsed_args.debug,
            format=parsed_args.format,
        )

        # Log scan start
        audit.log_scan_start(user_id, parsed_args.url, options.__dict__)
        scan_id = metrics.start_scan(user_id, parsed_args.url, adapter_type=parsed_args.url.split(":")[0])
        scan_error = None
        matches = []
        import time
        start_time = time.time()
        try:
            # Scan data store
            matches = scan(parsed_args.url, options, config.config)
        except Exception as e:
            scan_error = str(e)
            audit.log_error(user_id, "scan_error", scan_error)
            metrics.complete_scan(scan_id, matches_count=0, error=scan_error)
            raise
        duration = time.time() - start_time
        metrics.complete_scan(scan_id, matches_count=len(matches), error=scan_error)
        audit.log_scan_complete(user_id, parsed_args.url, len(matches), duration)

        # Print results
        if not matches:
            print("No matches found.")
            return 0

        # Import formatter
        from ..internal.formatters import get_formatter
        
        # Get formatter for specified format
        if parsed_args.format == 'sqlite':
            formatter = get_formatter(parsed_args.format, getattr(parsed_args, 'output', None))
            formatted_output = formatter.format(matches)
            print(formatted_output)
        else:
            formatter = get_formatter(parsed_args.format)
            formatted_output = formatter.format(matches)
            # Output to file or stdout
            if parsed_args.output:
                with open(parsed_args.output, 'w', encoding='utf-8') as f:
                    f.write(formatted_output)
                print(f"Results saved to: {parsed_args.output}")
            else:
                print(f"Found {len(matches)} matches:")
                print(formatted_output)

        # Nếu bật distributed, gửi job tới Celery
        if getattr(parsed_args, 'distributed', False):
            from ..internal.distributed import celery_app, distributed_scan
            adapter_type = parsed_args.url.split(":")[0] + "_adapter"
            scan_opts = options.__dict__
            rules = config.config.get('rules', {})
            # Gửi task tới Celery
            result = distributed_scan.delay(adapter_type, scan_opts, rules)
            print("[Distributed] Scan job submitted. Waiting for result...")
            matches = result.get(timeout=600)  # Chờ kết quả tối đa 10 phút
            print(f"[Distributed] Found {len(matches)} matches:")
            for match in matches:
                print(f"- {match['path']}: {match['value']}")
            return 0

        return 0

    except ScanError as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        return 1
    except Exception as e:
        if parsed_args and parsed_args.debug:
            raise
        print(f"Unexpected error: {str(e)}", file=sys.stderr)
        return 1

def main():
    import sys
    sys.exit(execute()) 