#!/usr/bin/env python3
"""
Example usage of OracleAdapterAsync for PDScan
"""

import asyncio
import sys
import os
from pathlib import Path

# Add parent directory to path to import pdscan
sys.path.insert(0, str(Path(__file__).parent.parent))

from pdscan.internal.oracle_adapter_async import OracleAdapterAsync
from pdscan.internal.scan_opts import ScanOptions

async def basic_scan_example():
    """Basic scan example with minimal configuration."""
    print("🔍 Basic Oracle Async Scan Example")
    print("=" * 50)
    
    # Configuration
    config = {
        'user': 'your_username',
        'password': 'your_password',
        'host': 'localhost',
        'port': 1521,
        'service_name': 'ORCL',
        'max_concurrent_tables': 5,
        'pool_min': 3,
        'pool_max': 10
    }
    
    # Create adapter
    adapter = OracleAdapterAsync('oracle://user:pass@host:port/service', config)
    
    # Scan options
    options = ScanOptions(
        show_data=True,
        show_all=False,
        only_patterns=['credit_card', 'email']
    )
    
    try:
        # Run scan
        matches = await adapter.scan(options)
        
        # Print results
        print(f"\n🎉 Scan completed! Found {len(matches)} matches:")
        for i, match in enumerate(matches[:10], 1):  # Show first 10
            print(f"{i}. {match['rule']} in {match['table']}.{match['column']}")
            print(f"   Value: {match['value'][:50]}...")
            print()
            
        if len(matches) > 10:
            print(f"... and {len(matches) - 10} more matches")
            
    except Exception as e:
        print(f"❌ Error during scan: {e}")

async def advanced_scan_example():
    """Advanced scan example with performance tuning."""
    print("\n🚀 Advanced Oracle Async Scan Example")
    print("=" * 50)
    
    # Advanced configuration for high performance
    config = {
        'user': 'your_username',
        'password': 'your_password',
        'host': 'localhost',
        'port': 1521,
        'service_name': 'ORCL',
        
        # High concurrency for large databases
        'max_concurrent_tables': 15,
        'fetch_size': 20000,
        
        # Connection pool tuning
        'pool_min': 10,
        'pool_max': 30,
        'pool_increment': 5,
        
        # Schema filtering
        'target_schema': 'PRODUCTION_DATA',  # Only scan specific schema
        'skip_schemas': {'SYS', 'SYSTEM', 'XDB', 'OUTLN', 'ORDSYS'},
        
        # Performance optimizations
        'early_termination': True,
        'value_caching': True,
        'column_optimization': True,
        'batch_optimization': True,
        'pattern_optimization': True,
        'adaptive_batch': True,
        
        # Batch size limits
        'min_batch_size': 2000,
        'max_batch_size': 75000,
        
        # Retry and timeout
        'retry_attempts': 3,
        'retry_delay': 2,
        'table_timeout': 180,
        'timeout': 60
    }
    
    # Create adapter
    adapter = OracleAdapterAsync('oracle://user:pass@host:port/service', config)
    
    # Comprehensive scan options
    options = ScanOptions(
        show_data=True,
        show_all=True,  # Show all matches per value
        only_patterns=['credit_card', 'email', 'ssn', 'phone', 'password'],
        sample_size=5000  # Sample size for large tables
    )
    
    try:
        # Run scan
        matches = await adapter.scan(options)
        
        # Analyze results
        pattern_counts = {}
        table_counts = {}
        
        for match in matches:
            # Count by pattern
            pattern = match['rule']
            pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1
            
            # Count by table
            table = match['table']
            table_counts[table] = table_counts.get(table, 0) + 1
        
        # Print summary
        print(f"\n📊 Scan Summary:")
        print(f"Total matches found: {len(matches)}")
        
        print(f"\n🔍 Matches by pattern:")
        for pattern, count in sorted(pattern_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {pattern}: {count}")
        
        print(f"\n📋 Top tables with matches:")
        for table, count in sorted(table_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {table}: {count}")
            
    except Exception as e:
        print(f"❌ Error during scan: {e}")

async def sync_compatibility_example():
    """Example showing sync compatibility methods."""
    print("\n🔄 Sync Compatibility Example")
    print("=" * 50)
    
    config = {
        'user': 'your_username',
        'password': 'your_password',
        'host': 'localhost',
        'port': 1521,
        'service_name': 'ORCL',
        'max_concurrent_tables': 5
    }
    
    # Create adapter
    adapter = OracleAdapterAsync('oracle://user:pass@host:port/service', config)
    
    # Scan options
    options = ScanOptions(
        show_data=False,
        only_patterns=['credit_card']
    )
    
    try:
        # Use sync methods (for compatibility with existing code)
        adapter.connect_sync()
        matches = adapter.scan_sync(options)
        adapter.disconnect_sync()
        
        print(f"✅ Sync scan completed! Found {len(matches)} credit card matches")
        
    except Exception as e:
        print(f"❌ Error during sync scan: {e}")

async def error_handling_example():
    """Example showing error handling capabilities."""
    print("\n🛡️ Error Handling Example")
    print("=" * 50)
    
    # Configuration with aggressive settings to trigger errors
    config = {
        'user': 'your_username',
        'password': 'your_password',
        'host': 'localhost',
        'port': 1521,
        'service_name': 'ORCL',
        'max_concurrent_tables': 50,  # Very high concurrency
        'pool_max': 100,              # Large pool
        'fetch_size': 50000,          # Large batch size
        'retry_attempts': 5,          # More retries
        'table_timeout': 30           # Short timeout
    }
    
    adapter = OracleAdapterAsync('oracle://user:pass@host:port/service', config)
    
    options = ScanOptions(
        show_data=False,
        only_patterns=['email']
    )
    
    try:
        # This might trigger some errors due to aggressive settings
        matches = await adapter.scan(options)
        print(f"✅ Scan completed despite some errors! Found {len(matches)} matches")
        
    except Exception as e:
        print(f"❌ Critical error: {e}")

async def performance_monitoring_example():
    """Example showing performance monitoring features."""
    print("\n📈 Performance Monitoring Example")
    print("=" * 50)
    
    config = {
        'user': 'your_username',
        'password': 'your_password',
        'host': 'localhost',
        'port': 1521,
        'service_name': 'ORCL',
        'max_concurrent_tables': 10,
        'fetch_size': 15000,
        'pool_min': 5,
        'pool_max': 20
    }
    
    adapter = OracleAdapterAsync('oracle://user:pass@host:port/service', config)
    
    options = ScanOptions(
        show_data=False,
        only_patterns=['credit_card', 'email', 'ssn']
    )
    
    try:
        # Run scan with performance monitoring
        matches = await adapter.scan(options)
        
        # Get detailed metrics
        metrics = adapter._get_performance_metrics()
        
        print(f"\n📊 Detailed Performance Metrics:")
        print(f"Tables completed: {metrics['tables_completed']}")
        print(f"Tables skipped: {metrics['tables_skipped']}")
        print(f"Rows processed: {metrics['total_rows_processed']:,}")
        print(f"Matches found: {metrics['total_matches_found']:,}")
        print(f"Rows/second: {metrics.get('rows_per_second', 0):,.0f}")
        print(f"Matches/second: {metrics.get('matches_per_second', 0):,.0f}")
        print(f"Memory usage: {metrics.get('current_memory_usage', 0):.1f}MB")
        print(f"Cache hit rate: {metrics.get('cache_hit_rate', 0):.1%}")
        print(f"Early termination rate: {metrics.get('early_termination_rate', 0):.1%}")
        print(f"Connection errors: {metrics['connection_errors']}")
        print(f"Retry attempts: {metrics['retry_count']}")
        
    except Exception as e:
        print(f"❌ Error during performance monitoring: {e}")

def main():
    """Main function to run all examples."""
    print("🎯 OracleAdapterAsync Examples")
    print("=" * 60)
    print("This script demonstrates various usage patterns of OracleAdapterAsync.")
    print("Please update the configuration with your actual Oracle connection details.")
    print()
    
    # Check if user wants to run examples
    response = input("Do you want to run the examples? (y/N): ").strip().lower()
    if response != 'y':
        print("Exiting...")
        return
    
    # Run examples
    examples = [
        basic_scan_example,
        advanced_scan_example,
        sync_compatibility_example,
        error_handling_example,
        performance_monitoring_example
    ]
    
    for example in examples:
        try:
            asyncio.run(example())
        except KeyboardInterrupt:
            print("\n⏹️ Example interrupted by user")
            break
        except Exception as e:
            print(f"❌ Error running example {example.__name__}: {e}")
        
        print("\n" + "="*60 + "\n")

if __name__ == "__main__":
    main() 