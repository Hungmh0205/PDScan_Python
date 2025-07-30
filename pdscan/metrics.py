"""
Performance metrics collection for PDScan
"""

import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from collections import defaultdict, Counter
from dataclasses import dataclass, asdict, field
from pathlib import Path

@dataclass
class ScanMetrics:
    """Metrics for a single scan"""
    user_id: str
    url: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    matches_count: int = 0
    error: Optional[str] = None
    adapter_type: Optional[str] = None

@dataclass
class SystemMetrics:
    """System-wide metrics"""
    total_scans: int = 0
    successful_scans: int = 0
    failed_scans: int = 0
    total_matches: int = 0
    total_duration: float = 0.0
    avg_duration: float = 0.0
    error_rate: float = 0.0
    scans_by_adapter: dict = field(default_factory=dict)
    scans_by_user: dict = field(default_factory=dict)
    
    def __post_init__(self):
        if self.scans_by_adapter is None:
            self.scans_by_adapter = defaultdict(int)
        if self.scans_by_user is None:
            self.scans_by_user = defaultdict(int)

class MetricsCollector:
    """Collect and manage performance metrics"""
    
    def __init__(self, metrics_file: Optional[str] = None):
        self.metrics_file = metrics_file
        self.scan_metrics: List[ScanMetrics] = []
        self.system_metrics = SystemMetrics()
        self.current_scans: Dict[str, ScanMetrics] = {}
        
    def start_scan(self, user_id: str, url: str, adapter_type: str) -> str:
        """Start tracking a scan"""
        scan_id = f"{user_id}_{int(time.time())}"
        scan_metric = ScanMetrics(
            user_id=user_id,
            url=url,
            start_time=datetime.utcnow(),
            adapter_type=adapter_type
        )
        self.current_scans[scan_id] = scan_metric
        return scan_id
    
    def complete_scan(self, scan_id: str, matches_count: int = 0, error: Optional[str] = None):
        """Complete tracking a scan"""
        if scan_id not in self.current_scans:
            return
            
        scan_metric = self.current_scans[scan_id]
        scan_metric.end_time = datetime.utcnow()
        scan_metric.duration = (scan_metric.end_time - scan_metric.start_time).total_seconds()
        scan_metric.matches_count = matches_count
        scan_metric.error = error
        
        # Add to completed scans
        self.scan_metrics.append(scan_metric)
        
        # Update system metrics
        self._update_system_metrics(scan_metric)
        
        # Remove from current scans
        del self.current_scans[scan_id]
        
        # Save to file if specified
        if self.metrics_file:
            self._save_metrics()
    
    def _update_system_metrics(self, scan_metric: ScanMetrics):
        """Update system-wide metrics"""
        self.system_metrics.total_scans += 1
        
        if scan_metric.error:
            self.system_metrics.failed_scans += 1
        else:
            self.system_metrics.successful_scans += 1
            
        self.system_metrics.total_matches += scan_metric.matches_count
        self.system_metrics.total_duration += scan_metric.duration or 0
        
        # Update averages
        if self.system_metrics.successful_scans > 0:
            self.system_metrics.avg_duration = (
                self.system_metrics.total_duration / self.system_metrics.successful_scans
            )
        
        # Update error rate
        if self.system_metrics.total_scans > 0:
            self.system_metrics.error_rate = (
                self.system_metrics.failed_scans / self.system_metrics.total_scans
            )
        
        # Update adapter and user counts
        if scan_metric.adapter_type:
            self.system_metrics.scans_by_adapter[scan_metric.adapter_type] = self.system_metrics.scans_by_adapter.get(scan_metric.adapter_type, 0) + 1
        self.system_metrics.scans_by_user[scan_metric.user_id] = self.system_metrics.scans_by_user.get(scan_metric.user_id, 0) + 1
    
    def get_system_metrics(self) -> SystemMetrics:
        """Get current system metrics"""
        return self.system_metrics
    
    def get_recent_scans(self, hours: int = 24) -> List[ScanMetrics]:
        """Get scans from the last N hours"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        return [
            scan for scan in self.scan_metrics
            if scan.start_time >= cutoff_time
        ]
    
    def get_user_metrics(self, user_id: str) -> Dict[str, Any]:
        """Get metrics for a specific user"""
        user_scans = [scan for scan in self.scan_metrics if scan.user_id == user_id]
        
        if not user_scans:
            return {}
            
        total_scans = len(user_scans)
        successful_scans = len([s for s in user_scans if not s.error])
        total_matches = sum(s.matches_count for s in user_scans)
        total_duration = sum(s.duration or 0 for s in user_scans)
        
        return {
            'user_id': user_id,
            'total_scans': total_scans,
            'successful_scans': successful_scans,
            'failed_scans': total_scans - successful_scans,
            'total_matches': total_matches,
            'total_duration': total_duration,
            'avg_duration': total_duration / successful_scans if successful_scans > 0 else 0,
            'error_rate': (total_scans - successful_scans) / total_scans if total_scans > 0 else 0
        }
    
    def export_prometheus(self) -> str:
        """Export metrics in Prometheus format"""
        metrics = []
        
        # System metrics
        metrics.append(f"# HELP pdscan_total_scans Total number of scans")
        metrics.append(f"# TYPE pdscan_total_scans counter")
        metrics.append(f"pdscan_total_scans {self.system_metrics.total_scans}")
        
        metrics.append(f"# HELP pdscan_successful_scans Total number of successful scans")
        metrics.append(f"# TYPE pdscan_successful_scans counter")
        metrics.append(f"pdscan_successful_scans {self.system_metrics.successful_scans}")
        
        metrics.append(f"# HELP pdscan_failed_scans Total number of failed scans")
        metrics.append(f"# TYPE pdscan_failed_scans counter")
        metrics.append(f"pdscan_failed_scans {self.system_metrics.failed_scans}")
        
        metrics.append(f"# HELP pdscan_total_matches Total number of matches found")
        metrics.append(f"# TYPE pdscan_total_matches counter")
        metrics.append(f"pdscan_total_matches {self.system_metrics.total_matches}")
        
        metrics.append(f"# HELP pdscan_avg_duration Average scan duration in seconds")
        metrics.append(f"# TYPE pdscan_avg_duration gauge")
        metrics.append(f"pdscan_avg_duration {self.system_metrics.avg_duration}")
        
        metrics.append(f"# HELP pdscan_error_rate Error rate as percentage")
        metrics.append(f"# TYPE pdscan_error_rate gauge")
        metrics.append(f"pdscan_error_rate {self.system_metrics.error_rate}")
        
        # Adapter metrics
        for adapter, count in self.system_metrics.scans_by_adapter.items():
            metrics.append(f"# HELP pdscan_scans_by_adapter Scans by adapter type")
            metrics.append(f"# TYPE pdscan_scans_by_adapter counter")
            metrics.append(f'pdscan_scans_by_adapter{{adapter="{adapter}"}} {count}')
        
        # User metrics
        for user, count in self.system_metrics.scans_by_user.items():
            metrics.append(f"# HELP pdscan_scans_by_user Scans by user")
            metrics.append(f"# TYPE pdscan_scans_by_user counter")
            metrics.append(f'pdscan_scans_by_user{{user="{user}"}} {count}')
        
        return "\n".join(metrics)
    
    def export_json(self) -> str:
        """Export metrics in JSON format"""
        return json.dumps({
            'system_metrics': asdict(self.system_metrics),
            'recent_scans': [
                {
                    'user_id': scan.user_id,
                    'url': scan.url,
                    'start_time': scan.start_time.isoformat(),
                    'end_time': scan.end_time.isoformat() if scan.end_time else None,
                    'duration': scan.duration,
                    'matches_count': scan.matches_count,
                    'error': scan.error,
                    'adapter_type': scan.adapter_type
                }
                for scan in self.get_recent_scans(24)
            ]
        }, indent=2)
    
    def _save_metrics(self):
        """Save metrics to file"""
        if not self.metrics_file:
            return
            
        metrics_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'system_metrics': asdict(self.system_metrics),
            'recent_scans': [
                asdict(scan) for scan in self.get_recent_scans(24)
            ]
        }
        
        # Convert datetime objects to strings for JSON serialization
        for scan in metrics_data['recent_scans']:
            if scan['start_time']:
                scan['start_time'] = scan['start_time'].isoformat()
            if scan['end_time']:
                scan['end_time'] = scan['end_time'].isoformat()
        
        with open(self.metrics_file, 'w') as f:
            json.dump(metrics_data, f, indent=2) 