"""
Audit logging for PDScan
"""

import logging
import json
import sys
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

class AuditLogger:
    """Audit logger for security events"""
    
    def __init__(self, log_file: Optional[str] = None, level: str = 'INFO'):
        self.logger = logging.getLogger('pdscan_audit')
        self.logger.setLevel(getattr(logging, level.upper()))
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler (if specified)
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
    
    def log_scan_start(self, user_id: str, url: str, options: Dict[str, Any]):
        """Log scan start event"""
        self.logger.info(
            f"SCAN_START - User: {user_id}, URL: {url}, Options: {json.dumps(options)}"
        )
    
    def log_scan_complete(self, user_id: str, url: str, matches_count: int, duration: float):
        """Log scan completion event"""
        self.logger.info(
            f"SCAN_COMPLETE - User: {user_id}, URL: {url}, "
            f"Matches: {matches_count}, Duration: {duration:.2f}s"
        )
    
    def log_login(self, user_id: str, success: bool, ip_address: Optional[str] = None):
        """Log login attempt"""
        status = "SUCCESS" if success else "FAILED"
        ip_info = f", IP: {ip_address}" if ip_address else ""
        self.logger.info(f"LOGIN_{status} - User: {user_id}{ip_info}")
    
    def log_config_change(self, user_id: str, config_type: str, details: str):
        """Log configuration change"""
        self.logger.warning(
            f"CONFIG_CHANGE - User: {user_id}, Type: {config_type}, Details: {details}"
        )
    
    def log_permission_denied(self, user_id: str, action: str, resource: str):
        """Log permission denied event"""
        self.logger.warning(
            f"PERMISSION_DENIED - User: {user_id}, Action: {action}, Resource: {resource}"
        )
    
    def log_error(self, user_id: str, error_type: str, error_message: str):
        """Log error event"""
        self.logger.error(
            f"ERROR - User: {user_id}, Type: {error_type}, Message: {error_message}"
        )

class JSONAuditLogger:
    """JSON format audit logger"""
    
    def __init__(self, log_file: Optional[str] = None):
        self.log_file = log_file
        self.logger = logging.getLogger('pdscan_audit_json')
        self.logger.setLevel(logging.INFO)
        
        if log_file:
            handler = logging.FileHandler(log_file)
            handler.setFormatter(logging.Formatter('%(message)s'))
            self.logger.addHandler(handler)
    
    def _log_event(self, event_type: str, data: Dict[str, Any]):
        """Log event in JSON format"""
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'event_type': event_type,
            **data
        }
        self.logger.info(json.dumps(log_entry))
    
    def log_scan_start(self, user_id: str, url: str, options: Dict[str, Any]):
        """Log scan start event in JSON"""
        self._log_event('scan_start', {
            'user_id': user_id,
            'url': url,
            'options': options
        })
    
    def log_scan_complete(self, user_id: str, url: str, matches_count: int, duration: float):
        """Log scan completion event in JSON"""
        self._log_event('scan_complete', {
            'user_id': user_id,
            'url': url,
            'matches_count': matches_count,
            'duration': duration
        })
    
    def log_login(self, user_id: str, success: bool, ip_address: Optional[str] = None):
        """Log login attempt in JSON"""
        self._log_event('login', {
            'user_id': user_id,
            'success': success,
            'ip_address': ip_address
        })
    
    def log_config_change(self, user_id: str, config_type: str, details: str):
        """Log configuration change in JSON"""
        self._log_event('config_change', {
            'user_id': user_id,
            'config_type': config_type,
            'details': details
        })
    
    def log_permission_denied(self, user_id: str, action: str, resource: str):
        """Log permission denied event in JSON"""
        self._log_event('permission_denied', {
            'user_id': user_id,
            'action': action,
            'resource': resource
        })
    
    def log_error(self, user_id: str, error_type: str, error_message: str):
        """Log error event in JSON"""
        self._log_event('error', {
            'user_id': user_id,
            'error_type': error_type,
            'error_message': error_message
        })

class AuditLogManager:
    """Manager for audit logging"""
    
    def __init__(self, log_dir: str = "logs", enable_json: bool = True):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        # Text logger
        self.text_logger = AuditLogger(
            log_file=str(self.log_dir / "audit.log")
        )
        
        # JSON logger
        self.json_logger = None
        if enable_json:
            self.json_logger = JSONAuditLogger(
                log_file=str(self.log_dir / "audit.json")
            )
    
    def log_scan_start(self, user_id: str, url: str, options: Dict[str, Any]):
        """Log scan start with both loggers"""
        self.text_logger.log_scan_start(user_id, url, options)
        if self.json_logger:
            self.json_logger.log_scan_start(user_id, url, options)
    
    def log_scan_complete(self, user_id: str, url: str, matches_count: int, duration: float):
        """Log scan complete with both loggers"""
        self.text_logger.log_scan_complete(user_id, url, matches_count, duration)
        if self.json_logger:
            self.json_logger.log_scan_complete(user_id, url, matches_count, duration)
    
    def log_login(self, user_id: str, success: bool, ip_address: Optional[str] = None):
        """Log login with both loggers"""
        self.text_logger.log_login(user_id, success, ip_address)
        if self.json_logger:
            self.json_logger.log_login(user_id, success, ip_address)
    
    def log_config_change(self, user_id: str, config_type: str, details: str):
        """Log config change with both loggers"""
        self.text_logger.log_config_change(user_id, config_type, details)
        if self.json_logger:
            self.json_logger.log_config_change(user_id, config_type, details)
    
    def log_permission_denied(self, user_id: str, action: str, resource: str):
        """Log permission denied with both loggers"""
        self.text_logger.log_permission_denied(user_id, action, resource)
        if self.json_logger:
            self.json_logger.log_permission_denied(user_id, action, resource)
    
    def log_error(self, user_id: str, error_type: str, error_message: str):
        """Log error with both loggers"""
        self.text_logger.log_error(user_id, error_type, error_message)
        if self.json_logger:
            self.json_logger.log_error(user_id, error_type, error_message) 