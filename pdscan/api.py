"""
REST API for PDScan
"""

from fastapi import FastAPI, HTTPException, Depends, Header, Request, Body, status
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, field_validator
from typing import List, Dict, Any, Optional
import uvicorn
import time
import os
from datetime import datetime
from collections import defaultdict
import threading
import logging

from .rbac import RBACManager
from .logging import AuditLogManager
from .metrics import MetricsCollector
from .security import SecurityManager, Authenticator
from .reporting import ReportGenerator
from .internal.main import scan
from .internal.scan_opts import ScanOptions
from .config import PDScanConfig
from .notification import notify_scan_complete, notify_scan_failed, notify_report_generated
from .notification import notify_scan_complete_email, notify_scan_failed_email, notify_report_generated_email
from .notification import notify_scan_complete_slack, notify_scan_failed_slack, notify_report_generated_slack

# Pydantic models with validation
class ScanRequest(BaseModel):
    url: str
    sample_size: int = 1000
    show_data: bool = False
    show_all: bool = False
    format: str = "json"
    
    @field_validator('url')
    @classmethod
    def validate_url(cls, v):
        if not v or not v.strip():
            raise ValueError('URL cannot be empty')
        return v.strip()
    
    @field_validator('sample_size')
    @classmethod
    def validate_sample_size(cls, v):
        if v < 1 or v > 100000:
            raise ValueError('Sample size must be between 1 and 100000')
        return v
    
    @field_validator('format')
    @classmethod
    def validate_format(cls, v):
        allowed_formats = ['json', 'csv', 'table']
        if v not in allowed_formats:
            raise ValueError(f'Format must be one of: {allowed_formats}')
        return v

class ScanResponse(BaseModel):
    scan_id: str
    status: str
    message: str
    matches_count: int = 0
    estimated_duration: Optional[float] = None

class ScanResult(BaseModel):
    scan_id: str
    url: str
    matches: List[Dict[str, Any]]
    scan_info: Dict[str, Any]
    metrics: Optional[Dict[str, Any]] = None

class ReportRequest(BaseModel):
    scan_id: str
    format: str = "html"  # html, json, csv, pdf
    
    @field_validator('format')
    @classmethod
    def validate_format(cls, v):
        allowed_formats = ['html', 'json', 'csv', 'pdf']
        if v not in allowed_formats:
            raise ValueError(f'Format must be one of: {allowed_formats}')
        return v

class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    timestamp: str

# Global instances
app = FastAPI(
    title="PDScan API",
    description="API for scanning data stores for personal data",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Initialize components
security = SecurityManager()
authenticator = Authenticator(security)
rbac = RBACManager()
audit = AuditLogManager(log_dir="logs")
metrics = MetricsCollector(metrics_file="logs/metrics.json")
reporter = ReportGenerator(output_dir="reports")

# In-memory storage for demo (in production, use database)
scan_results = {}
scan_requests = {}

# Security middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["*"]  # Configure properly for production
)

# Rate limiting
rate_limits = defaultdict(lambda: {'count': 0, 'reset': time.time() + 60})
rate_limit_lock = threading.Lock()
RATE_LIMIT = 60  # requests per minute

def rate_limit(user_id: str):
    """Apply rate limiting per user"""
    with rate_limit_lock:
        rl = rate_limits[user_id]
        now = time.time()
        if now > rl['reset']:
            rl['count'] = 0
            rl['reset'] = now + 60
        rl['count'] += 1
        if rl['count'] > RATE_LIMIT:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS, 
                detail="Rate limit exceeded. Try again later."
            )

async def get_current_user(request: Request, authorization: str = Header(None)) -> str:
    """Authenticate user via API key"""
    api_key = None
    # Try header first
    if authorization:
        api_key = authorization.replace("Bearer ", "")
    # Fallback to query param
    elif "api_key" in request.query_params:
        api_key = request.query_params["api_key"]
    
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, 
            detail="API key required"
        )
    
    user_id = authenticator.authenticate_api_key(api_key)
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, 
            detail="Invalid API key"
        )
    
    # Apply rate limiting
    rate_limit(user_id)
    return user_id

def check_permission(user_id: str, permission: str):
    """Check if user has permission"""
    if not rbac.check_permission(user_id, permission):
        audit.log_permission_denied(user_id, permission, "API")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail=f"Permission denied: {permission}"
        )

# Exception handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions"""
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=exc.detail,
            timestamp=datetime.utcnow().isoformat()
        ).model_dump()
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle general exceptions"""
    logging.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=ErrorResponse(
            error="Internal server error",
            detail=str(exc) if app.debug else None,
            timestamp=datetime.utcnow().isoformat()
        ).model_dump()
    )

@app.post("/api/v1/scan", response_model=ScanResponse)
async def start_scan(
    request: ScanRequest,
    user_id: str = Depends(get_current_user)
):
    """Start a new scan"""
    try:
        check_permission(user_id, "scan")
        
        # Generate scan ID
        scan_id = f"{user_id}_{int(time.time())}"
        
        # Log scan start
        audit.log_scan_start(user_id, request.url, request.model_dump())
        
        # Start metrics tracking
        metrics_scan_id = metrics.start_scan(user_id, request.url, request.url.split(":")[0])
        
        # Store scan request
        scan_requests[scan_id] = {
            "user_id": user_id,
            "request": request.model_dump(),
            "start_time": datetime.utcnow(),
            "metrics_scan_id": metrics_scan_id,
            "status": "running"
        }
        
        # Perform scan
        try:
            # Create scan options
            options = ScanOptions(
                show_data=request.show_data,
                show_all=request.show_all,
                sample_size=request.sample_size,
                format=request.format
            )
            
            # Perform scan
            matches = scan(request.url, options, {})
            
            # Update scan results
            duration = time.time() - scan_requests[scan_id]["start_time"].timestamp()
            scan_results[scan_id] = {
                "matches": matches,
                "scan_info": {
                    "url": request.url,
                    "total_matches": len(matches),
                    "duration": duration,
                    "sample_size": request.sample_size,
                    "user_id": user_id,
                    "timestamp": scan_requests[scan_id]["start_time"].isoformat()
                }
            }
            
            # Complete metrics
            metrics.complete_scan(metrics_scan_id, len(matches))
            
            # Log scan complete
            audit.log_scan_complete(user_id, request.url, len(matches), duration)
            
            # Update status
            scan_requests[scan_id]["status"] = "completed"
            
            # Gửi webhook notification scan_complete
            notify_scan_complete(user_id, scan_id, len(matches), status="completed", logger=audit.text_logger.logger)
            # Gửi email notification scan_complete
            notify_scan_complete_email(user_id, scan_id, len(matches), status="completed", logger=audit.text_logger.logger)
            # Gửi Slack notification scan_complete
            notify_scan_complete_slack(user_id, scan_id, len(matches), status="completed", logger=audit.text_logger.logger)
            
            return ScanResponse(
                scan_id=scan_id,
                status="completed",
                message=f"Scan completed. Found {len(matches)} matches.",
                matches_count=len(matches),
                estimated_duration=duration
            )
            
        except Exception as e:
            # Log error
            audit.log_error(user_id, "scan_error", str(e))
            metrics.complete_scan(metrics_scan_id, 0, str(e))
            
            # Update status
            scan_requests[scan_id]["status"] = "failed"
            
            # Gửi webhook notification scan_failed
            notify_scan_failed(user_id, scan_id, str(e), logger=audit.text_logger.logger)
            # Gửi email notification scan_failed
            notify_scan_failed_email(user_id, scan_id, str(e), logger=audit.text_logger.logger)
            # Gửi Slack notification scan_failed
            notify_scan_failed_slack(user_id, scan_id, str(e), logger=audit.text_logger.logger)
            
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Scan failed: {str(e)}"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Unexpected error in start_scan: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@app.get("/api/v1/scan/{scan_id}", response_model=ScanResult)
async def get_scan_result(
    scan_id: str,
    user_id: str = Depends(get_current_user)
):
    """Get scan results"""
    try:
        check_permission(user_id, "view_reports")
        
        if scan_id not in scan_results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail="Scan not found"
            )
        
        # Check if user owns this scan or is admin
        scan_owner = scan_requests.get(scan_id, {}).get("user_id")
        if scan_owner != user_id and not rbac.check_permission(user_id, "manage_users"):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, 
                detail="Access denied"
            )
        
        result = scan_results[scan_id]
        
        # Get system metrics
        system_metrics = metrics.get_system_metrics()
        metrics_dict = {
            "total_scans": system_metrics.total_scans,
            "successful_scans": system_metrics.successful_scans,
            "failed_scans": system_metrics.failed_scans,
            "avg_duration": system_metrics.avg_duration,
            "error_rate": system_metrics.error_rate
        }
        
        return ScanResult(
            scan_id=scan_id,
            url=result["scan_info"]["url"],
            matches=result["matches"],
            scan_info=result["scan_info"],
            metrics=metrics_dict
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Unexpected error in get_scan_result: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@app.post("/api/v1/reports")
async def generate_report(
    request: ReportRequest,
    user_id: str = Depends(get_current_user)
):
    """Generate report for scan"""
    try:
        check_permission(user_id, "view_reports")
        
        if request.scan_id not in scan_results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail="Scan not found"
            )
        
        result = scan_results[request.scan_id]
        
        # Get system metrics
        system_metrics = metrics.get_system_metrics()
        metrics_dict = {
            "total_scans": system_metrics.total_scans,
            "successful_scans": system_metrics.successful_scans,
            "failed_scans": system_metrics.failed_scans,
            "success_rate": system_metrics.successful_scans / system_metrics.total_scans if system_metrics.total_scans > 0 else 0,
            "avg_duration": system_metrics.avg_duration,
            "error_rate": system_metrics.error_rate
        }
        
        # Generate report
        try:
            if request.format == "html":
                report_file = reporter.generate_html_report(
                    result["matches"], result["scan_info"], metrics_dict
                )
            elif request.format == "json":
                report_file = reporter.generate_json_report(
                    result["matches"], result["scan_info"], metrics_dict
                )
            elif request.format == "csv":
                report_file = reporter.generate_csv_report(
                    result["matches"], result["scan_info"]
                )
            elif request.format == "pdf":
                try:
                    report_file = reporter.generate_pdf_report(
                        result["matches"], result["scan_info"], metrics_dict
                    )
                except ImportError:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST, 
                        detail="PDF generation not available"
                    )
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, 
                    detail="Unsupported format"
                )
            
            # Gửi webhook notification report_generated
            notify_report_generated(user_id, request.scan_id, request.format, report_url=None, logger=audit.text_logger.logger)
            # Gửi email notification report_generated với file đính kèm
            notify_report_generated_email(user_id, request.scan_id, request.format, report_file=report_file, logger=audit.text_logger.logger)
            # Gửi Slack notification report_generated
            notify_report_generated_slack(user_id, request.scan_id, request.format, report_url=None, logger=audit.text_logger.logger)
            
            # Return file
            return FileResponse(
                report_file,
                media_type="application/octet-stream",
                filename=os.path.basename(report_file)
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logging.error(f"Report generation error: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Report generation failed: {str(e)}"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Unexpected error in generate_report: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@app.get("/api/v1/metrics")
async def get_metrics(user_id: str = Depends(get_current_user)):
    """Get system metrics"""
    try:
        check_permission(user_id, "view_reports")
        
        system_metrics = metrics.get_system_metrics()
        
        return {
            "total_scans": system_metrics.total_scans,
            "successful_scans": system_metrics.successful_scans,
            "failed_scans": system_metrics.failed_scans,
            "total_matches": system_metrics.total_matches,
            "avg_duration": system_metrics.avg_duration,
            "error_rate": system_metrics.error_rate,
            "scans_by_adapter": dict(system_metrics.scans_by_adapter),
            "scans_by_user": dict(system_metrics.scans_by_user)
        }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Unexpected error in get_metrics: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@app.get("/api/v1/metrics/prometheus")
async def get_prometheus_metrics(user_id: str = Depends(get_current_user)):
    """Get metrics in Prometheus format"""
    try:
        check_permission(user_id, "view_reports")
        return metrics.export_prometheus()
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Unexpected error in get_prometheus_metrics: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@app.get("/api/v1/scans")
async def list_scans(user_id: str = Depends(get_current_user)):
    """List user's scans"""
    try:
        check_permission(user_id, "view_reports")
        
        user_scans = []
        for scan_id, scan_data in scan_requests.items():
            if scan_data["user_id"] == user_id or rbac.check_permission(user_id, "manage_users"):
                user_scans.append({
                    "scan_id": scan_id,
                    "url": scan_data["request"]["url"],
                    "status": scan_data["status"],
                    "start_time": scan_data["start_time"].isoformat(),
                    "matches_count": len(scan_results.get(scan_id, {}).get("matches", []))
                })
        
        return {"scans": user_scans}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Unexpected error in list_scans: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@app.post("/api/v1/auth/key")
async def generate_api_key(user_id: str = Depends(get_current_user)):
    """Generate new API key for user"""
    try:
        check_permission(user_id, "manage_users")
        
        api_key = authenticator.add_api_key(user_id)
        return {"api_key": api_key, "user_id": user_id}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Unexpected error in generate_api_key: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@app.get("/api/v1/health")
async def health_check():
    """Health check endpoint"""
    try:
        return {
            "status": "healthy", 
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0.0",
            "components": {
                "security": "ok",
                "rbac": "ok",
                "audit": "ok",
                "metrics": "ok",
                "reporting": "ok"
            }
        }
    except Exception as e:
        logging.error(f"Health check failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service unhealthy"
        )

# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all API requests"""
    start_time = time.time()
    
    try:
        # Log request
        audit.text_logger.logger.info(f"API_REQUEST {request.method} {request.url}")
        
        # Process request
        response = await call_next(request)
        
        # Log response
        duration = time.time() - start_time
        audit.text_logger.logger.info(f"API_RESPONSE {request.method} {request.url} {response.status_code} {duration:.3f}s")
        
        return response
    except Exception as e:
        # Log error
        duration = time.time() - start_time
        audit.text_logger.logger.error(f"API_ERROR {request.method} {request.url} {str(e)} {duration:.3f}s")
        raise

# User management endpoints
@app.post("/api/v1/users/assign-role")
async def assign_role(
    user_id: str = Body(...),
    role: str = Body(...),
    admin_id: str = Depends(get_current_user)
):
    """Assign role to user"""
    try:
        check_permission(admin_id, "manage_users")
        
        if not user_id or not role:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User ID and role are required"
            )
        
        ok = rbac.assign_role(user_id, role)
        if ok:
            audit.log_config_change(admin_id, "role_assignment", f"Assigned role {role} to user {user_id}")
            return {"status": "success", "user_id": user_id, "role": role}
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, 
                detail="Invalid role"
            )
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Unexpected error in assign_role: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@app.get("/api/v1/users")
async def list_users(admin_id: str = Depends(get_current_user)):
    """List all users"""
    try:
        check_permission(admin_id, "manage_users")
        return {"users": rbac.list_users()}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Unexpected error in list_users: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@app.get("/api/v1/roles")
async def list_roles(admin_id: str = Depends(get_current_user)):
    """List all roles"""
    try:
        check_permission(admin_id, "manage_users")
        return {"roles": rbac.list_roles()}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Unexpected error in list_roles: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@app.get("/api/v1/permissions")
async def list_permissions(admin_id: str = Depends(get_current_user)):
    """List all permissions"""
    try:
        check_permission(admin_id, "manage_users")
        return {"permissions": rbac.list_permissions()}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Unexpected error in list_permissions: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

def run_api_server(host: str = "0.0.0.0", port: int = 8000, debug: bool = False):
    """Run the API server"""
    app.debug = debug
    uvicorn.run(app, host=host, port=port, log_level="info")

if __name__ == "__main__":
    run_api_server() 