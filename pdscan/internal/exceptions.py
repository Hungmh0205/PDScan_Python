"""Exceptions for PDScan."""

class ScanError(Exception):
    """Base exception for scanning errors."""
    pass

class ConnectionError(ScanError):
    """Error connecting to data store."""
    pass

class AuthenticationError(ScanError):
    """Error authenticating with data store."""
    pass

class PermissionError(ScanError):
    """Error accessing data store."""
    pass

class TimeoutError(ScanError):
    """Error due to timeout."""
    pass

class ValidationError(ScanError):
    """Error validating input."""
    pass 