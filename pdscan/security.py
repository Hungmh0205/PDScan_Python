"""
Security framework for PDScan
"""

import os
import base64
import hashlib
import hmac
import time
from typing import Optional, Dict, Any
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import json
import copy

class SecurityManager:
    """Security manager for authentication and encryption"""
    
    def __init__(self, secret_key: Optional[str] = None):
        self.secret_key = secret_key or os.getenv('PDSCAN_SECRET_KEY')
        if not self.secret_key:
            # Generate a new key if none provided
            self.secret_key = Fernet.generate_key().decode()
        self.fernet = self._create_fernet()
        
    def _create_fernet(self) -> Fernet:
        """Create Fernet cipher from secret key"""
        if isinstance(self.secret_key, str):
            key = self.secret_key.encode()
        else:
            key = self.secret_key
            
        # Ensure key is 32 bytes for Fernet
        if len(key) != 32:
            # Derive key using PBKDF2
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=b'pdscan_salt',
                iterations=100000,
            )
            key = base64.urlsafe_b64encode(kdf.derive(key))
            
        return Fernet(key)
    
    def encrypt_config(self, config: Dict[str, Any]) -> str:
        """Encrypt sensitive configuration data"""
        config_str = json.dumps(config)
        encrypted = self.fernet.encrypt(config_str.encode())
        return base64.urlsafe_b64encode(encrypted).decode()
    
    def decrypt_config(self, encrypted_config: str) -> Dict[str, Any]:
        """Decrypt configuration data"""
        encrypted = base64.urlsafe_b64decode(encrypted_config.encode())
        decrypted = self.fernet.decrypt(encrypted)
        return json.loads(decrypted.decode())
    
    def hash_password(self, password: str) -> str:
        """Hash password using SHA-256"""
        return hashlib.sha256(password.encode()).hexdigest()
    
    def verify_password(self, password: str, hashed: str) -> bool:
        """Verify password against hash"""
        return self.hash_password(password) == hashed
    
    def generate_api_key(self, user_id: str) -> str:
        """Generate API key for user"""
        timestamp = str(int(time.time()))
        data = f"{user_id}:{timestamp}"
        signature = hmac.new(
            self.secret_key.encode(),
            data.encode(),
            hashlib.sha256
        ).hexdigest()
        return f"{data}:{signature}"
    
    def verify_api_key(self, api_key: str, user_id: str) -> bool:
        """Verify API key"""
        try:
            parts = api_key.split(':')
            if len(parts) != 3:
                return False
                
            key_user_id, timestamp, signature = parts
            
            # Check if user_id matches
            if key_user_id != user_id:
                return False
                
            # Check if key is not expired (24 hours)
            key_time = int(timestamp)
            current_time = int(time.time())
            if current_time - key_time > 86400:  # 24 hours
                return False
                
            # Verify signature
            data = f"{key_user_id}:{timestamp}"
            expected_signature = hmac.new(
                self.secret_key.encode(),
                data.encode(),
                hashlib.sha256
            ).hexdigest()
            
            return hmac.compare_digest(signature, expected_signature)
            
        except (ValueError, IndexError):
            return False
    
    def mask_sensitive_data(self, data: str, pattern: str = r'password|secret|key|token') -> str:
        """Mask sensitive data in strings"""
        import re
        return re.sub(pattern, '***', data, flags=re.IGNORECASE)

class Authenticator:
    """Authentication handler"""
    
    def __init__(self, security_manager: SecurityManager):
        self.security = security_manager
        self.api_keys = {}  # In production, use database
        
    def authenticate_api_key(self, api_key: str) -> Optional[str]:
        """Authenticate using API key, returns user_id if valid"""
        for user_id, stored_key in self.api_keys.items():
            if self.security.verify_api_key(api_key, user_id):
                return user_id
        return None
    
    def add_api_key(self, user_id: str) -> str:
        """Add new API key for user"""
        api_key = self.security.generate_api_key(user_id)
        self.api_keys[user_id] = api_key
        return api_key
    
    def remove_api_key(self, user_id: str) -> bool:
        """Remove API key for user"""
        if user_id in self.api_keys:
            del self.api_keys[user_id]
            return True
        return False

class ConfigEncryption:
    """Configuration encryption utilities"""
    
    def __init__(self, security_manager: SecurityManager):
        self.security = security_manager
    
    def encrypt_sensitive_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Encrypt sensitive parts of configuration"""
        encrypted_config = config.copy()
        
        # Encrypt database passwords
        if 'database' in config and 'connections' in config['database']:
            for conn in config['database']['connections']:
                if 'password' in conn:
                    conn['password'] = self.security.encrypt_config({'password': conn['password']})
        
        # Encrypt API keys
        if 'api' in config and 'keys' in config['api']:
            for key in config['api']['keys']:
                if 'secret' in key:
                    key['secret'] = self.security.encrypt_config({'secret': key['secret']})
        
        return encrypted_config
    
    def decrypt_sensitive_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Decrypt sensitive parts of configuration"""
        decrypted_config = copy.deepcopy(config)
        
        # Decrypt database passwords
        if 'database' in decrypted_config and 'connections' in decrypted_config['database']:
            for conn in decrypted_config['database']['connections']:
                if 'password' in conn and isinstance(conn['password'], str) and conn['password'].startswith('gAAAAA'):
                    try:
                        decrypted = self.security.decrypt_config(conn['password'])
                        conn['password'] = decrypted.get('password', conn['password'])
                    except Exception:
                        pass  # Keep encrypted if decryption fails
        
        # Decrypt API keys
        if 'api' in decrypted_config and 'keys' in decrypted_config['api']:
            for key in decrypted_config['api']['keys']:
                if 'secret' in key and isinstance(key['secret'], str) and key['secret'].startswith('gAAAAA'):
                    try:
                        decrypted = self.security.decrypt_config(key['secret'])
                        key['secret'] = decrypted.get('secret', key['secret'])
                    except Exception:
                        pass  # Keep encrypted if decryption fails
        
        return decrypted_config 