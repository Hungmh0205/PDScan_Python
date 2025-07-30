import os
import yaml
from typing import Any, Dict

class ConfigError(Exception):
    pass

class PDScanConfig:
    def __init__(self, config_path: str = None):
        self.config_path = config_path or os.environ.get('PDSCAN_CONFIG', 'config/pdscan.yaml')
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        if not os.path.exists(self.config_path):
            raise ConfigError(f"Config file not found: {self.config_path}")
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        # Override bằng env nếu có
        for key, value in os.environ.items():
            if key.startswith('PDSCAN_'):
                # Ví dụ: PDSCAN_DB_URL -> db_url
                config_key = key[7:].lower()
                config[config_key] = value
        self._validate_config(config)
        return config

    def _validate_config(self, config: Dict[str, Any]):
        # TODO: Thêm validate chi tiết hơn
        if 'database' not in config:
            raise ConfigError('Missing database config')
        if 'connections' not in config['database']:
            raise ConfigError('Missing database.connections config')

    def get(self, key: str, default=None):
        return self.config.get(key, default)

    def get_db_connections(self):
        return self.config['database']['connections']

    def get_webhook_config(self):
        return self.config.get('webhook', {})

    def get_email_config(self):
        return self.config.get('email', {})

    def get_slack_config(self):
        return self.config.get('slack', {}) 