import yaml
import os
from typing import Dict, Any

class ApplicationConfig:
    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), 'application.yaml')
        
        self.config_path = config_path
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                return yaml.safe_load(file) or {}
        except FileNotFoundError:
            print(f"Warning: Configuration file {self.config_path} not found. Using defaults.")
            return {}
        except yaml.YAMLError as e:
            print(f"Error parsing YAML configuration: {e}")
            return {}
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration"""
        return self._config.get('logging', {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(funcName)s:%(lineno)d] - %(message)s',
            'date_format': '%Y-%m-%d %H:%M:%S',
            'handlers': {
                'console': {'enabled': True, 'level': 'DEBUG'},
                'file': {'enabled': False}
            }
        })

    def get_fastapi_config(self) -> Dict[str, Any]:
        """Get FastAPI configuration"""
        return self._config.get('fastapi', {
            'host': '0.0.0.0',
            'port': 8000,
            'debug': False,
            'reload': False,
            'workers': 1,
            'title': 'Automator API',
            'description': 'Automator Services API',
            'version': '1.0.0',
            'docs_url': '/docs',
            'redoc_url': '/redoc',
            'openapi_url': '/openapi.json',
            'cors': {
                'allow_origins': ['*'],
                'allow_credentials': True,
                'allow_methods': ['*'],
                'allow_headers': ['*']
            }
        })

    def get_application_config(self) -> Dict[str, Any]:
        """Get application configuration"""
        return self._config.get('application', {
            'name': 'Automator'
        })

    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration"""
        return self._config.get('database', {
            'mongodb': {
                'host': 'localhost',
                'port': 27017,
                'database': 'automator_db',
                'username': 'user_automator',
                'password': 'p@ssw0rd@Automator',
                'auth_source': 'admin',
                'replica_set': '',
                'ssl': False,
                'ssl_cert_reqs': 'CERT_NONE',
                'max_pool_size': 100,
                'min_pool_size': 0,
                'max_idle_time_ms': 0,
                'connect_timeout_ms': 20000,
                'server_selection_timeout_ms': 30000,
                'socket_timeout_ms': 0,
                'heartbeat_frequency_ms': 10000
            }
        })

    def get_mongodb_config(self) -> Dict[str, Any]:
        """Get MongoDB configuration"""
        return self._config.get('database', {}).get('mongodb', {
            'host': 'localhost',
            'port': 27017,
            'database': 'automator_db',
            'username': 'user_automator',
            'password': 'p@ssw0rd@Automator',
            'auth_source': 'admin',
            'replica_set': '',
            'ssl': False,
            'ssl_cert_reqs': 'CERT_NONE',
            'max_pool_size': 100,
            'min_pool_size': 0,
            'max_idle_time_ms': 0,
            'connect_timeout_ms': 20000,
            'server_selection_timeout_ms': 30000,
            'socket_timeout_ms': 0,
            'heartbeat_frequency_ms': 10000
        })

    def get_security_config(self) -> dict:
        """Get security configuration"""
        return self._config.get('security', {
            'bcrypt_salt_rounds': 12,
            'password_min_length': 8,
            'password_max_length': 128,
            'session_timeout_minutes': 30,
            'max_login_attempts': 5,
            'lockout_duration_minutes': 15,
            'jwt': {
                'secret_key': 'default-secret-key',
                'algorithm': 'HS256',
                'access_token_expire_minutes': 30,
                'refresh_token_expire_days': 7,
                'issuer': 'automator-api',
                'audience': 'automator-users'
            }
        })
    
    def get_jwt_config(self) -> dict:
        """Get JWT configuration"""
        security_config = self.get_security_config()
        return security_config.get('jwt', {
            'secret_key': 'default-secret-key',
            'algorithm': 'HS256',
            'access_token_expire_minutes': 30,
            'refresh_token_expire_days': 7,
            'issuer': 'automator-api',
            'audience': 'automator-users'
        })

config = ApplicationConfig()