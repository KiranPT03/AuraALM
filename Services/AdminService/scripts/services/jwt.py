from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import jwt
from jwt.exceptions import InvalidTokenError, ExpiredSignatureError
from scripts.utils.logger import log


class JWTService:
    def __init__(self, config):
        self.config = config
        self.jwt_config = self.config.get_jwt_config()
        self.secret_key = self.jwt_config['secret_key']
        self.algorithm = self.jwt_config['algorithm']
        self.access_token_expire_minutes = self.jwt_config['access_token_expire_minutes']
        self.refresh_token_expire_days = self.jwt_config['refresh_token_expire_days']
        self.issuer = self.jwt_config['issuer']
        self.audience = self.jwt_config['audience']
    
    def create_access_token(self, user_id: str, roles: List[str], org_id: Optional[str] = None, business_units: Optional[List[str]] = None, additional_claims: Optional[Dict[str, Any]] = None) -> str:
        """
        Create JWT access token
        
        Args:
            user_id: User identifier
            roles: List of user roles
            organization: Organization identifier
            business_units: Business units identifiers
            additional_claims: Optional additional claims to include
            
        Returns:
            JWT access token string
        """
        try:
            now = datetime.utcnow()
            expire = now + timedelta(minutes=self.access_token_expire_minutes)
            
            payload = {
                'user_id': user_id,
                'roles': roles,
                'token_type': 'access',
                'iat': now,
                'exp': expire,
                'iss': self.issuer,
                'aud': self.audience
            }
            
            # Add organization and business_units if provided
            if org_id:
                payload['org_id'] = org_id
            if business_units:
                payload['business_units'] = business_units  
            
            if additional_claims:
                payload.update(additional_claims)
            
            token = jwt.encode(payload, self.secret_key, algorithm=self.algorithm)
            log.debug(f"Access token created for user: {user_id}")
            return token
            
        except Exception as e:
            log.error(f"Error creating access token: {str(e)}")
            raise Exception(f"Failed to create access token: {str(e)}")
    
    def create_refresh_token(self, user_id: str, org_id: Optional[str] = None, business_units: Optional[List[str]] = None, additional_claims: Optional[Dict[str, Any]] = None) -> str:
        """
        Create JWT refresh token
        
        Args:
            user_id: User identifier
            organization: Organization identifier
            business_units: Business units identifiers
            additional_claims: Optional additional claims to include
            
        Returns:
            JWT refresh token string
        """
        try:
            now = datetime.utcnow()
            expire = now + timedelta(days=self.refresh_token_expire_days)
            
            payload = {
                'user_id': user_id,
                'token_type': 'refresh',
                'iat': now,
                'exp': expire,
                'iss': self.issuer,
                'aud': self.audience
            }
            
            # Add organization and business_units if provided
            if org_id:
                payload['org_id'] = org_id
            if business_units:
                payload['business_units'] = business_units
            
            if additional_claims:
                payload.update(additional_claims)
            
            token = jwt.encode(payload, self.secret_key, algorithm=self.algorithm)
            log.debug(f"Refresh token created for user: {user_id}")
            return token
            
        except Exception as e:
            log.error(f"Error creating refresh token: {str(e)}")
            raise Exception(f"Failed to create refresh token: {str(e)}")
    
    def validate_access_token(self, token: str) -> Dict[str, Any]:
        """
        Validate JWT access token
        
        Args:
            token: JWT token string
            
        Returns:
            Decoded token payload
            
        Raises:
            Exception: If token is invalid or expired
        """
        try:
            payload = jwt.decode(
                token, 
                self.secret_key, 
                algorithms=[self.algorithm],
                audience=self.audience,
                issuer=self.issuer
            )
            
            if payload.get('token_type') != 'access':
                raise Exception("Invalid token type")
            
            log.debug(f"Access token validated for user: {payload.get('user_id')}")
            return payload
            
        except ExpiredSignatureError:
            log.warning("Access token has expired")
            raise Exception("Token has expired")
        except InvalidTokenError as e:
            log.warning(f"Invalid access token: {str(e)}")
            raise Exception("Invalid token")
        except Exception as e:
            log.error(f"Error validating access token: {str(e)}")
            raise Exception(f"Token validation failed: {str(e)}")
    
    def validate_refresh_token(self, token: str) -> Dict[str, Any]:
        """
        Validate JWT refresh token
        
        Args:
            token: JWT token string
            
        Returns:
            Decoded token payload
            
        Raises:
            Exception: If token is invalid or expired
        """
        try:
            payload = jwt.decode(
                token, 
                self.secret_key, 
                algorithms=[self.algorithm],
                audience=self.audience,
                issuer=self.issuer
            )
            
            if payload.get('token_type') != 'refresh':
                raise Exception("Invalid token type")
            
            log.debug(f"Refresh token validated for user: {payload.get('user_id')}")
            return payload
            
        except ExpiredSignatureError:
            log.warning("Refresh token has expired")
            raise Exception("Refresh token has expired")
        except InvalidTokenError as e:
            log.warning(f"Invalid refresh token: {str(e)}")
            raise Exception("Invalid refresh token")
        except Exception as e:
            log.error(f"Error validating refresh token: {str(e)}")
            raise Exception(f"Refresh token validation failed: {str(e)}")
    
    def refresh_access_token(self, refresh_token: str, roles: List[str]) -> str:
        """
        Create new access token using refresh token
        
        Args:
            refresh_token: Valid refresh token
            roles: Updated user roles
            
        Returns:
            New access token
            
        Raises:
            Exception: If refresh token is invalid
        """
        try:
            payload = self.validate_refresh_token(refresh_token)
            user_id = payload.get('user_id')
            
            if not user_id:
                raise Exception("Invalid refresh token payload")
            
            new_access_token = self.create_access_token(user_id, roles)
            log.info(f"Access token refreshed for user: {user_id}")
            return new_access_token
            
        except Exception as e:
            log.error(f"Error refreshing access token: {str(e)}")
            raise Exception(f"Failed to refresh access token: {str(e)}")