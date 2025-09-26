from typing import Optional, List
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from scripts.services.jwt import JWTService
from scripts.utils.logger import log
from scripts.config.application import config


security = HTTPBearer()
jwt_service = JWTService(config)


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """
    Dependency to get current user from JWT token
    
    Args:
        credentials: HTTP Authorization credentials
        
    Returns:
        User information from token including org_id and business_units
        
    Raises:
        HTTPException: If token is invalid or expired
    """
    try:
        token = credentials.credentials
        payload = jwt_service.validate_access_token(token)
        return {
            'user_id': payload.get('user_id'),
            'roles': payload.get('roles', []),
            'org_id': payload.get('org_id'),
            'business_units': payload.get('business_units'),
            'token_payload': payload
        }
    except Exception as e:
        log.warning(f"Authentication failed: {str(e)}")

        error_response = {
                    "success": False,
                    "status_code": 401,
                    "message": "Invalid authentication credentials",
                    "data": {
                        "token": token
                    },
                    "errors": [
                        {
                            "code": "INVALID_TOKEN",
                            "message": "Token has expired"
                        }
                    ]
                }      
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=error_response,
            
        )


def require_roles(required_roles: List[str]):
    """
    Dependency factory to require specific roles
    
    Args:
        required_roles: List of required roles
        
    Returns:
        Dependency function
    """
    def role_checker(current_user: dict = Depends(get_current_user)) -> dict:
        user_roles = current_user.get('roles', [])
        
        if not any(role in user_roles for role in required_roles):
            log.warning(f"Access denied for user {current_user.get('user_id')}: insufficient roles")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions"
            )
        
        return current_user
    
    return role_checker


def require_organization(required_org_id: str):
    """
    Dependency factory to require specific organization
    
    Args:
        required_org_id: Required organization ID
        
    Returns:
        Dependency function
    """
    def org_checker(current_user: dict = Depends(get_current_user)) -> dict:
        user_org_id = current_user.get('org_id')
        
        if not user_org_id or user_org_id != required_org_id:
            log.warning(f"Access denied for user {current_user.get('user_id')}: wrong organization")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied: wrong organization"
            )
        
        return current_user
    
    return org_checker


def require_business_units(required_business_units: List[str]):
    """
    Dependency factory to require specific business units
    
    Args:
        required_business_units: Required business units IDs
        
    Returns:
        Dependency function
    """
    def bu_checker(current_user: dict = Depends(get_current_user)) -> dict:
        user_business_units = current_user.get('business_units', [])
        
        if not user_business_units or not any(bu in user_business_units for bu in required_business_units):
            log.warning(f"Access denied for user {current_user.get('user_id')}: wrong business units")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied: wrong business units"
            )
        
        return current_user
    
    return bu_checker


def require_org_and_roles(required_org_id: str, required_roles: List[str]):
    """
    Dependency factory to require both organization and specific roles
    
    Args:
        required_org_id: Required organization ID
        required_roles: List of required roles
        
    Returns:
        Dependency function
    """
    def org_role_checker(current_user: dict = Depends(get_current_user)) -> dict:
        user_org_id = current_user.get('org_id')
        user_roles = current_user.get('roles', [])
        
        # Check organization
        if not user_org_id or user_org_id != required_org_id:   
            log.warning(f"Access denied for user {current_user.get('user_id')}: wrong organization")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied: wrong organization"
            )
        
        # Check roles
        if not any(role in user_roles for role in required_roles):
            log.warning(f"Access denied for user {current_user.get('user_id')}: insufficient roles")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions"
            )
        
        return current_user
    
    return org_role_checker


def require_admin(current_user: dict = Depends(get_current_user)) -> dict:
    """
    Dependency to require admin role
    
    Args:
        current_user: Current user information
        
    Returns:
        User information if admin
        
    Raises:
        HTTPException: If user is not admin
    """
    user_roles = current_user.get('roles', [])
    
    if 'admin' not in user_roles:
        log.warning(f"Admin access denied for user {current_user.get('user_id')}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    
    return current_user


def optional_auth(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> Optional[dict]:
    """
    Optional authentication dependency
    
    Args:
        credentials: Optional HTTP Authorization credentials
        
    Returns:
        User information if authenticated, None otherwise
    """
    if not credentials:
        return None
    
    try:
        token = credentials.credentials
        payload = jwt_service.validate_access_token(token)
        return {
            'user_id': payload.get('user_id'),
            'roles': payload.get('roles', []),
            'org_id': payload.get('org_id'),
            'business_units': payload.get('business_units'),
            'token_payload': payload
        }
    except Exception as e:
        log.debug(f"Optional authentication failed: {str(e)}")
        return None