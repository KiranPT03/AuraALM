from typing import Optional, List, Dict, Any, Union 
from datetime import datetime, date 
from pydantic import BaseModel, EmailStr, Field, HttpUrl, validator 
 
class Address(BaseModel): 
    """Represents a physical address.""" 
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None

    
class Profile(BaseModel): 
    """Represents a user's profile information.""" 
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    bio: Optional[str] = None 
    date_of_birth: Optional[date] = None 
    profile_picture_url: Optional[HttpUrl] = None 
    phone_number: Optional[str] = None 
    gender: Optional[str] = None 
    locale: Optional[str] = 'en-US'  # User's language and region 
    timezone: Optional[str] = None

 
class SocialProfile(BaseModel): 
    """Represents a user's social media links.""" 
    platform: Optional[str] = None
    url: Optional[HttpUrl] = None
    handle: Optional[str] = None 
 
class Preferences(BaseModel): 
    """Represents a user's preferences.""" 
    theme: Optional[str] = 'light'  # e.g., 'light', 'dark' 
    notifications_enabled: Optional[bool] = True 
    email_notifications_enabled: Optional[bool] = True 
    is_public: Optional[bool] = True 
    content_language: Optional[str] = 'en' 
     
class Security(BaseModel): 
    """Represents user security and authentication data.""" 
    is_email_verified: Optional[bool] = False 
    is_phone_verified: Optional[bool] = False 
    password_hash: Optional[str] = None 
    last_login: Optional[datetime] = None 
    mfa_enabled: Optional[bool] = False  # Multi-factor authentication 
    recovery_codes: Optional[List[str]] = None 
 
class Membership(BaseModel): 
    """Represents a user's membership or subscription status.""" 
    status: Optional[str] = 'free_tier' # e.g., 'free_tier', 'premium', 'pro' 
    start_date: Optional[datetime] = None 
    end_date: Optional[datetime] = None 
 
class User(BaseModel): 
    """ 
    A comprehensive user data model with detailed fields. 
    """ 
    user_id: Optional[str] = Field(None, description="Unique identifier for the user.") 
    email: Optional[EmailStr] = Field(None, description="User's primary email address.") 
    username: Optional[str] = Field(None, description="User's unique username.") 
     
    # Nested models for detailed information 
    profile: Optional[Profile] = None
    address: Optional[Address] = None 
    preferences: Optional[Preferences] = None
    security: Optional[Security] = None
    org_id: Optional[str] = None
    business_units: Optional[List[str]] = None
    membership: Optional[Membership] = None
     
    # Lists and dictionaries for dynamic data 
    social_profiles: Optional[List[SocialProfile]] = [] 
    roles: Optional[List[str]] = []  # e.g., 'admin', 'moderator', 'editor' 
    groups: Optional[List[str]] = [] 
    tags: Optional[List[str]] = [] # e.g., 'new_user', 'vip' 
    metadata: Optional[Dict[str, Any]] = {} # A catch-all for custom data 
     
    # Timestamps 
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow) 
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow) 
     
    # Status and flags 
    is_active: Optional[bool] = True 
    is_banned: Optional[bool] = False 
    is_suspended: Optional[bool] = False
    is_logged_in: Optional[bool] = False
 
    @validator('updated_at', pre=True, always=True) 
    def set_updated_at(cls, v): 
        """Validator to automatically update the timestamp.""" 
        return datetime.utcnow()
