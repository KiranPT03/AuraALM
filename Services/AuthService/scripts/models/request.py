from typing import Optional
from pydantic import BaseModel, Field, EmailStr

class LoginRequest(BaseModel):
    """
    A data model for a login request.
    """
    email: EmailStr = Field(..., description="The user's email address, which must be a valid email format.")
    password: str = Field(..., min_length=8, description="The user's password. Must be at least 8 characters long.")