from typing import Optional, List
from pydantic import BaseModel, Field, EmailStr


class TokenPayload(BaseModel):
    user_id: str
    roles: List[str]
    token_type: str
    iat: int
    exp: int
    iss: str
    aud: str
    org_id: str
    business_units: List[str]

class UserProfile(BaseModel):
    user_id: str
    roles: List[str]
    org_id: str
    business_units: List[str]
    token_payload: TokenPayload