from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel


class ErrorDetail(BaseModel):
    code: str
    message: str
    field: str


class ResponseData(BaseModel):
    success: bool
    status_code: int
    message: str
    data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]
    errors: List[ErrorDetail]

class AuthResponse(BaseModel):
    access_token: str
    token_type: str
    expires_in: int
    refresh_token: str