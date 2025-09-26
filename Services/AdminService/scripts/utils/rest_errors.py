from typing import Dict, Any, List, Optional, Union
from scripts.models.response import ResponseData as ResponseDataModel, ErrorDetail


class RestErrors:
    
    # 2xx Success Responses
    @staticmethod
    def success_200(message: str, data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None, errors: List[ErrorDetail] = []) -> Dict[str, Any]:
        """HTTP 200 OK - Standard success response"""
        response = ResponseDataModel(
            success=True,
            status_code=200,
            message=message,
            data=data,
            errors=[]
        )
        return response.model_dump()
    
    @staticmethod
    def created_201(message: str, data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None, errors: List[ErrorDetail] = []) -> Dict[str, Any]:
        """HTTP 201 Created - Resource successfully created"""
        response = ResponseDataModel(
            success=True,
            status_code=201,
            message=message,
            data=data,
            errors=[]
        )
        return response.model_dump()
    
    @staticmethod
    def accepted_202(message: str, data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None, errors: List[ErrorDetail] = []) -> Dict[str, Any]:
        """HTTP 202 Accepted - Request accepted for processing"""
        response = ResponseDataModel(
            success=True,
            status_code=202,
            message=message,
            data=data,
            errors=[]
        )
        return response.model_dump()
    
    @staticmethod
    def no_content_204(message: str, data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None, errors: List[ErrorDetail] = []) -> Dict[str, Any]:
        """HTTP 204 No Content - Success with no response body"""
        response = ResponseDataModel(
            success=True,
            status_code=204,
            message=message,
            data=data,
            errors=errors
        )
        return response.model_dump()
    
    # 4xx Client Error Responses
    @staticmethod
    def bad_request_400(message: str, data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None, errors: List[ErrorDetail] = []) -> Dict[str, Any]:
        """HTTP 400 Bad Request - Invalid request syntax"""
        response = ResponseDataModel(
            success=False,
            status_code=400,
            message=message,
            data=data,
            errors=errors
        )
        return response.model_dump()
    
    @staticmethod
    def unauthorized_401(message: str, data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None, errors: List[ErrorDetail] = []) -> Dict[str, Any]:
        """HTTP 401 Unauthorized - Authentication required"""
        response = ResponseDataModel(
            success=False,
            status_code=401,
            message=message,
            data=data,
            errors=errors       
        )
        return response.model_dump()
    
    @staticmethod
    def forbidden_403(message: str, data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None, errors: List[ErrorDetail] = []) -> Dict[str, Any]:
        """HTTP 403 Forbidden - Access denied"""
        response = ResponseDataModel(
            success=False,
            status_code=403,
            message=message,
            data=data,
            errors=errors       
        )
        return response.model_dump()
    
    @staticmethod
    def not_found_404(message: str, data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None, errors: List[ErrorDetail] = []) -> Dict[str, Any]:
        """HTTP 404 Not Found - Resource not found"""
        response = ResponseDataModel(
            success=False,
            status_code=404,
            message=message,
            data=data,
            errors=errors
        )
        return response.model_dump()
    
    @staticmethod
    def unprocessable_entity_422(message: str, data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None, errors: List[ErrorDetail] = None) -> Dict[str, Any]:
        """HTTP 422 Unprocessable Entity - Validation error"""
        response = ResponseDataModel(
            success=False,
            status_code=422,
            message=message,
            data=data,
            errors=errors or []
        )
        return response.model_dump()
    
    @staticmethod
    def too_many_requests_429(message: str, data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None, errors: List[ErrorDetail] = []) -> Dict[str, Any]:
        """HTTP 429 Too Many Requests - Rate limit exceeded"""
        response = ResponseDataModel(
            success=False,
            status_code=429,
            message=message,
            data=data,
            errors=errors
        )
        return response.model_dump()
    
    # 5xx Server Error Responses
    @staticmethod
    def internal_server_error_500(message: str, data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None, errors: List[ErrorDetail] = []) -> Dict[str, Any]:
        """HTTP 500 Internal Server Error - Server encountered an error"""
        response = ResponseDataModel(
            success=False,
            status_code=500,
            message=message,
            data=data,
            errors=errors
        )
        return response.model_dump()
    
    @staticmethod
    def bad_gateway_502(message: str, data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None, errors: List[ErrorDetail] = []) -> Dict[str, Any]:
        """HTTP 502 Bad Gateway - Invalid response from upstream server"""
        response = ResponseDataModel(
            success=False,
            status_code=502,
            message=message,
            data=data,
            errors=errors
        )
        return response.model_dump()
    
    @staticmethod
    def service_unavailable_503(message: str, data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None, errors: List[ErrorDetail] = []) -> Dict[str, Any]:
        """HTTP 503 Service Unavailable - Server temporarily unavailable"""
        response = ResponseDataModel(
            success=False,
            status_code=503,
            message=message,
            data=data,
            errors=errors
        )
        return response.model_dump()
    
    @staticmethod
    def gateway_timeout_504(message: str, data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None, errors: List[ErrorDetail] = []) -> Dict[str, Any]:
        """HTTP 504 Gateway Timeout - Upstream server timeout"""
        response = ResponseDataModel(
            success=False,
            status_code=504,
            message=message,
            data=data,
            errors=errors
        )
        return response.model_dump()
    
    # Helper method for custom responses with validation errors
    @staticmethod
    def validation_error_422_with_details(message: str, validation_errors: List[Dict[str, str]], data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None, errors: List[ErrorDetail] = []) -> Dict[str, Any]:
        """HTTP 422 with detailed validation errors"""
        error_details = [
            ErrorDetail(
                code=error.get('code', 'VALIDATION_ERROR'),
                message=error.get('message', ''),
                field=error.get('field', '')
            )
            for error in validation_errors
        ]
        
        response = ResponseDataModel(
            success=False,
            status_code=422,
            message=message,
            data=data,
            errors=error_details
        )
        return response.model_dump()