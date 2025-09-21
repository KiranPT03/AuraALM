from fastapi import APIRouter, HTTPException, status, Depends

from scripts.utils.logger import log
from scripts.config.application import config
from scripts.services.auth_services import AuthorizationService
from scripts.models.request import LoginRequest
from scripts.models.user import User
from scripts.services.jwt_dependancy import get_current_user


class AuthorizationRouter:
    def __init__(self, config):
        self.config = config
        self.auth_router = APIRouter(
            prefix='/auth',
            tags=['auth']
        )
        self.auth_service = AuthorizationService(config)

    def register_routes(self):
        self.auth_router.add_api_route("/login", self.login, methods=['POST'], status_code=200)
        self.auth_router.add_api_route("/refresh", self.refresh_token, methods=['POST'], status_code=200)
        self.auth_router.add_api_route("/logout", self.logout, methods=['DELETE'], status_code=204)
        self.auth_router.add_api_route("/register", self.register, methods=['POST'], status_code=201)
        self.auth_router.add_api_route("/me", self.get_me, methods=['GET'], status_code=200)
        return self.auth_router
    
    async def login(self, login_request: LoginRequest):
        """
        Login a user
        """
        log.debug("router auth request: {}".format(login_request.model_dump()))
        
        response = self.auth_service.login(login_request)

        match response['status_code']:
            case status.HTTP_200_OK:
                return response            
            case _:
                raise HTTPException(status_code=response['status_code'], detail=response)

    async def register(self, user: User):
        log.info("Router auth request: {}".format(user.model_dump()))
        response = self.auth_service.register(user)
        match response['status_code']:
            case status.HTTP_201_CREATED:
                return response            
            case _:
                raise HTTPException(status_code=response['status_code'], detail=response)
    
    async def logout(self, access_payload = Depends(get_current_user)):
        log.info("router auth request: {}".format(access_payload))
        response =  self.auth_service.logout(access_payload)
        match response['status_code']:
            case status.HTTP_204_NO_CONTENT:
                return response            
            case _:
                raise HTTPException(status_code=response['status_code'], detail=response)

    async def refresh_token(self, access_payload = Depends(get_current_user)):
        log.info("router auth request: {}".format(access_payload))
        return self.auth_service.refresh_token()
    
    async def get_me(self, access_payload = Depends(get_current_user)):
        log.info("router auth request: {}".format(access_payload))
        response = self.auth_service.get_me(access_payload)
        match response['status_code']:
            case status.HTTP_200_OK:
                return response            
            case _:
                raise HTTPException(status_code=response['status_code'], detail=response)

ar = AuthorizationRouter(config)
auth_router= ar.register_routes()
