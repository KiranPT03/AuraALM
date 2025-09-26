from fastapi import APIRouter, HTTPException, status, Depends

from scripts.utils.logger import log
from scripts.config.application import config
from scripts.services.user_services import UserService
from scripts.models.request import UserProfile
from scripts.models.user import User
from scripts.services.jwt_dependancy import get_current_user


class UserRouter:
    def __init__(self, config):
        self.config = config
        self.user_router = APIRouter(
            prefix='/users',
            tags=['admin-users']
        )
        self.user_service = UserService(config)

    def register_routes(self):
        self.user_router.add_api_route("/", self.create_user, methods=['POST'], status_code=201)
        self.user_router.add_api_route("/{user_id}", self.get_user, methods=['GET'], status_code=200)
        self.user_router.add_api_route("/{user_id}", self.update_user, methods=['PUT'], status_code=200)
        self.user_router.add_api_route("/{user_id}", self.delete_user, methods=['DELETE'], status_code=204)
        self.user_router.add_api_route("/", self.get_users, methods=['GET'], status_code=200)
        return self.user_router

    async def create_user(self, user: User, logged_user: dict = Depends(get_current_user)):
        logged_user = UserProfile(**logged_user)
        response = self.user_service.create_user(logged_user, user)
        match response['status_code']:
            case status.HTTP_201_CREATED:
                return response            
            case _:
                raise HTTPException(status_code=response['status_code'], detail=response)

    async def get_user(self, user_id: str, logged_user: dict = Depends(get_current_user)):
        logged_user = UserProfile(**logged_user)
        response = self.user_service.get_user(logged_user, user_id)
        match response['status_code']:
            case status.HTTP_200_OK:
                return response            
            case _:
                raise HTTPException(status_code=response['status_code'], detail=response)

    async def update_user(self, user_id: str, user: User, logged_user: dict = Depends(get_current_user)):
        logged_user = UserProfile(**logged_user)
        response = self.user_service.update_user(logged_user,user, user_id)
        match response['status_code']:
            case status.HTTP_200_OK:
                return response            
            case _:
                raise HTTPException(status_code=response['status_code'], detail=response)

    async def delete_user(self, user_id: str, logged_user: dict = Depends(get_current_user)):
        logged_user = UserProfile(**logged_user)
        response = self.user_service.delete_user(logged_user, user_id)
        match response['status_code']:
            case status.HTTP_204_NO_CONTENT:
                return response            
            case _:
                raise HTTPException(status_code=response['status_code'], detail=response)

    async def get_users(self, logged_user: dict = Depends(get_current_user)):
        log.info(f"get_users: {logged_user}")
        logged_user = UserProfile(**logged_user)
        response = self.user_service.get_users(logged_user)
        match response['status_code']:
            case status.HTTP_200_OK:
                return response            
            case _:
                raise HTTPException(status_code=response['status_code'], detail=response)

ur = UserRouter(config)
user_router = ur.register_routes()
