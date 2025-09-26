from fastapi import APIRouter, HTTPException, status, Depends
from typing import Optional

from scripts.utils.logger import log
from scripts.config.application import config
from scripts.models.request import UserProfile
from scripts.models.project import Project
from scripts.models.module import Module
from scripts.services.jwt_dependancy import get_current_user
# from scripts.services.project_services import ProjectService  # TODO: Create this service


class ProjectRouter:
    def __init__(self, config):
        self.config = config
        self.project_router = APIRouter(
            prefix='/projects',
            tags=['admin-projects']
        )
        # Note: Project service will need to be created
        # self.project_service = ProjectService(config)  # TODO: Create this service

    def register_routes(self):
        # Project CRUD routes
        self.project_router.add_api_route("/", self.create_project, methods=['POST'], status_code=201)
        self.project_router.add_api_route("/{project_id}", self.get_project, methods=['GET'], status_code=200)
        self.project_router.add_api_route("/{project_id}", self.update_project, methods=['PUT'], status_code=200)
        self.project_router.add_api_route("/{project_id}", self.delete_project, methods=['DELETE'], status_code=204)
        self.project_router.add_api_route("/", self.get_projects, methods=['GET'], status_code=200)
        # self.project_router.add_api_route("/{project_id}/modules", self.get_project_modules, methods=['GET'], status_code=200)
        
        # Module CRUD routes under projects
        self.project_router.add_api_route("/{project_id}/modules", self.create_module, methods=['POST'], status_code=201)
        self.project_router.add_api_route("/{project_id}/modules/{module_id}", self.get_module, methods=['GET'], status_code=200)
        self.project_router.add_api_route("/{project_id}/modules/{module_id}", self.update_module, methods=['PUT'], status_code=200)
        self.project_router.add_api_route("/{project_id}/modules/{module_id}", self.delete_module, methods=['DELETE'], status_code=204)
        self.project_router.add_api_route("/{project_id}/modules", self.get_modules, methods=['GET'], status_code=200)
        
        return self.project_router

    # Project CRUD operations
    async def create_project(self, project: Project, logged_user: dict = Depends(get_current_user)):
        """Create a new project"""
        log.debug(f"logged user: {logged_user}")
        logged_user = UserProfile(**logged_user)
        log.info(f"Creating project by user: {logged_user.user_id}")
        
        # TODO: Implement project service
        # response = self.project_service.create_project(logged_user, project)
        # match response['status_code']:
        #     case status.HTTP_201_CREATED:
        #         return response            
        #     case _:
        #         raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED, 
            detail="Project service not yet implemented"
        )

    async def get_project(self, project_id: str, logged_user: dict = Depends(get_current_user)):
        """Get project by ID"""
        logged_user = UserProfile(**logged_user)
        log.info(f"Getting project {project_id} by user: {logged_user.user_id}")
        
        # TODO: Implement project service
        # response = self.project_service.get_project(logged_user, project_id)
        # match response['status_code']:
        #     case status.HTTP_200_OK:
        #         return response            
        #     case _:
        #         raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED, 
            detail="Project service not yet implemented"
        )

    async def update_project(self, project_id: str, project: Project, logged_user: dict = Depends(get_current_user)):
        """Update project by ID"""
        logged_user = UserProfile(**logged_user)
        log.info(f"Updating project {project_id} by user: {logged_user.user_id}")
        
        # TODO: Implement project service
        # response = self.project_service.update_project(logged_user, project, project_id)
        # match response['status_code']:
        #     case status.HTTP_200_OK:
        #         return response            
        #     case _:
        #         raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED, 
            detail="Project service not yet implemented"
        )

    async def delete_project(self, project_id: str, logged_user: dict = Depends(get_current_user)):
        """Delete project by ID"""
        log.debug(f"logged user: {logged_user}")
        logged_user = UserProfile(**logged_user)
        log.info(f"Deleting project {project_id} by user: {logged_user.user_id}")
        
        # TODO: Implement project service
        # response = self.project_service.delete_project(logged_user, project_id)
        # match response['status_code']:
        #     case status.HTTP_204_NO_CONTENT:
        #         return response            
        #     case _:
        #         raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED, 
            detail="Project service not yet implemented"
        )

    async def get_projects(self, limit: Optional[int] = 100, skip: Optional[int] = 0, logged_user: dict = Depends(get_current_user)):
        """Get all projects with pagination"""
        logged_user = UserProfile(**logged_user)
        log.info(f"Getting projects by user: {logged_user.user_id}, limit: {limit}, skip: {skip}")
        
        # TODO: Implement project service
        # response = self.project_service.get_projects(logged_user, limit, skip)
        # match response['status_code']:
        #     case status.HTTP_200_OK:
        #         return response            
        #     case _:
        #         raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED, 
            detail="Project service not yet implemented"
        )

    async def get_project_modules(self, project_id: str, logged_user: dict = Depends(get_current_user)):
        """Get all modules within a project"""
        logged_user = UserProfile(**logged_user)
        log.info(f"Getting modules in project {project_id} by user: {logged_user.user_id}")
        
        # TODO: Implement project service
        # response = self.project_service.get_project_modules(logged_user, project_id)
        # match response['status_code']:
        #     case status.HTTP_200_OK:
        #         return response            
        #     case _:
        #         raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED, 
            detail="Project service not yet implemented"
        )

    # Module CRUD operations
    async def create_module(self, project_id: str, module: Module, logged_user: dict = Depends(get_current_user)):
        """Create a new module within a project"""
        logged_user = UserProfile(**logged_user)
        log.info(f"Creating module in project {project_id} by user: {logged_user.user_id}")
        
        # TODO: Implement module service
        # response = self.project_service.create_module(logged_user, module, project_id)
        # match response['status_code']:
        #     case status.HTTP_201_CREATED:
        #         return response            
        #     case _:
        #         raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED, 
            detail="Module service not yet implemented"
        )

    async def get_module(self, project_id: str, module_id: str, logged_user: dict = Depends(get_current_user)):
        """Get module by ID within a project"""
        logged_user = UserProfile(**logged_user)
        log.info(f"Getting module {module_id} in project {project_id} by user: {logged_user.user_id}")
        
        # TODO: Implement module service
        # response = self.project_service.get_module(logged_user, module_id, project_id)
        # match response['status_code']:
        #     case status.HTTP_200_OK:
        #         return response            
        #     case _:
        #         raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED, 
            detail="Module service not yet implemented"
        )

    async def update_module(self, project_id: str, module_id: str, module: Module, logged_user: dict = Depends(get_current_user)):
        """Update module by ID within a project"""
        logged_user = UserProfile(**logged_user)
        log.info(f"Updating module {module_id} in project {project_id} by user: {logged_user.user_id}")
        
        # TODO: Implement module service
        # response = self.project_service.update_module(logged_user, module, module_id, project_id)
        # match response['status_code']:
        #     case status.HTTP_200_OK:
        #         return response            
        #     case _:
        #         raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED, 
            detail="Module service not yet implemented"
        )

    async def delete_module(self, project_id: str, module_id: str, logged_user: dict = Depends(get_current_user)):
        """Delete module by ID within a project"""
        logged_user = UserProfile(**logged_user)
        log.info(f"Deleting module {module_id} in project {project_id} by user: {logged_user.user_id}")
        
        # TODO: Implement module service
        # response = self.project_service.delete_module(logged_user, module_id, project_id)
        # match response['status_code']:
        #     case status.HTTP_204_NO_CONTENT:
        #         return response            
        #     case _:
        #         raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED, 
            detail="Module service not yet implemented"
        )

    async def get_modules(self, project_id: str, limit: Optional[int] = 100, skip: Optional[int] = 0, logged_user: dict = Depends(get_current_user)):
        """Get all modules within a project with pagination"""
        logged_user = UserProfile(**logged_user)
        log.info(f"Getting modules in project {project_id} by user: {logged_user.user_id}, limit: {limit}, skip: {skip}")
        
        # TODO: Implement module service
        # response = self.project_service.get_modules(logged_user, project_id, limit, skip)
        # match response['status_code']:
        #     case status.HTTP_200_OK:
        #         return response            
        #     case _:
        #         raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED, 
            detail="Module service not yet implemented"
        )


# Initialize router
pr_router = ProjectRouter(config)
project_router = pr_router.register_routes()