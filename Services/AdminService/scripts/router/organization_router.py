from fastapi import APIRouter, HTTPException, status, Depends
from typing import Optional

from scripts.utils.logger import log
from scripts.config.application import config
from scripts.models.request import UserProfile
from scripts.models.organization import Organization
from scripts.models.business_unit import BusinessUnit
from scripts.services.jwt_dependancy import get_current_user
from scripts.services.organization_services import OrganizationService


class OrganizationRouter:
    def __init__(self, config):
        self.config = config
        self.organization_router = APIRouter(
            prefix='/organizations',
            tags=['admin-organizations']
        )
        # Note: Organization service will need to be created
        self.organization_service = OrganizationService(config)

    def register_routes(self):
        # Organization CRUD routes
        self.organization_router.add_api_route("/", self.create_organization, methods=['POST'], status_code=201)
        self.organization_router.add_api_route("/{org_id}", self.get_organization, methods=['GET'], status_code=200)
        self.organization_router.add_api_route("/{org_id}", self.update_organization, methods=['PUT'], status_code=200)
        self.organization_router.add_api_route("/{org_id}", self.delete_organization, methods=['DELETE'], status_code=204)
        self.organization_router.add_api_route("/", self.get_organizations, methods=['GET'], status_code=200)
        self.organization_router.add_api_route("/{org_id}/units", self.get_organization_units, methods=['GET'], status_code=200)
        
        # Business Unit CRUD routes under organizations
        self.organization_router.add_api_route("/{org_id}/business-units", self.create_business_unit, methods=['POST'], status_code=201)
        self.organization_router.add_api_route("/{org_id}/business-units/{bu_id}", self.get_business_unit, methods=['GET'], status_code=200)
        self.organization_router.add_api_route("/{org_id}/business-units/{bu_id}", self.update_business_unit, methods=['PUT'], status_code=200)
        self.organization_router.add_api_route("/{org_id}/business-units/{bu_id}", self.delete_business_unit, methods=['DELETE'], status_code=204)
        self.organization_router.add_api_route("/{org_id}/business-units", self.get_business_units, methods=['GET'], status_code=200)
        
        return self.organization_router

    # Organization CRUD operations
    async def create_organization(self, organization: Organization, logged_user: dict = Depends(get_current_user)):
        """Create a new organization"""
        log.debug(f"logged user: {logged_user}")
        logged_user = UserProfile(**logged_user)
        log.info(f"Creating organization by user: {logged_user.user_id}")
        
        # TODO: Implement organization service
        response = self.organization_service.create_organization(logged_user, organization)
        match response['status_code']:
            case status.HTTP_201_CREATED:
                return response            
            case _:
                raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        # raise HTTPException(
        #     status_code=status.HTTP_501_NOT_IMPLEMENTED, 
        #     detail="Organization service not yet implemented"
        # )

    async def get_organization(self, org_id: str, logged_user: dict = Depends(get_current_user)):
        """Get organization by ID"""
        logged_user = UserProfile(**logged_user)
        log.info(f"Getting organization {org_id} by user: {logged_user.user_id}")
        
        # TODO: Implement organization service
        response = self.organization_service.get_organization(logged_user, org_id)
        match response['status_code']:
            case status.HTTP_200_OK:
                return response            
            case _:
                raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        # raise HTTPException(
        #     status_code=status.HTTP_501_NOT_IMPLEMENTED, 
        #     detail="Organization service not yet implemented"
        # )

    async def update_organization(self, org_id: str, organization: Organization, logged_user: dict = Depends(get_current_user)):
        """Update organization by ID"""
        logged_user = UserProfile(**logged_user)
        log.info(f"Updating organization {org_id} by user: {logged_user.user_id}")
        
        # TODO: Implement organization service
        response = self.organization_service.update_organization(logged_user, organization, org_id)
        match response['status_code']:
            case status.HTTP_200_OK:
                return response            
            case _:
                raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        # raise HTTPException(
        #     status_code=status.HTTP_501_NOT_IMPLEMENTED, 
        #     detail="Organization service not yet implemented"
        # )

    async def delete_organization(self, org_id: str, logged_user: dict = Depends(get_current_user)):
        """Delete organization by ID"""
        log.debug(f"logged user: {logged_user}")
        logged_user = UserProfile(**logged_user)
        log.info(f"Deleting organization {org_id} by user: {logged_user.user_id}")
        
        # TODO: Implement organization service
        response = self.organization_service.delete_organization(logged_user, org_id)
        match response['status_code']:
            case status.HTTP_204_NO_CONTENT:
                return response            
            case _:
                raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        # raise HTTPException(
        #     status_code=status.HTTP_501_NOT_IMPLEMENTED, 
        #     detail="Organization service not yet implemented"
        # )

    async def get_organizations(self, limit: Optional[int] = 100, skip: Optional[int] = 0, logged_user: dict = Depends(get_current_user)):
        """Get all organizations with pagination"""
        logged_user = UserProfile(**logged_user)
        log.info(f"Getting organizations by user: {logged_user.user_id}, limit: {limit}, skip: {skip}")
        
        # TODO: Implement organization service
        response = self.organization_service.get_organizations(logged_user, limit, skip)
        match response['status_code']:
            case status.HTTP_200_OK:
                return response            
            case _:
                raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        # raise HTTPException(
        #     status_code=status.HTTP_501_NOT_IMPLEMENTED, 
        #     detail="Organization service not yet implemented"
        # )

    def get_organization_units(self, org_id: str, logged_user: dict = Depends(get_current_user)):
        """Get all business units within an organization"""
        logged_user = UserProfile(**logged_user)
        log.info(f"Getting business units in organization {org_id} by user: {logged_user.user_id}")
        
        # TODO: Implement organization service
        response = self.organization_service.get_organization_units(logged_user, org_id)
        match response['status_code']:
            case status.HTTP_200_OK:
                return response            
            case _:
                raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        # raise HTTPException(
        #     status_code=status.HTTP_501_NOT_IMPLEMENTED, 
        #     detail="Organization service not yet implemented"
        # )

    # Business Unit CRUD operations
    async def create_business_unit(self, org_id: str, business_unit: BusinessUnit, logged_user: dict = Depends(get_current_user)):
        """Create a new business unit within an organization"""
        logged_user = UserProfile(**logged_user)
        log.info(f"Creating business unit in organization {org_id} by user: {logged_user.user_id}")
        
        # TODO: Implement business unit service
        response = self.organization_service.create_business_unit(logged_user, business_unit, org_id)
        match response['status_code']:
            case status.HTTP_201_CREATED:
                return response            
            case _:
                raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        # raise HTTPException(
        #     status_code=status.HTTP_501_NOT_IMPLEMENTED, 
        #     detail="Business unit service not yet implemented"
        # )

    async def get_business_unit(self, org_id: str, bu_id: str, logged_user: dict = Depends(get_current_user)):
        """Get business unit by ID within an organization"""
        logged_user = UserProfile(**logged_user)
        log.info(f"Getting business unit {bu_id} in organization {org_id} by user: {logged_user.user_id}")
        
        # TODO: Implement business unit service
        # response = self.business_unit_service.get_business_unit(logged_user, bu_id, org_id)
        # match response['status_code']:
        #     case status.HTTP_200_OK:
        #         return response            
        #     case _:
        #         raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED, 
            detail="Business unit service not yet implemented"
        )

    async def update_business_unit(self, org_id: str, bu_id: str, business_unit: BusinessUnit, logged_user: dict = Depends(get_current_user)):
        """Update business unit by ID within an organization"""
        logged_user = UserProfile(**logged_user)
        log.info(f"Updating business unit {bu_id} in organization {org_id} by user: {logged_user.user_id}")
        
        # TODO: Implement business unit service
        # response = self.business_unit_service.update_business_unit(logged_user, business_unit, bu_id, org_id)
        # match response['status_code']:
        #     case status.HTTP_200_OK:
        #         return response            
        #     case _:
        #         raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED, 
            detail="Business unit service not yet implemented"
        )

    async def delete_business_unit(self, org_id: str, bu_id: str, logged_user: dict = Depends(get_current_user)):
        """Delete business unit by ID within an organization"""
        logged_user = UserProfile(**logged_user)
        log.info(f"Deleting business unit {bu_id} in organization {org_id} by user: {logged_user.user_id}")
        
        # TODO: Implement business unit service
        response = self.organization_service.delete_business_unit(logged_user, bu_id, org_id)
        match response['status_code']:
            case status.HTTP_204_NO_CONTENT:
                return response            
            case _:
                raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        # raise HTTPException(
        #     status_code=status.HTTP_501_NOT_IMPLEMENTED, 
        #     detail="Business unit service not yet implemented"
        # )

    async def get_business_units(self, org_id: str, limit: Optional[int] = 100, skip: Optional[int] = 0, logged_user: dict = Depends(get_current_user)):
        """Get all business units within an organization with pagination"""
        logged_user = UserProfile(**logged_user)
        log.info(f"Getting business units in organization {org_id} by user: {logged_user.user_id}, limit: {limit}, skip: {skip}")
        
        # TODO: Implement business unit service
        response = self.organization_service.get_business_units(logged_user, org_id, limit, skip)
        match response['status_code']:
            case status.HTTP_200_OK:
                return response            
            case _:
                raise HTTPException(status_code=response['status_code'], detail=response)
        
        # Temporary response until service is implemented
        # raise HTTPException(
        #     status_code=status.HTTP_501_NOT_IMPLEMENTED, 
        #     detail="Business unit service not yet implemented"
        # )


# Initialize router
or_router = OrganizationRouter(config)
organization_router = or_router.register_routes()