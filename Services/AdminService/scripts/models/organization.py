from typing import Optional, List, Dict, Any, Union
from datetime import datetime
from pydantic import BaseModel, Field, EmailStr, HttpUrl


# Address model for structured address data
class Address(BaseModel):
    class Config:
        extra = "forbid"
        
    street: Optional[str] = Field(None, description="Street address")
    city: Optional[str] = Field(None, description="City")
    state: Optional[str] = Field(None, description="State or province")
    zip_code: Optional[str] = Field(None, description="ZIP or postal code")
    country: Optional[str] = Field(None, description="Country")


class Organization(BaseModel):
    """
    A comprehensive data model for a generic organization.
    """
    class Config:
        extra = "forbid"
        
    org_id: Optional[str] = Field(None, description="Unique identifier for the organization (e.g., 'ACME-CORP').")
    name: Optional[str] = Field(None, description="The full, official name of the organization.")
    is_active: Optional[bool] = Field(None, description="Whether the organization is currently active.")
    short_name: Optional[str] = Field(None, description="A shortened or alias name for the organization.")
    description: Optional[str] = Field(None, description="A brief description of the organization's mission or business.")
    
    # Contact and Location
    primary_contact: Optional[str] = Field(None, description="The primary point of contact within the organization.")
    email: Optional[EmailStr] = Field(None, description="The main corporate email address.")
    website: Optional[HttpUrl] = Field(None, description="The official website of the organization.")
    address: Optional[Union[str, Address]] = Field(None, description="The physical address of the organization's headquarters. Can be a string or structured address object.")
    
    # Hierarchy and Relationships
    parent_org_id: Optional[str] = Field(None, description="The ID of the parent organization, if this is a subsidiary or department.")
    status: Optional[str] = Field(None, description="The operational status of the organization (e.g., 'active', 'inactive', 'dissolved').")
    business_units: Optional[List[str]] = Field(None, description="The business unit to which the organization belongs.")
    
    # Membership and Resources
    members: Optional[List[str]] = Field(None, description="A list of user IDs who are members of this organization.")
    projects: Optional[List[str]] = Field(None, description="A list of project IDs associated with this organization.")
    
    # Timestamps
    established_date: Optional[datetime] = Field(None, description="The date the organization was officially established.")
    created_at: Optional[datetime] = Field(None, description="When the organization record was created.")
    updated_at: Optional[datetime] = Field(None, description="When the organization record was last updated.")
    
    # A flexible field for additional, unstructured data.
    metadata: Optional[Dict[str, Any]] = Field(None, description="A key-value store for extra organizational details.")
