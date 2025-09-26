from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field


    
class BusinessUnit(BaseModel):
    """
    A comprehensive data model for a generic business unit.
    """
    class Config:
        extra = "forbid"
        
    bu_id: Optional[str] = Field(None, description="Unique identifier for the business unit (e.g., 'SALES-EAST').")
    name: Optional[str] = Field(None, description="The official name of the business unit.")
    description: Optional[str] = Field(None, description="A detailed description of the business unit's function.")
    
    # Hierarchy and Relationships
    parent_org: Optional[str] = Field(None, description="The organization this business unit belongs to.")
    parent_bu_id: Optional[str] = Field(None, description="The ID of the parent business unit, if this is a sub-unit.")
    head: Optional[str] = Field(None, description="The person who leads the business unit.")
    
    # Resources
    members: Optional[List[str]] = Field(None, description="A list of user IDs who are members of this business unit.")
    projects: Optional[List[str]] = Field(None, description="A list of project IDs managed or owned by this business unit.")
    
    # Status and Lifecycle
    status: Optional[str] = Field(None, description="The operational status of the business unit (e.g., 'active', 'inactive', 'dissolved').")
    
    # Timestamps
    created_at: Optional[datetime] = Field(None, description="When the business unit record was created.")
    updated_at: Optional[datetime] = Field(None, description="When the business unit record was last updated.")
    
    # A flexible field for additional, unstructured data.
    metadata: Optional[Dict[str, Any]] = Field(None, description="A key-value store for extra business unit details.")