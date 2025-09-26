from typing import Optional, List, Dict, Any 
from datetime import datetime, date 
from pydantic import BaseModel, Field, HttpUrl

from scripts.models.user import User
from scripts.models.organization import Organization
 
class Project(BaseModel): 
    """ 
    A comprehensive data model for a generic project. 
    """ 
    class Config:
        extra = "forbid"
        
    project_id: Optional[str] = Field(None, description="Unique identifier for the project.") 
    name: Optional[str] = Field(None, description="The name of the project.") 
    description: Optional[str] = Field(None, description="A detailed description of the project.") 
    status: Optional[str] = Field(None, description="The current status of the project (e.g., 'planning', 'in_progress', 'completed', 'archived').") 
     
    # Ownership and Hierarchy 
    owner: Optional[str] = Field(None, description="The primary owner of the project.") 
    parent_project_id: Optional[str] = Field(None, description="The ID of the parent project, if this is a sub-project.") 
    org_id: Optional[str] = Field(None, description="The organization that owns the project.") 
     
    # Time and Dates 
    start_date: Optional[date] = Field(None, description="The planned start date for the project.")
    due_date: Optional[date] = Field(None, description="The target completion date.") 
    completed_at: Optional[datetime] = Field(None, description="The timestamp when the project was completed.") 
    modules: Optional[List[str]] = Field(None, description="A list of modules associated with the project.")
    # Relationships and Members 
    members: Optional[List[str]] = Field(None, description="A list of all members associated with the project.") 
    tags: Optional[List[str]] = Field(None, description="Categorization tags (e.g., 'marketing', 'backend', 'urgent').") 
     
    # Project Metrics and Metadata 
    budget: Optional[float] = Field(None, description="The allocated budget for the project.") 
    priority: Optional[str] = Field(None, description="The priority level of the project (e.g., 'low', 'medium', 'high', 'critical').") 
     
    # Timestamps 
    created_at: Optional[datetime] = Field(None, description="When the project record was created.")
    updated_at: Optional[datetime] = Field(None, description="When the project record was last updated.")
 
    # A flexible field for additional, unstructured data. 
    metadata: Optional[Dict[str, Any]] = Field(None, description="A key-value store for extra project details.")