from typing import Optional, List, Dict, Any 
from datetime import datetime, date 
from pydantic import BaseModel, Field 
 
# A lightweight user model for ownership and team members. 
class ModuleMember(BaseModel): 
    class Config:
        extra = "forbid"
        
    user_id: Optional[str] = None
    username: Optional[str] = None
    role: Optional[str] = None # e.g., 'lead', 'contributor' 
     
# A lightweight model for representing an associated project. 
class ParentProject(BaseModel): 
    class Config:
        extra = "forbid"
        
    project_id: Optional[str] = None
    project_name: Optional[str] = None
 
class Module(BaseModel): 
    """ 
    A comprehensive data model for a generic module, a subpart of a project. 
    """ 
    class Config:
        extra = "forbid"
        
    module_id: Optional[str] = Field(None, description="Unique identifier for the module.") 
    name: Optional[str] = Field(None, description="The name of the module.") 
    description: Optional[str] = Field(None, description="A detailed description of the module.") 
    status: Optional[str] = Field(None, description="The current status of the module (e.g., 'not_started', 'in_progress', 'completed').") 
     
    # Hierarchy and Ownership 
    project_id: Optional[str] = Field(None, description="The project this module belongs to.") 
    owner: Optional[str] = Field(None, description="The primary person responsible for the module.") 
     
    # Time and Dates 
    start_date: Optional[date] = Field(None, description="The planned start date for the module.") 
    due_date: Optional[date] = Field(None, description="The target completion date.") 
    completed_at: Optional[datetime] = Field(None, description="The timestamp when the module was completed.") 
 
    # Relationships and Members 
    members: Optional[str] = Field(None, description="A list of all members assigned to the module.") 
    tags: Optional[List[str]] = Field(None, description="Categorization tags (e.g., 'frontend', 'database', 'SEO').") 
     
    # Metrics and Metadata 
    priority: Optional[str] = Field(None, description="The priority level of the module (e.g., 'low', 'medium', 'high').") 
     
    # Timestamps 
    created_at: Optional[datetime] = Field(None, description="When the module record was created.")
    updated_at: Optional[datetime] = Field(None, description="When the module record was last updated.")
 
    # A flexible field for additional, unstructured data. 
    metadata: Optional[Dict[str, Any]] = Field(None, description="A key-value store for extra module details.")
