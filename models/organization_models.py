from pydantic import BaseModel
from typing import List
import uuid

class OrganizationCreateRequest(BaseModel):
    organization_name: str
    description: str
    tags: List[str] = []

class OrganizationUpdateRequest(BaseModel):
    description: str = None
    tags: List[str] = None

class OrganizationResponse(BaseModel):
    organization_id: uuid.UUID
    organization_name: str
    description: str
    creation_date: str  
    tags: List[str] = None