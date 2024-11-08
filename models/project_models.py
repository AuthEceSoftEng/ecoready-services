from pydantic import BaseModel
from typing import List, Optional
import uuid


class ProjectCreateRequest(BaseModel):
    project_name: str
    description: Optional[str]
    tags: List[str]


class ProjectUpdateRequest(BaseModel):
    description: Optional[str]
    tags: Optional[List[str]]


class ProjectResponse(BaseModel):
    organization_id: uuid.UUID
    project_id: uuid.UUID
    organization_name: str
    project_name: str
    description: Optional[str]
    tags: List[str]
    creation_date: str

