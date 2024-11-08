from pydantic import BaseModel
from typing import List, Dict, Any
import uuid

class CollectionCreateRequest(BaseModel):
    name: str
    description: str
    tags: List[str] = []
    collection_schema: Dict[str, Any]

class CollectionUpdateRequest(BaseModel):
    description: str = None
    tags: List[str] = None

class CollectionResponse(BaseModel):
    collection_name: str
    collection_id: uuid.UUID
    project_id: uuid.UUID
    project_name: str
    organization_id: uuid.UUID
    organization_name: str
    description: str
    tags: List[str]
    creation_date: str
    collection_schema: Dict[str, Any]