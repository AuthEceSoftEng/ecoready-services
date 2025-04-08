from fastapi import APIRouter, Depends, Query
from models.project_keys_models import (
    ProjectKeyResponse, 
    BaseKeyModel, 
    ProjectKeyCreateRequest
)
from services.project_keys_service import (
    fetch_project_keys_by_category_service,
    regenerate_key_service,
    create_project_key_service,
    delete_keys_by_category_service,
    delete_key_by_value_service
)
from dependencies import verify_user_belongs_to_organization, check_organization_exists, check_project_exists
import uuid
from typing import Optional, List

router = APIRouter(dependencies=[Depends(check_organization_exists), Depends(check_project_exists), Depends(verify_user_belongs_to_organization)])
TAG = "Project Key Management"

# Create a new project key
@router.post("/api/organizations/{organization_id}/projects/{project_id}/keys", tags=[TAG], response_model=ProjectKeyResponse)
async def create_project_key(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    key_data: ProjectKeyCreateRequest
):
    return await create_project_key_service(project_id, key_data)

# Fetch project keys by category or all
@router.get("/api/organizations/{organization_id}/projects/{project_id}/keys", tags=[TAG], response_model=List[ProjectKeyResponse])
async def fetch_project_keys_by_category(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    key_category: Optional[str] = Query(None, enum=["read", "write", "master", "all"]),
):
    return await fetch_project_keys_by_category_service(project_id, key_category)

# Regenerate a project key by providing the key value in the body
@router.put("/api/organizations/{organization_id}/projects/{project_id}/keys/regenerate", tags=[TAG], response_model=ProjectKeyResponse)
async def regenerate_project_key(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    key_data: BaseKeyModel,
):
    return await regenerate_key_service(organization_id, project_id, key_data)

# Delete keys by category (query parameter)
@router.delete("/api/organizations/{organization_id}/projects/{project_id}/keys/delete_keys", tags=[TAG])
async def delete_keys_by_category(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    key_category: str = Query(..., enum=["read", "write", "master", "all"])
):
    return await delete_keys_by_category_service(project_id, key_category)

# Delete a key by providing the key value in the body
@router.delete("/api/organizations/{organization_id}/projects/{project_id}/keys/delete_key", tags=[TAG])
async def delete_key_by_value(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    key_data: BaseKeyModel
):
    return await delete_key_by_value_service(project_id, key_data)

