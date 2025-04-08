from fastapi import APIRouter, Depends, HTTPException, Query
from models.collection_models import CollectionCreateRequest, CollectionResponse, CollectionUpdateRequest
from services.collection_service import (
    create_collection_service,
    update_collection_service,
    delete_collection_service,
    get_all_collections_service,
    get_collection_info_service
)
from dependencies import check_organization_exists, check_project_exists, verify_api_key_access, verify_endpoint_access, verify_master_access
from utilities.collection_utils import check_collection_exists
from typing import List
import uuid

router = APIRouter(dependencies=[Depends(check_organization_exists), Depends(check_project_exists)])
TAG = "Collection Management"

# Create a new collection
@router.post("/organizations/{organization_id}/projects/{project_id}/collections", tags=[TAG],  dependencies=[Depends(verify_master_access)])
async def create_collection(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    collection_data: CollectionCreateRequest,
):
    return await create_collection_service(organization_id, project_id, collection_data)

# Update an existing collection
@router.put("/organizations/{organization_id}/projects/{project_id}/collections/{collection_id}", tags=[TAG], dependencies=[Depends(check_collection_exists), Depends(verify_master_access)])
async def update_collection(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    collection_data: CollectionUpdateRequest
):
    return await update_collection_service(organization_id, project_id, collection_id, collection_data)

# Delete a collection
@router.delete("/organizations/{organization_id}/projects/{project_id}/collections/{collection_id}", tags=[TAG], dependencies=[Depends(check_collection_exists), Depends(verify_master_access)])
async def delete_collection(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    collection_id: uuid.UUID
):
    return await delete_collection_service(organization_id, project_id, collection_id)

# Get all collections of a project
@router.get("/organizations/{organization_id}/projects/{project_id}/collections", tags=[TAG], response_model=List[CollectionResponse], dependencies=[Depends(verify_endpoint_access)])
async def get_all_collections(
    organization_id: uuid.UUID,
    project_id: uuid.UUID
):
    return await get_all_collections_service(organization_id, project_id)

# Get information for a specific collection
@router.get("/organizations/{organization_id}/projects/{project_id}/collections/{collection_id}", tags=[TAG], response_model=CollectionResponse, dependencies=[Depends(check_collection_exists), Depends(verify_endpoint_access)])
async def get_collection_info(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    collection_id: uuid.UUID
):
    return await get_collection_info_service(organization_id, project_id, collection_id)
