from fastapi import APIRouter, Depends, HTTPException
from services.organization_service import (
    create_organization_service,
    get_organization_info_service,
    update_organization_service,
    delete_organization_service,
    get_all_organizations_service
)
from dependencies import check_organization_exists, get_current_user_from_jwt, verify_superadmin, verify_user_belongs_to_organization
from models.organization_models import OrganizationCreateRequest, OrganizationUpdateRequest, OrganizationResponse
import uuid
from typing import List

router = APIRouter()
TAG = "Organization Management"

# POST: Create an organization
@router.post("/organizations", tags=[TAG], dependencies=[Depends(verify_superadmin)])
async def create_organization(data: OrganizationCreateRequest):
    return await create_organization_service(data)

# GET: Get information of a specific organization by organization_id
@router.get("/organizations/{organization_id}", tags=[TAG], response_model=OrganizationResponse, dependencies=[Depends(verify_user_belongs_to_organization)])
async def get_organization_info(organization_id: uuid.UUID, organization: dict = Depends(check_organization_exists)):
    return await get_organization_info_service(organization)

# PUT: Update an organization by organization_id
@router.put("/organizations/{organization_id}", tags=[TAG], dependencies=[Depends(check_organization_exists)])
async def update_organization(
    organization_id: uuid.UUID,
    data: OrganizationUpdateRequest
):
    return await update_organization_service(organization_id, data)

# DELETE: Delete an organization by organization_id
@router.delete("/organizations/{organization_id}", tags=[TAG], dependencies=[Depends(check_organization_exists), Depends(verify_superadmin)])
async def delete_organization(organization_id: uuid.UUID, organization: dict = Depends(check_organization_exists)):
    return await delete_organization_service(organization)

# GET: Get information of all organizations
@router.get("/organizations", tags=[TAG], dependencies=[Depends(verify_superadmin)], response_model=List[OrganizationResponse])
async def get_all_organizations():
    return await get_all_organizations_service()