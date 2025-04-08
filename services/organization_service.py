from utilities.organization_utils import (
    insert_organization,
    get_organization_by_name,
    update_organization_in_db,
    delete_organization_from_db,
    create_keyspace_in_db,
    delete_keyspace_in_db,
    get_all_organizations_from_db
)
from fastapi import HTTPException
from models.organization_models import OrganizationCreateRequest, OrganizationUpdateRequest, OrganizationResponse
import uuid
from dependencies import contains_special_characters
from typing import List

# Create organization service
async def create_organization_service(data: OrganizationCreateRequest):
    if contains_special_characters(data.organization_name):
        raise HTTPException(status_code=400, detail="Invalid name format. Names can only contain latin letters, numbers, and underscores.")
    existing_org = await get_organization_by_name(data.organization_name)
    if existing_org:
        raise HTTPException(status_code=409, detail="Organization already exists")
    org_id = uuid.uuid4()
    try:
        await insert_organization(org_id, data)
        await create_keyspace_in_db(data.organization_name)
        return {"message": "Organization created successfully", "organization_id": str(org_id)}
    except Exception as e:
        await delete_organization_from_db(org_id)
        raise HTTPException(status_code=500, detail=f"Failed to create organization: {str(e)}")

# Get organization info service
async def get_organization_info_service(org) -> OrganizationResponse:
    return OrganizationResponse(
        organization_id=org.id,
        organization_name=org.organization_name,
        description=org.description,
        creation_date=str(org.creation_date),
        tags=org.tags if org.tags else []
    )

# Update organization service (request body used here)
async def update_organization_service(org_id: uuid.UUID, data: OrganizationUpdateRequest):
    await update_organization_in_db(org_id, data.description, data.tags)
    return {"message": "Organization updated successfully."}
# Delete organization service
async def delete_organization_service(organization):
    try:
        await delete_organization_from_db(organization.id)
        await delete_keyspace_in_db(organization.organization_name)
        return {"message": "Organization and all related data successfully deleted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete organization: {str(e)}")

# Service to get all organizations
async def get_all_organizations_service() -> List[OrganizationResponse]:
    organizations = await get_all_organizations_from_db()
    if not organizations:
        return {"organizations": []}
    return [
        OrganizationResponse(
            organization_id=org.id,
            organization_name=org.organization_name,
            description=org.description,
            creation_date=str(org.creation_date),
            tags=org.tags if org.tags else []
        )
        for org in organizations
    ]