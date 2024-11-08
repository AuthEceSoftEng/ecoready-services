from datetime import datetime
from uuid import UUID
from models.project_keys_models import (
    ProjectKeyCreateRequest, 
    RegenerateKeyRequest, 
    DeleteKeyRequest, 
    ProjectKeyResponse
)
from utilities.project_keys_utils import (
    insert_project_key,
    fetch_project_keys_by_category,
    update_project_key,
    delete_keys_by_category,
    delete_key_by_value,
    get_project_key_by_value
)
from fastapi import HTTPException

# Create a new project key
async def create_project_key_service(project_id: UUID, key_data: ProjectKeyCreateRequest) -> ProjectKeyResponse:
    key_value = insert_project_key(project_id, key_data.key_type)
    return ProjectKeyResponse(
        api_key=key_value,
        project_id=project_id,
        key_type=key_data.key_type,
        created_at=datetime.utcnow().isoformat()
    )

# Fetch project keys by category or all keys
async def fetch_project_keys_by_category_service(project_id: UUID, key_category: str):
    project_keys = fetch_project_keys_by_category(project_id, key_category)
    
    if not project_keys:
        raise HTTPException(status_code=404, detail="No keys found for this project.")
    
    return [
        ProjectKeyResponse(
            api_key=key.api_key,
            project_id=key.project_id,
            key_type=key.key_type,
            created_at=key.created_at.isoformat()
        ) for key in project_keys
    ]

# Regenerate a project key by providing the key value
async def regenerate_key_service(organization_id: UUID, project_id: UUID, key_data: RegenerateKeyRequest) -> ProjectKeyResponse:
    existing_key = get_project_key_by_value(key_data.key_value, project_id)

    if not existing_key:
        raise HTTPException(status_code=404, detail="Key not found or doesn't belong to this project.")
    
    new_key_value = update_project_key(existing_key.id, key_data.key_value)
    return ProjectKeyResponse(
        api_key=new_key_value,
        project_id=project_id,
        key_type=existing_key.key_type,
        created_at=datetime.utcnow().isoformat()
    )

# Delete keys by category
async def delete_keys_by_category_service(project_id: UUID, key_category: str):
    return delete_keys_by_category(project_id, key_category)

# Delete a key by value
async def delete_key_by_value_service(project_id: UUID, key_data: DeleteKeyRequest):
    existing_key = get_project_key_by_value(key_data.key_value, project_id)
    
    if not existing_key:
        raise HTTPException(status_code=404, detail="Key not found or doesn't belong to this project.")
    
    delete_key_by_value(existing_key.id)
    
    return {"message": "Key deleted successfully."}

