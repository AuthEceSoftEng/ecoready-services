import uuid
from fastapi import HTTPException, status
from utilities.project_utils import (
    create_project_in_db,
    update_project_in_db,
    delete_project_in_db,
    get_all_organization_projects_from_db,
    get_project_by_id
)
from typing import List
from models.project_models import ProjectCreateRequest, ProjectUpdateRequest, ProjectResponse
from dependencies import contains_special_characters, get_organization_by_id

async def create_project_service(organization_id: uuid.UUID, project_data: ProjectCreateRequest):
    # Create the project and return its data in response format
    if contains_special_characters(project_data.project_name, allow_spaces=False, allow_underscores=False):
         raise HTTPException(status_code=400, detail="Invalid name format. Names can only contain latin letters and numbers.")       
    project = await create_project_in_db(organization_id, project_data)
    return {"message": f"Project {project_data.project_name} created succesfully", "id": project}


async def update_project_service(project_id: uuid.UUID, project_data: ProjectUpdateRequest):
    await update_project_in_db(project_id, project_data)


async def delete_project_service(project_id: uuid.UUID):    
    await delete_project_in_db(project_id)


async def get_all_projects_service(organization_id: uuid.UUID) -> List[ProjectResponse]:
    rows = await get_all_organization_projects_from_db(organization_id)
    return [
        ProjectResponse(
            project_id=row.id,
            organization_id=row.organization_id,
            organization_name= get_organization_by_id(row.organization_id).organization_name,
            project_name= row.project_name,
            description=row.description,
            tags=row.tags if row.tags else [],
            creation_date=str(row.creation_date)
        ) 
        for row in rows
    ]


async def get_project_by_id_service(row) -> ProjectResponse:
    return ProjectResponse(
            project_id=row.id,
            organization_id=row.organization_id,
            organization_name= get_organization_by_id(row.organization_id).organization_name,
            project_name= row.project_name,
            description=row.description,
            tags=row.tags if row.tags else [],
            creation_date=str(row.creation_date)
        ) 
