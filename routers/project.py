from fastapi import APIRouter, Depends, HTTPException
from typing import List
from models.project_models import ProjectCreateRequest, ProjectUpdateRequest, ProjectResponse
from services.project_service import create_project_service, update_project_service, delete_project_service, get_all_projects_service, get_project_by_id_service
from dependencies import check_organization_exists, check_project_exists, verify_user_belongs_to_organization, verify_endpoint_access
import uuid

router = APIRouter(dependencies=[Depends(check_organization_exists)])

TAG = "Project Management"


@router.post("/organizations/{organization_id}/projects", tags=[TAG], dependencies=[Depends(verify_user_belongs_to_organization)])
async def create_project(
    organization_id: uuid.UUID,
    project_data: ProjectCreateRequest
):
    return await create_project_service(organization_id, project_data)


@router.put("/organizations/{organization_id}/projects/{project_id}", tags=[TAG], dependencies=[Depends(verify_user_belongs_to_organization), Depends(check_project_exists)])
async def update_project(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    project_data: ProjectUpdateRequest,
):
    await update_project_service(project_id, project_data)
    return {"message": "Project successfully updated"}


@router.delete("/organizations/{organization_id}/projects/{project_id}", tags=[TAG], dependencies=[Depends(verify_user_belongs_to_organization), Depends(check_project_exists)])
async def delete_project(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
):
    await delete_project_service(project_id)
    return {"message": "Project successfully deleted"}


@router.get("/organizations/{organization_id}/projects", tags=[TAG], response_model=List[ProjectResponse], dependencies=[Depends(verify_user_belongs_to_organization)])
async def get_all_projects(
    organization_id: uuid.UUID,
):
    return await get_all_projects_service(organization_id)


@router.get("/organizations/{organization_id}/projects/{project_id}", tags=[TAG], response_model=ProjectResponse, dependencies=[Depends(verify_endpoint_access)])
async def get_project_by_id(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    project = Depends(check_project_exists)
):
    return await get_project_by_id_service(project)
