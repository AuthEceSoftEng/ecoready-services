from fastapi import APIRouter, Depends, HTTPException
from dependencies import check_organization_exists, get_current_user_from_jwt, verify_superadmin
import uuid
from services.user_service import (
    create_user_service,
    delete_user_service,
    update_user_password_service,
    get_all_users_service,
)
from models.user_models import UserRequest, Username

router = APIRouter(tags=["Organization User Management"], dependencies=[Depends(check_organization_exists)])


@router.post("/organizations/{organization_id}/users")
async def create_user(
    organization_id: uuid.UUID,
    user_data: UserRequest,
    current_user: dict = Depends(verify_superadmin)
):
    return await create_user_service(organization_id, user_data)


@router.get("/organizations/{organization_id}/users")
async def get_all_users(
    organization_id: uuid.UUID,
    current_user: dict = Depends(verify_superadmin)
):
    return await get_all_users_service(organization_id)


@router.put("/organizations/{organization_id}/users/update_password")
async def update_user_password(
    organization_id: uuid.UUID,
    data: UserRequest,
    current_user: dict = Depends(get_current_user_from_jwt)
):
    return await update_user_password_service(organization_id, data, current_user)


@router.delete("/organizations/{organization_id}/users")
async def delete_user(
    organization_id: uuid.UUID,
    data: Username,
    current_user: dict = Depends(get_current_user_from_jwt)
):
    return await delete_user_service(organization_id, data.username, current_user)
