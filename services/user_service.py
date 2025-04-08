from fastapi import HTTPException, status
from utilities.user_utils import (
    get_user_by_username,
    get_user_by_username_and_org_id,
    insert_user,
    delete_user_from_db,
    update_user_password_in_db,
    get_all_users_in_organization
)
from utilities.organization_utils import get_organization_by_id
from models.user_models import UserRequest
import uuid
from passlib.context import CryptContext
from dependencies import contains_special_characters

# Initialize the password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


async def create_user_service(organization_id: uuid.UUID, user_data: UserRequest):
    if contains_special_characters(user_data.username, allow_spaces=False, allow_underscores=True):
        raise HTTPException(status_code=400, detail="Invalid username format. Usernames can only contain latin letters, numbers, and underscores.")
    existing_user = get_user_by_username(user_data.username)
    if existing_user:
        raise HTTPException(status_code=409, detail="Username already exists")

    hashed_password = pwd_context.hash(user_data.password)
    user_id = uuid.uuid4()

    await insert_user(user_id, organization_id, user_data.username, hashed_password)

    return {"message": "User created successfully", "user_id": str(user_id)}


async def delete_user_service(organization_id: uuid.UUID, username: str, current_user: dict):
    user_to_delete = await get_user_by_username_and_org_id(username,organization_id)
    if not user_to_delete:
        raise HTTPException(status_code=404, detail="User not found")

    # Superadmins can delete any user; users can only delete themselves
    if current_user.role != "superadmin" and current_user.username != username:
        raise HTTPException(status_code=403, detail="Unauthorized to delete this user")

    await delete_user_from_db(user_to_delete.id)
    return {"message": "User deleted successfully"}


async def update_user_password_service(organization_id: uuid.UUID, data: UserRequest, current_user: dict):
    user_to_update = await get_user_by_username_and_org_id(data.username, organization_id)
    if not user_to_update:
        raise HTTPException(status_code=404, detail="User not found")

    # Superadmins can update any user's password; users can only update their own passwords
    if current_user.role != "superadmin" and current_user.username != data.username:
        raise HTTPException(status_code=403, detail="Unauthorized to update password for this user")

    hashed_password = pwd_context.hash(data.password)
    await update_user_password_in_db(user_to_update.id, hashed_password)

    return {"message": "User password updated successfully"}


async def get_all_users_service(organization_id: uuid.UUID):
    users = await get_all_users_in_organization(organization_id)
    return {"users": users}
