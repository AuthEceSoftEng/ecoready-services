# app/dependencies.py

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, APIKeyHeader, HTTPAuthorizationCredentials
from collections.abc import Mapping, Sequence
from jose import JWTError, jwt
from services.auth_service import verify_jwt_token
from utilities.user_utils import get_user_by_username
from utilities.cassandra_connector import get_cassandra_session
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id
import re
import uuid
from typing import Optional,List


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/token", auto_error=False)
header_scheme = APIKeyHeader(name="X-API-Key", auto_error=False)

def get_current_user_from_jwt(token: str = Depends(oauth2_scheme)):
    if token is not None:
        username = verify_jwt_token(token)
        user = get_user_by_username(username)
        if user is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        return user
    else:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No credentials provided")
def validate_api_key(api_key: str):
    query = "SELECT key_type, project_id FROM api_keys WHERE api_key=%s LIMIT 1 allow filtering"
    session = get_cassandra_session()
    try:
        key_data = session.execute(query, (api_key,)).one()
        if key_data:
            return key_data.key_type, key_data.project_id
    except:
        raise HTTPException(status_code=status.HTTP_401_FORBIDDEN, detail="Invalid API key")

def check_api_key(key_type: str, key_project_id: uuid.UUID, project_id: uuid.UUID, accepted_roles: List[str]):
    if project_id != key_project_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="You cannot access this project")
    if key_type not in accepted_roles:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Insufficient privileges to access this resource")
    return True
def verify_superadmin(current_user: dict = Depends(get_current_user_from_jwt)):
    if current_user.role != "superadmin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have superadmin privileges"
        )
    return current_user

def verify_user_belongs_to_organization(
    organization_id: uuid.UUID,
    current_user: dict = Depends(get_current_user_from_jwt)
):
    # Check if the user is a superadmin or belongs to the organization
    if current_user.role != "superadmin" and current_user.organization_id != organization_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"You don't have access to this organization."
        )
    return current_user


def verify_endpoint_access(
        organization_id: uuid.UUID,
        project_id: uuid.UUID,
        jwt_token: Optional[str] = Depends(oauth2_scheme),
        api_key: Optional[HTTPAuthorizationCredentials] = Depends(header_scheme)
):
    roles = ['master','read','write']
    if jwt_token:
        return verify_user_belongs_to_organization(organization_id, get_current_user_from_jwt(jwt_token))
    elif api_key:
        return verify_api_key_access(project_id, roles, api_key)
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No credentials provided"
        )
def verify_master_access(
        organization_id: uuid.UUID,
        project_id: uuid.UUID,
        jwt_token: Optional[str] = Depends(oauth2_scheme),
        api_key: Optional[HTTPAuthorizationCredentials] = Depends(header_scheme)
):
    if jwt_token:
        return verify_user_belongs_to_organization(organization_id, get_current_user_from_jwt(jwt_token))
    elif api_key:
        return verify_api_key_access(project_id, ['master'], api_key)
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No credentials provided"
        )
def verify_api_key_access(
        project_id: uuid.UUID,
        roles: List[str] = ['master','read','write'],
        api_key: Optional[HTTPAuthorizationCredentials] = Depends(header_scheme)
):
    key_type, key_project_id = validate_api_key(api_key)
    return check_api_key(key_type, key_project_id, project_id, roles)

def contains_special_characters(s, allow_spaces=True, allow_underscores=True):
    if s.strip() == "":
        return True
    # Regex pattern that matches any special character except underscore
    if allow_underscores:
        if allow_spaces:
            pattern = r"[^a-zA-Z0-9_ ]"
        else:
            pattern = r"[^a-zA-Z0-9_]"
    else:
        pattern = r"[^a-zA-Z0-9]"
    # Search for the pattern in the string
    if re.search(pattern, s):
        return True
    else:
        return False

def check_organization_exists(organization_id: uuid.UUID):
    organization = get_organization_by_id(organization_id)
    if not organization:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Organization not found.")
    return organization

def check_project_exists(project_id: uuid.UUID, organization_id: uuid.UUID):
    project = get_project_by_id(project_id, organization_id)
    if not project:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found.")
    return project

