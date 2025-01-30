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
import pandas as pd
from datetime import datetime, timedelta

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
def validate_api_key(api_key: str, project_id: uuid.UUID):
    query = "SELECT key_type, project_id FROM api_keys WHERE api_key=%s AND project_id=%s LIMIT 1 allow filtering"
    session = get_cassandra_session()
    try:
        key_data = session.execute(query, (api_key,project_id, )).one()
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
def verify_write_access(
        organization_id: uuid.UUID,
        project_id: uuid.UUID,
        jwt_token: Optional[str] = Depends(oauth2_scheme),
        api_key: Optional[HTTPAuthorizationCredentials] = Depends(header_scheme)
):
    if jwt_token:
        return verify_user_belongs_to_organization(organization_id, get_current_user_from_jwt(jwt_token))
    elif api_key:
        return verify_api_key_access(project_id, ['write','master'], api_key)
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No credentials provided"
        )
def verify_api_key_access(
        project_id: uuid.UUID,
        roles: List[str] = ['master','read','write'],
        api_key: Optional[HTTPAuthorizationCredentials] = Depends(header_scheme)
):
    key_type, key_project_id = validate_api_key(api_key, project_id)
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

def generate_filter_condition(prop_name, operator, prop_value):
    if operator == "eq":
        if isinstance(prop_value, (int, float)):
            return f"{prop_name} = {prop_value}"
        else:
            return f"{prop_name} = '{prop_value}'"
    elif operator == "ne":
        if isinstance(prop_value, (int, float)):
            return f"{prop_name} != {prop_value}"
        else:
            return f"{prop_name} != '{prop_value}'"
    elif operator == "lt":
        if isinstance(prop_value, (int, float)):
            return f"{prop_name} < {prop_value}"
        else:
            return f"{prop_name} < '{prop_value}'"
    elif operator == "lte":
        if isinstance(prop_value, (int, float)):
            return f"{prop_name} <= {prop_value}"
        else:
            return f"{prop_name} <= '{prop_value}'"
    elif operator == "gt":
        if isinstance(prop_value, (int, float)):
            return f"{prop_name} > {prop_value}"
        else:
            return f"{prop_name} > '{prop_value}'"
    elif operator == "gte":
        if isinstance(prop_value, (int, float)):
            return f"{prop_name} >= {prop_value}"
        else:
            return f"{prop_name} >= '{prop_value}'"
    elif operator == "in":
        if all(isinstance(v, (int, float)) for v in prop_value):
            values = ", ".join([str(v) for v in prop_value])
        else:
            values = ", ".join([f"'{v}'" for v in prop_value])
        return f"{prop_name} IN ({values})"
    elif operator == "contains":
        return f"{prop_name} CONTAINS '{prop_value}'"
    elif operator == "not_contains":
        return f"{prop_name} NOT CONTAINS '{prop_value}'"
    return ""

def get_interval_start(timestamp, reference_time, interval_unit, interval_value=1):
    if interval_unit.lower() in ['minutes', 'minute']:
        # Align to the start of the minute interval from the reference point
        total_minutes_since_reference = int((timestamp - reference_time).total_seconds() // 60)
        interval_start_minutes = (total_minutes_since_reference // interval_value) * interval_value
        interval_start = reference_time + timedelta(minutes=interval_start_minutes)
        return interval_start.replace(second=0, microsecond=0)

    elif interval_unit.lower() in ['hours', 'hour']:
        # Align to the start of the hour interval from the reference point
        total_hours_since_reference = int((timestamp - reference_time).total_seconds() // 3600)
        interval_start_hours = (total_hours_since_reference // interval_value) * interval_value
        interval_start = reference_time + timedelta(hours=interval_start_hours)
        return interval_start.replace(minute=0, second=0, microsecond=0)

    elif interval_unit.lower() in ['day', 'days']:
        # Align to the start of the day interval, using the reference time
        start_date = reference_time.replace(hour=0, minute=0, second=0, microsecond=0)
        days_offset = (timestamp - start_date).days % interval_value
        interval_start = start_date + timedelta(days=((timestamp - start_date).days - days_offset))
        return interval_start

    elif interval_unit.lower() in ['weeks', 'week']:
        # Align to the start of the week interval, starting from the reference week (align to Monday)
        start_of_week = reference_time - timedelta(days=reference_time.weekday())
        weeks_since_reference = (timestamp - start_of_week).days // 7
        interval_start_week = (weeks_since_reference // interval_value) * interval_value
        interval_start = start_of_week + timedelta(weeks=interval_start_week)
        return interval_start.replace(hour=0, minute=0, second=0, microsecond=0)

    elif interval_unit.lower() in ['month', 'months']:
        # Calculate the "interval_value" month start from the reference time
        month_offset = ((timestamp.year - reference_time.year) * 12 + timestamp.month - reference_time.month) % interval_value
        new_month = timestamp.month - month_offset
        if new_month <= 0:
            new_month += 12
            return timestamp.replace(year=timestamp.year - 1, month=new_month, day=1, hour=0, minute=0, second=0, microsecond=0)
        return timestamp.replace(month=new_month, day=1, hour=0, minute=0, second=0, microsecond=0)

    else:
        raise ValueError(f"Unsupported interval unit: {interval_unit}")

# Modify the aggregate_data function to support intervals consistently aligned with reference time
def aggregate_data(data, interval_value, interval_unit, stat, attribute, group_by):
    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # If group_by is not in the DataFrame, raise an error
    if group_by not in df.columns:
        raise KeyError(f"The column '{group_by}' does not exist in the data.")

    # Find the earliest timestamp to use as a reference point for interval calculation
    reference_time = df['timestamp'].min()

    # Calculate the interval start times consistently using the reference time
    df['interval_start'] = df.apply(lambda row: get_interval_start(row['timestamp'], reference_time, interval_unit, interval_value), axis=1)

    # Check if 'interval_start' was successfully added
    if 'interval_start' not in df.columns:
        raise KeyError("The 'interval_start' column could not be created.")
    if stat == 'avg':
        grouped = df.groupby([group_by, 'interval_start']).agg({attribute: 'mean'}).reset_index()
    elif stat == 'max':
        grouped = df.groupby([group_by, 'interval_start']).agg({attribute: 'max'}).reset_index()
    elif stat == 'min':
        grouped = df.groupby([group_by, 'interval_start']).agg({attribute: 'min'}).reset_index()
    elif stat == 'sum':
        grouped = df.groupby([group_by, 'interval_start']).agg({attribute: 'sum'}).reset_index()
    elif stat == 'count':
        grouped = df.groupby([group_by, 'interval_start']).agg({attribute: 'count'}).reset_index()
    else:
        raise ValueError(f"Unsupported statistical operation: {stat}")

    grouped.rename(columns={attribute: f"{stat}_{attribute}"}, inplace=True)

    # Round the values to 3 decimal places
    grouped[f"{stat}_{attribute}"] = grouped[f"{stat}_{attribute}"].round(3)
    return grouped.to_dict(orient='records')