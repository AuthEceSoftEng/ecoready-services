from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi import HTTPException, status, Depends
import hashlib
from cassandra.cluster import Cluster,Session
from cassandra_service import CassandraService
#from . import CassandraService
from fastapi.security import HTTPBasic, HTTPBasicCredentials, HTTPBearer, HTTPAuthorizationCredentials
from typing import  Optional
from fastapi import Depends, FastAPI, HTTPException, status
from typing_extensions import Annotated
bearer_security = HTTPBearer(auto_error=False)
basic_security = HTTPBasic(auto_error=False)

users_db = {
    "alice": {
        "username": "alice",
        "password": "alice-secret"
    },
    "bob": {
        "username": "bob",
        "password": "bob-secret"
    },
    "kafka": {
        "username": "kafka",
        "password": "pass123"
    },
    "Admin": {
        "username": "Admin",
        "password": "pass123"
    }
}
# Cassandra configuration
CASSANDRA_CONTACT_POINTS = ['155.207.19.243','155.207.19.242']  # Replace with >
CASSANDRA_PORT = 9042

cluster = Cluster(CASSANDRA_CONTACT_POINTS, port=CASSANDRA_PORT)
session=cluster.connect('metadata')

def get_user_from_db(username: str):
    query = "SELECT username, password, organization_id FROM user WHERE username=%s LIMIT 1 allow filtering"
    user = session.execute(query, (username,)).one()
    if user:
        return {
            "username": user.username,
            "password": user.password,
            "organization_id": user.organization_id,
        }
    return None
def validate_api_key(api_key: str):
    query = "SELECT key_type, project_id FROM api_keys WHERE api_key=%s LIMIT 1 allow filtering"
    key_data = session.execute(query, (api_key,)).one()
    if key_data:
        return key_data.key_type, key_data.project_id
    return None, None

def get_current_user(
    bearer_credentials: Annotated[Optional[HTTPAuthorizationCredentials], Depends(bearer_security)],
    basic_credentials: Annotated[Optional[HTTPBasicCredentials], Depends(basic_security)]
):
    if basic_credentials:
        # Basic Authentication (Admin or Regular User)
        if basic_credentials.username == "admin" and basic_credentials.password == "pass123":
            return {"username": "admin", "role": "admin"}
        else:
            user = get_user_from_db(basic_credentials.username)
            if user and user["password"] == basic_credentials.password:
                return {"username": basic_credentials.username, "organization_id": user["organization_id"]}
            else:
                raise HTTPException(status_code=401, detail="Invalid username or password")

    elif bearer_credentials:
        # Bearer Authentication (Token-based)
        try:
            api_key_type, project_id = validate_api_key(bearer_credentials.credentials)
            if api_key_type:
                return {"role": api_key_type, "project_id": project_id, "key":bearer_credentials.credentials}
            else:
                raise HTTPException(status_code=403, detail="Invalid API Key")
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid token")

    else:
        # No credentials provided
        raise HTTPException(status_code=401, detail="Credentials not provided")

