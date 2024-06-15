from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi import HTTPException, status, Depends
import hashlib
from cassandra_service import CassandraService
#from . import CassandraService

security = HTTPBasic()

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

def get_cassandra_service():
    cassandra_service = CassandraService()
    try:
        yield cassandra_service
    finally:
        cassandra_service.close()

def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    user = users_db.get(credentials.username)
    if not user or user["password"] != credentials.password:
        raise HTTPException(
            status_code=401,
            detail="Incorrect username or password",
        )
    return user

