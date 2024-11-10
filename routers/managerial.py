from fastapi import APIRouter, FastAPI, HTTPException, Depends, Query, WebSocket, WebSocketDisconnect, Body
from dependencies import get_current_user
from cassandra.cluster import Cluster
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, AclBinding, AclOperation, AclPermissionType, AclBindingFilter, ResourceType, ResourcePatternType, NewTopic
from typing import List, Dict, Any, Optional
import json
import threading 
import time
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
import docker
docker_client = docker.from_env()
import uuid
from cassandra.cluster import Session
from pydantic import BaseModel
import uuid
import hashlib
from datetime import datetime
from fastapi import Query
class UserCreateRequest(BaseModel):
    username: str
    password: str
class UpdatePasswordRequest(BaseModel):
    password: str
TAG = "Organization Management"
app = FastAPI()
router = APIRouter()

CASSANDRA_CONTACT_POINTS = ['155.207.19.242']  # Replace with >
CASSANDRA_PORT = 9042
KAFKA_BOOTSTRAP_SERVERS='155.207.19.243:59498'
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RetryPolicy
from cassandra.query import SimpleStatement

# Define an execution profile with an increased timeout
execution_profile = ExecutionProfile(
    request_timeout=60.0,  # Increase the timeout to 60 seconds
    retry_policy=RetryPolicy()  # Optional: Retry policy for failed requests
)

# Apply the execution profile to the cluster
cluster = Cluster(
    contact_points=['155.207.19.242'],
    port=9042,
    execution_profiles={EXEC_PROFILE_DEFAULT: execution_profile}
)
session = cluster.connect('metadata')

@router.post("/api/organizations", tags=[TAG])
async def create_organization(organization_name: str, description: str, tags1: List[str] = Query(), user: dict = Depends(get_current_user)):
    # Step 1:
    if user["role"]!="admin":
        raise HTTPException(status_code=401, detail="Unauthorized")
    query = "SELECT id FROM organization WHERE organization_name=%s LIMIT 1 allow filtering"
    existing_org = session.execute(query, (organization_name,)).one()
    if existing_org:
        raise HTTPException(status_code=409, detail="Organization already exists")
    org_id = uuid.uuid4()
    insert_query = """
    INSERT INTO organization (id, organization_name, description, creation_date, tags)
    VALUES (%s, %s, %s, toTimestamp(now()), %s)
    """
    session.execute(
        insert_query,
        (org_id, organization_name, description, tags1)
    )
    keyspace_name = organization_name.lower()
    replication_strategy = {
        'class': 'SimpleStrategy',
        'replication_factor': '2'
    }
    create_keyspace_query = f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
    WITH replication = {replication_strategy}
    """

    try:
        session.execute(create_keyspace_query)
    except Exception as e:
        delete_query = "DELETE FROM organization WHERE id=%s"
        session.execute(delete_query, (org_id,))
        # If keyspace creation fails, roll back the organization creation (if desired)
        # You may want to add logic to delete the previously inserted organization
        raise HTTPException(status_code=500, detail=f"Failed to create keyspace: {str(e)}")


    return {"message": "Organization created successfully", "organization_id": str(org_id)}

@router.put("/api/organizations/{organization_name}", tags=[TAG])
async def update_organization(
    organization_name: str,
    description: str = Query(None),
    tags1: List[str] = Query(None),
    user: dict = Depends(get_current_user)
):
    if user["role"] != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    query = "SELECT id FROM organization WHERE organization_name=%s LIMIT 1 allow filtering"
    existing_org = session.execute(query, (organization_name,)).one()
    if not existing_org:
        raise HTTPException(status_code=404, detail="Organization not found")

    update_query = "UPDATE organization SET "
    update_params = []

    if description:
        update_query += "description=%s, "
        update_params.append(description)
    if tags1:
        update_query += "tags=%s, "
        update_params.append(tags1)

    update_query = update_query.rstrip(", ") + " WHERE id=%s"
    update_params.append(existing_org.id)

    session.execute(update_query, tuple(update_params))

    return {"message": "Organization successfully updated"}
@router.delete("/api/organizations/{organization_name}", tags=[TAG])
async def delete_organization(
    organization_name: str,
    user: dict = Depends(get_current_user)
):
    if user["role"] != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Fetch the organization by name using ALLOW FILTERING
    query = "SELECT id FROM organization WHERE organization_name=%s ALLOW FILTERING"
    existing_org = session.execute(query, (organization_name,)).one()
    if not existing_org:
        raise HTTPException(status_code=404, detail="Organization not found")
    
    organization_id = existing_org.id

    # Delete all users associated with the organization
    user_query = "SELECT id FROM user WHERE organization_id=%s ALLOW FILTERING"
    users = session.execute(user_query, (organization_id,))
    for user in users:
        session.execute("DELETE FROM user WHERE id=%s", (user.id,))

    # Delete all projects associated with the organization
    project_query = "SELECT id, project_name FROM project WHERE organization_id=%s ALLOW FILTERING"
    projects = session.execute(project_query, (organization_id,))
    kafka_topics_to_delete = []
    for project in projects:
        project_id = project.id
        project_name=project.project_name
        # Delete all collections associated with each project
        collection_query = "SELECT id, collection_name FROM collection WHERE project_id=%s ALLOW FILTERING"
        collections = session.execute(collection_query, (project_id,))
        for collection in collections:
            collection_id, collection_name = collection  # Unpacking the tuple
            try:
                container_name = f"{organization_name}_{project_name}_{collection_name}_consumer"
                container = docker_client.containers.get(container_name)
                container.stop()
                container.remove()
            except docker.errors.NotFound:
                continue
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Failed to remove consumer container: {str(e)}")
            # Add the Kafka topic to the list of topics to delete
            kafka_topics_to_delete.append(f"{organization_name.lower()}.{project_name.lower()}.{collection_name.lower()}")
            session.execute("DELETE FROM collection WHERE id=%s", (collection_id,))        
        # Delete API keys associated with each project
        api_key_query = "SELECT id FROM api_keys WHERE project_id=%s ALLOW FILTERING"
        api_keys = session.execute(api_key_query, (project.id,))
        for api_key in api_keys:
            session.execute("DELETE FROM api_keys WHERE id=%s", (api_key.id,))
        
        # Finally, delete the project itself
        session.execute("DELETE FROM project WHERE id=%s", (project.id,))

    # Delete the organization itself
    session.execute("DELETE FROM organization WHERE id=%s", (organization_id,))

    # Drop the keyspace associated with the organization
    keyspace_name = organization_name.lower()
    drop_keyspace_query = f"DROP KEYSPACE IF EXISTS {keyspace_name}"
    session.execute(drop_keyspace_query)
    kafka_admin_client = AdminClient({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    })
    if len(kafka_topics_to_delete) >  0:
        delete_topics_result = kafka_admin_client.delete_topics(kafka_topics_to_delete, operation_timeout=60)

        # Handle any errors that may arise during topic deletion
        for topic, future in delete_topics_result.items():
            try:
                future.result(timeout=60)  # Block until the topic is actually deleted
                print(f"Deleted topic: {topic}")
            except Exception as e:
                raise HTTPException(status_code=401, detail=f"Failed to delete topic {topic}: {e}")
    return {"message": "Organization and all related records successfully deleted"}


@router.get("/api/organizations/{organization_name}", tags=[TAG])
async def get_organization_info(
    organization_name: str,
    user: dict = Depends(get_current_user)
):
    if user["role"] != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Fetch the organization by name using ALLOW FILTERING
    query = "SELECT * FROM organization WHERE organization_name=%s ALLOW FILTERING"
    organization = session.execute(query, (organization_name,)).one()
    
    if not organization:
        raise HTTPException(status_code=404, detail="Organization not found")

    return {
        "organization_name": organization.organization_name,
        "description": organization.description,
        "creation_date": organization.creation_date,
        "tags": organization.tags
    }

@router.get("/api/organizations", tags=[TAG])
async def get_all_organizations(
    user: dict = Depends(get_current_user)
):
    if user["role"] != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    query = "SELECT id, organization_name, description, creation_date, tags FROM organization"
    organizations = session.execute(query).all()

    return {
        "organizations": [
            {
                "organization_name": org.organization_name,
                "description": org.description,
                "creation_date": org.creation_date,
                "tags": org.tags
            } for org in organizations
        ]
    }
TAG = "Organization User Management"

@router.post("/api/organizations/{organization_name}/users", tags=[TAG])
async def create_user(
    organization_name: str,
    user_data: UserCreateRequest,
    user: dict = Depends(get_current_user)
):
    if user["role"] != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Fetch the organization by name
    query = "SELECT id FROM organization WHERE organization_name=%s ALLOW FILTERING"
    organization = session.execute(query, (organization_name,)).one()
    if not organization:
        raise HTTPException(status_code=404, detail="Organization not found")

    # Check if the user already exists
    query = "SELECT id FROM user WHERE username=%s AND organization_id=%s ALLOW FILTERING"
    existing_user = session.execute(query, (user_data.username, organization.id)).one()
    if existing_user:
        raise HTTPException(status_code=409, detail="User already exists")

    # Insert the new user into the database
    user_id = uuid.uuid4()
    insert_query = """
    INSERT INTO user (id, organization_id, username, password)
    VALUES (%s, %s, %s, %s)
    """
    session.execute(insert_query, (user_id, organization.id, user_data.username, user_data.password))

    return {"message": "User created successfully", "user_id": str(user_id)}

@router.delete("/api/organizations/{organization_name}/users/{username}", tags=[TAG])
async def delete_user(
    organization_name: str,
    username: str,
    user: dict = Depends(get_current_user)
):
    if user["role"] != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Fetch the organization by name
    query = "SELECT id FROM organization WHERE organization_name=%s ALLOW FILTERING"
    organization = session.execute(query, (organization_name,)).one()
    if not organization:
        raise HTTPException(status_code=404, detail="Organization not found")

    # Check if the user exists
    query = "SELECT id FROM user WHERE username=%s AND organization_id=%s ALLOW FILTERING"
    existing_user = session.execute(query, (username, organization.id)).one()
    if not existing_user:
        raise HTTPException(status_code=404, detail="User not found")

    # Delete the user from the database
    delete_query = "DELETE FROM user WHERE id=%s"
    session.execute(delete_query, (existing_user.id,))

    return {"message": "User deleted successfully"}

@router.put("/api/organizations/{organization_name}/users/{username}", tags=[TAG])
async def update_user_password(
    organization_name: str,
    username: str,
    password_data: UpdatePasswordRequest,
    user: dict = Depends(get_current_user)
):
    if user["role"] != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Fetch the organization by name
    query = "SELECT id FROM organization WHERE organization_name=%s ALLOW FILTERING"
    organization = session.execute(query, (organization_name,)).one()
    if not organization:
        raise HTTPException(status_code=404, detail="Organization not found")

    # Check if the user exists
    query = "SELECT id FROM user WHERE username=%s AND organization_id=%s ALLOW FILTERING"
    existing_user = session.execute(query, (username, organization.id)).one()
    if not existing_user:
        raise HTTPException(status_code=404, detail="User not found")

    # Update the user's password
    update_query = "UPDATE user SET password=%s WHERE id=%s"
    session.execute(update_query, (password_data.password, existing_user.id))

    return {"message": "User password successfully updated"}

@router.get("/api/organizations/{organization_name}/users", tags=[TAG])
async def get_all_users(
    organization_name: str,
    user: dict = Depends(get_current_user)
):
    if user["role"] != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Fetch the organization by name
    query = "SELECT id FROM organization WHERE organization_name=%s ALLOW FILTERING"
    organization = session.execute(query, (organization_name,)).one()
    if not organization:
        raise HTTPException(status_code=404, detail="Organization not found")

    # Fetch all users in the organization
    query = "SELECT id, username FROM user WHERE organization_id=%s allow filtering"
    users = session.execute(query, (organization.id,)).all()

    return {
        "users": [
            {
                "username": user.username
            } for user in users
        ]
    }

def verify_user_belongs_to_organization(user: dict, organization_name: str) -> bool:
    # Fetch the organization ID based on the organization name
    query = "SELECT id FROM organization WHERE organization_name=%s ALLOW FILTERING"
    organization = session.execute(query, (organization_name,)).one()
    if not organization:
        raise HTTPException(status_code=404, detail="Organization not found")

    # Check if the user's organization ID matches
    return user["organization_id"] == organization.id

from typing import List
TAG = "Organization Projects Management"

@router.post("/api/organizations/{organization_name}/projects", tags=[TAG])
async def create_project(
    organization_name: str,
    project_name: str = Query(...),
    description: str = Query(...),
    tags: List[str] = Query(...),
    user: dict = Depends(get_current_user)
):
    # Verify if the user belongs to the organization
    if not verify_user_belongs_to_organization(user, organization_name):
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Check if the project already exists
    query = "SELECT id FROM project WHERE project_name=%s AND organization_id=%s ALLOW FILTERING"
    existing_project = session.execute(query, (project_name, user["organization_id"])).one()
    if existing_project:
        raise HTTPException(status_code=409, detail="Project already exists")

    # Insert the new project into the database
    project_id = uuid.uuid4()
    insert_query = """
    INSERT INTO project (id, organization_id, project_name, description, tags, creation_date)
    VALUES (%s, %s, %s, %s, %s, toTimestamp(now()))
    """
    session.execute(insert_query, (project_id, user["organization_id"], project_name, description, tags))

    return {"message": "Project successfully created", "project_id": str(project_id)}

from typing import List
from fastapi import Query

@router.put("/api/organizations/{organization_name}/projects/{project_name}", tags=[TAG])
async def update_project(
    organization_name: str,
    project_name: str,
    description: str = Query(None),
    tags: List[str] = Query(None),
    user: dict = Depends(get_current_user)
):
    # Verify if the user belongs to the organization
    if not verify_user_belongs_to_organization(user, organization_name):
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Check if the project exists
    query = "SELECT id FROM project WHERE project_name=%s AND organization_id=%s ALLOW FILTERING"
    existing_project = session.execute(query, (project_name, user["organization_id"])).one()
    if not existing_project:
        raise HTTPException(status_code=404, detail="Project not found")

    # Build the update query dynamically
    update_fields = []
    update_values = []

    if description is not None:
        update_fields.append("description=%s")
        update_values.append(description)

    if tags is not None:
        update_fields.append("tags=%s")
        update_values.append(tags)

    if not update_fields:
        raise HTTPException(status_code=400, detail="No fields to update")

    # Add the project ID to the update values
    update_values.append(existing_project.id)

    # Create the final update query
    update_query = f"UPDATE project SET {', '.join(update_fields)} WHERE id=%s"

    # Execute the update query
    session.execute(update_query, tuple(update_values))

    return {"message": "Project successfully updated"}

@router.delete("/api/organizations/{organization_name}/projects/{project_name}", tags=[TAG])
async def delete_project(
    organization_name: str,
    project_name: str,
    user: dict = Depends(get_current_user)
):
    # Verify if the user belongs to the organization
    if not verify_user_belongs_to_organization(user, organization_name):
        raise HTTPException(status_code=401, detail="Unauthorized")
    # Check if the project exists
    query = "SELECT id FROM project WHERE project_name=%s AND organization_id=%s ALLOW FILTERING"
    existing_project = session.execute(query, (project_name, user["organization_id"])).one()
    if not existing_project:
        raise HTTPException(status_code=404, detail="Project not found")
    project_id = existing_project.id
    kafka_topics_to_delete = []
    # Step 1: Delete related records from the collection, api_keys, and user tables
    # Delete collections related to the project
    collection_query = "SELECT id, collection_name FROM collection WHERE project_id=%s ALLOW FILTERING"
    collections = session.execute(collection_query, (project_id,))
    for collection in collections:
        collection_id, collection_name = collection
        
        # Drop the corresponding table in Cassandra
        keyspace_name = organization_name.lower()
        table_name = f"{project_name.lower()}_{collection_name.lower()}"
        drop_table_query = f"DROP TABLE IF EXISTS {keyspace_name}.{table_name}"
        try:
            session.execute(drop_table_query)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to drop table {table_name}: {str(e)}")
        try:
            container_name = f"{organization_name}_{project_name}_{collection_name}_consumer"
            container = docker_client.containers.get(container_name)
            container.stop()
            container.remove()
        except docker.errors.NotFound:
            continue
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to remove consumer container: {str(e)}")        
        # Add the Kafka topic to the list of topics to delete
        kafka_topics_to_delete.append(f"{organization_name.lower()}.{project_name.lower()}.{collection_name.lower()}")
        
        session.execute("DELETE FROM collection WHERE id=%s", (collection.id,))

    # Delete API keys related to the project
    api_keys_query = "SELECT id FROM api_keys WHERE project_id=%s ALLOW FILTERING"
    api_keys = session.execute(api_keys_query, (project_id,))
    for api_key in api_keys:
        session.execute("DELETE FROM api_keys WHERE id=%s", (api_key.id,))
        
    # Delete the project
    delete_query = "DELETE FROM project WHERE id=%s"
    session.execute(delete_query, (existing_project.id,))
    kafka_admin_client = AdminClient({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    })
    if len(kafka_topics_to_delete) >  0:
        delete_topics_result = kafka_admin_client.delete_topics(kafka_topics_to_delete, operation_timeout=60)
    
        # Handle any errors that may arise during topic deletion
        for topic, future in delete_topics_result.items():
            try:
                future.result(timeout=60)  # Block until the topic is actually deleted
                print(f"Deleted topic: {topic}")
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Failed to delete topic {topic}: {e}")
    return {"message": "Project successfully deleted"}

@router.get("/api/organizations/{organization_name}/projects", tags=[TAG])
async def get_all_projects(
    organization_name: str,
    user: dict = Depends(get_current_user)
):
    # Verify if the user belongs to the organization
    if not verify_user_belongs_to_organization(user, organization_name):
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Fetch all projects for the organization
    query = "SELECT id, project_name, description, tags, creation_date FROM project WHERE organization_id=%s allow filtering"
    projects = session.execute(query, (user["organization_id"],)).all()

    return {
        "projects": [
            {
                "project_name": project.project_name,
                "description": project.description,
                "tags": project.tags,
                "creation_date": project.creation_date
            } for project in projects
        ]
    }

@router.get("/api/organizations/{organization_name}/projects/{project_name}", tags=[TAG])
async def get_project_info(
    organization_name: str,
    project_name: str,
    user: dict = Depends(get_current_user)
):
    # Verify if the user belongs to the organization
    if not verify_user_belongs_to_organization(user, organization_name):
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Fetch the project information
    query = "SELECT id, project_name, description, tags, creation_date FROM project WHERE project_name=%s AND organization_id=%s ALLOW FILTERING"
    project = session.execute(query, (project_name, user["organization_id"])).one()

    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    return {
        "project_name": project.project_name,
        "description": project.description,
        "tags": project.tags,
        "creation_date": project.creation_date
    }
TAG = "Project Key Management"

def generate_key(project_id: uuid.UUID, key_type: str) -> str:
    # Generate a new key using UUID and hashing
    new_key = hashlib.sha256(f"{project_id}-{key_type}-{uuid.uuid4()}".encode()).hexdigest()
    return new_key

@router.post("/api/organizations/{organization_name}/projects/{project_name}/keys", tags=[TAG])
async def generate_project_key(
    organization_name: str,
    project_name: str,
    key_type: str = Query('read', enum=['read', 'write', 'master']),
    user: dict = Depends(get_current_user)
):
    # Verify if the user belongs to the organization
    if not verify_user_belongs_to_organization(user, organization_name):
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Check if the project exists
    query = "SELECT id FROM project WHERE project_name=%s AND organization_id=%s ALLOW FILTERING"
    project = session.execute(query, (project_name, user["organization_id"])).one()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    # Generate a new key
    new_key = generate_key(project.id, key_type)

    # Check if a key of the specified type already exists for this project
    key_query = "SELECT id FROM metadata.api_keys WHERE project_id=%s AND key_type=%s allow filtering"
    existing_key = session.execute(key_query, (project.id, key_type)).one()

    if existing_key:
        # Update the existing key
        update_query = """
        UPDATE metadata.api_keys SET api_key=%s, created_at=%s WHERE id=%s
        """
        session.execute(update_query, (new_key, datetime.utcnow(), existing_key.id))
        return {"message": "Key updated successfully", "key_value": new_key}
    else:
        # Insert the new key into the api_keys table
        key_id = uuid.uuid4()
        insert_query = """
        INSERT INTO metadata.api_keys (id, api_key, created_at, key_type, project_id)
        VALUES (%s, %s, %s, %s, %s)
        """
        session.execute(insert_query, (key_id, new_key, datetime.utcnow(), key_type, project.id))
        return {"message": "Key generated successfully", "key_value": new_key}

@router.get("/api/organizations/{organization_name}/projects/{project_name}/keys", tags=[TAG])
async def get_project_key(
    organization_name: str,
    project_name: str,
    key_type: str = Query('read', enum=['read', 'write', 'master']),
    user: dict = Depends(get_current_user)
):
    # Verify if the user belongs to the organization
    if not verify_user_belongs_to_organization(user, organization_name):
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Check if the project exists
    query = "SELECT id FROM project WHERE project_name=%s AND organization_id=%s ALLOW FILTERING"
    project = session.execute(query, (project_name, user["organization_id"])).one()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    # Retrieve the requested key
    key_query = "SELECT api_key FROM metadata.api_keys WHERE project_id=%s AND key_type=%s allow filtering"
    project_key = session.execute(key_query, (project.id, key_type)).one()

    if not project_key:
        raise HTTPException(status_code=404, detail="Key not found")

    return {"key_value": project_key.api_key}


TAG = "Collection Management"
from confluent_kafka.admin import AdminClient, NewTopic

@router.post("/api/organizations/{organization_name}/projects/{project_name}/collections", tags=[TAG])
async def create_collection(
    organization_name: str,
    project_name: str,
    name: str,
    description: str,
    schema: Dict[str, Any],
    tags: List[str] = Query(...),
    user: dict = Depends(get_current_user)
):
    print(schema)
    # Check if the user has the master role
    if user["role"] != "master":
        raise HTTPException(status_code=403, detail="Forbidden: Master key required")

    # Fetch the organization ID using the organization name
    org_query = "SELECT id FROM organization WHERE organization_name=%s ALLOW FILTERING"
    organization = session.execute(org_query, (organization_name,)).one()
    
    # If organization is not found
    if not organization:
        raise HTTPException(status_code=404, detail="Organization not found")

    # Fetch the project using the organization ID
    project_query = "SELECT id FROM project WHERE project_name=%s AND organization_id=%s ALLOW FILTERING"
    project = session.execute(project_query, (project_name, organization.id)).one()
    
    # Case 1: The project doesn't exist
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    
    # Case 2: The project exists but the project ID does not match the token's project ID
    if project.id != user["project_id"]:
        raise HTTPException(status_code=403, detail="Access denied.")

    # Check if the collection already exists
    collection_query = "SELECT id FROM collection WHERE collection_name=%s AND project_id=%s ALLOW FILTERING"
    existing_collection = session.execute(collection_query, (name, project.id)).one()
    if existing_collection:
        raise HTTPException(status_code=409, detail="Collection already exists")

    # Add the default fields to the schema
    schema["day"] = "DATE"
    schema["timestamp"] = "TIMESTAMP"
    
    if "key" not in schema:
        schema["key"] = "TEXT"

    # Construct the CREATE TABLE query
    keyspace_name = organization_name.lower()
    table_name = f"{project_name.lower()}_{name.lower()}"

    columns = ", ".join([f"{col_name.lower()} {col_type.lower()}" for col_name, col_type in schema.items()])
    primary_key = "((key, day), timestamp)"  # Key and day as the partition key, and timestamp as the clustering key

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {keyspace_name}.{table_name} (
        {columns},
        PRIMARY KEY {primary_key}
    ) WITH CLUSTERING ORDER BY (timestamp DESC)
    """

    try:
        session.execute(create_table_query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create collection table: {str(e)}")

    # Create the Kafka topic
    kafka_topic_name = f"{organization_name.lower()}.{project_name.lower()}.{name.lower()}"

    kafka_admin_client = AdminClient({'bootstrap.servers': '155.207.19.243:59498'})
    new_topic = NewTopic(kafka_topic_name, num_partitions=4, replication_factor=1)
    fs = kafka_admin_client.create_topics([new_topic])
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} created successfully")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to create Kafka topic {topic}: {str(e)}")

    try:
        container_name = f"{organization_name}_{project_name}_{name}_consumer"
        topic_name = f"{organization_name}.{project_name}.{name}"
        keyspace_name = organization_name.lower()
        table_name = f"{project_name}_{name}"
        
        docker_client.containers.run(
            "kafka-cassandra-consumer",  # Replace with your actual image name
            name=container_name,
            environment={
                "KAFKA_BOOTSTRAP_SERVERS": "155.207.19.243:59498",
                "CASSANDRA_CONTACT_POINTS": "155.207.19.242",
                "CASSANDRA_PORT": "9042",
                "COLLECTION_NAME": name,
                "PROJECT_NAME": project_name,
                "ORGANIZATION_NAME": organization_name
            },
            detach=True,
            restart_policy={"Name": "always"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start consumer container: {str(e)}")
    # Insert the new collection into the collection metadata table
    collection_id = uuid.uuid4()
    insert_query = """
    INSERT INTO collection (id, collection_name, creation_date, description, organization_id, project_id, tags)
    VALUES (%s, %s, toTimestamp(now()), %s, %s, %s, %s)
    """
    session.execute(insert_query, (collection_id, name, description, organization.id, project.id, tags))

    return {"message": "Collection created successfully", "collection_id": str(collection_id)}

@router.put("/api/organizations/{organization_name}/projects/{project_name}/collections/{collection_name}", tags=[TAG])
async def update_collection(
    organization_name: str,
    project_name: str,
    collection_name: str,
    description: str = Query(None),
    tags: List[str] = Query(None),
    user: dict = Depends(get_current_user)
):
    # Check if the user has the master role
    if user["role"] != "master":
        raise HTTPException(status_code=403, detail="Forbidden: Master key required")

    # Fetch the organization ID using the organization name
    org_query = "SELECT id FROM organization WHERE organization_name=%s ALLOW FILTERING"
    organization = session.execute(org_query, (organization_name,)).one()
    
    # If organization is not found
    if not organization:
        raise HTTPException(status_code=404, detail="Organization not found")

    # Fetch the project using the organization ID
    project_query = "SELECT id FROM project WHERE project_name=%s AND organization_id=%s ALLOW FILTERING"
    project = session.execute(project_query, (project_name, organization.id)).one()
    
    # Case 1: The project doesn't exist
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    
    # Case 2: The project exists but the project ID does not match the token's project ID
    if project.id != user["project_id"]:
        raise HTTPException(status_code=403, detail="Access denied: Project ID does not match")

    # Check if the collection exists
    collection_query = "SELECT id FROM collection WHERE collection_name=%s AND project_id=%s ALLOW FILTERING"
    existing_collection = session.execute(collection_query, (collection_name, project.id)).one()
    if not existing_collection:
        raise HTTPException(status_code=404, detail="Collection not found")

    # Dynamically build the update query based on provided fields
    update_fields = []
    update_values = []

    if description is not None:
        update_fields.append("description=%s")
        update_values.append(description)

    if tags is not None:
        update_fields.append("tags=%s")
        update_values.append(tags)

    if not update_fields:
        raise HTTPException(status_code=400, detail="No fields to update")

    # Add the collection ID to the update values
    update_values.append(existing_collection.id)

    # Create the final update query
    update_query = f"UPDATE collection SET {', '.join(update_fields)} WHERE id=%s"

    # Execute the update query
    session.execute(update_query, tuple(update_values))

    return {"message": "Collection updated successfully"}

@router.delete("/api/organizations/{organization_name}/projects/{project_name}/collections/{collection_name}", tags=[TAG])
async def delete_collection(
    organization_name: str,
    project_name: str,
    collection_name: str,
    user: dict = Depends(get_current_user)
):
    # Check if the user has the master role
    if user["role"] != "master":
        raise HTTPException(status_code=403, detail="Forbidden: Master key required")

    # Fetch the organization ID using the organization name
    org_query = "SELECT id FROM organization WHERE organization_name=%s ALLOW FILTERING"
    organization = session.execute(org_query, (organization_name,)).one()
    
    # If organization is not found
    if not organization:
        raise HTTPException(status_code=404, detail="Organization not found")

    # Fetch the project using the organization ID
    project_query = "SELECT id FROM project WHERE project_name=%s AND organization_id=%s ALLOW FILTERING"
    project = session.execute(project_query, (project_name, organization.id)).one()
    
    # Case 1: The project doesn't exist
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    
    # Case 2: The project exists but the project ID does not match the token's project ID
    if project.id != user["project_id"]:
        raise HTTPException(status_code=403, detail="Access denied: Project ID does not match")

    # Check if the collection exists
    collection_query = "SELECT id FROM collection WHERE collection_name=%s AND project_id=%s ALLOW FILTERING"
    existing_collection = session.execute(collection_query, (collection_name, project.id)).one()
    if not existing_collection:
        raise HTTPException(status_code=404, detail="Collection not found")
    # Drop the Cassandra table
    # Construct the Cassandra table name
    keyspace_name = organization_name.lower()
    table_name = f"{project_name.lower()}_{collection_name.lower()}"
    drop_table_query = f"DROP TABLE IF EXISTS {keyspace_name}.{table_name}"
    try:
        session.execute(drop_table_query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete Cassandra table: {str(e)}")

    # Delete the Kafka topic
    kafka_topic_name = f"{organization_name.lower()}.{project_name.lower()}.{collection_name.lower()}"
    kafka_admin_client = AdminClient({'bootstrap.servers': '155.207.19.243:59498'})
    try:
        kafka_admin_client.delete_topics([kafka_topic_name])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete Kafka topic: {str(e)}")
    # Delete the collection
    delete_query = "DELETE FROM collection WHERE id=%s"
    session.execute(delete_query, (existing_collection.id,))
    try:
        container_name = f"{organization_name}_{project_name}_{collection_name}_consumer"
        container = docker_client.containers.get(container_name)
        container.stop()
        container.remove()
    except docker.errors.NotFound:
        raise HTTPException(status_code=404, detail="Consumer container not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to remove consumer container: {str(e)}")

    return {"message": "Collection deleted successfully"}

@router.get("/api/organizations/{organization_name}/projects/{project_name}/collections", tags=[TAG])
async def get_all_collections(
    organization_name: str,
    project_name: str,
    user: dict = Depends(get_current_user)
):
    # Check if the user has at least a read role
    if user["role"] not in ["master"]:
        raise HTTPException(status_code=403, detail="Forbidden: Insufficient permissions")

    # Fetch the organization ID using the organization name
    org_query = "SELECT id FROM organization WHERE organization_name=%s ALLOW FILTERING"
    organization = session.execute(org_query, (organization_name,)).one()
    
    # If organization is not found
    if not organization:
        raise HTTPException(status_code=404, detail="Organization not found")

    # Fetch the project using the organization ID
    project_query = "SELECT id FROM project WHERE project_name=%s AND organization_id=%s ALLOW FILTERING"
    project = session.execute(project_query, (project_name, organization.id)).one()
    
    # Case 1: The project doesn't exist
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    
    # Case 2: The project exists but the project ID does not match the token's project ID
    if project.id != user["project_id"]:
        raise HTTPException(status_code=403, detail="Access denied")

    # Fetch all collections for the project
    collections_query = "SELECT id, collection_name, description, creation_date, tags FROM collection WHERE project_id=%s allow filtering"
    collections = session.execute(collections_query, (project.id,)).all()

    return {
        "collections": [
            {
                "collection_name": collection.collection_name,
                "description": collection.description,
                "creation_date": collection.creation_date,
                "tags": collection.tags
            } for collection in collections
        ]
    }


@router.get("/api/organizations/{organization_name}/projects/{project_name}/collections/{collection_name}", tags=[TAG])
async def get_collection_info(
    organization_name: str,
    project_name: str,
    collection_name: str,
    user: dict = Depends(get_current_user)
):
    # Check if the user has at least a read role
    if user["role"] not in ["master"]:
        raise HTTPException(status_code=403, detail="Forbidden: Insufficient permissions")

    # Fetch the organization ID using the organization name
    org_query = "SELECT id FROM organization WHERE organization_name=%s ALLOW FILTERING"
    organization = session.execute(org_query, (organization_name,)).one()
    
    # If organization is not found
    if not organization:
        raise HTTPException(status_code=404, detail="Organization not found")

    # Fetch the project using the organization ID
    project_query = "SELECT id FROM project WHERE project_name=%s AND organization_id=%s ALLOW FILTERING"
    project = session.execute(project_query, (project_name, organization.id)).one()
    
    # Case 1: The project doesn't exist
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    
    # Case 2: The project exists but the project ID does not match the token's project ID
    if project.id != user["project_id"]:
        raise HTTPException(status_code=403, detail="Access denied: Project ID does not match")

    # Fetch the collection information
    collection_query = "SELECT id, collection_name, creation_date, description, tags FROM collection WHERE collection_name=%s AND project_id=%s ALLOW FILTERING"
    collection = session.execute(collection_query, (collection_name, project.id)).one()

    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found")
    # Fetch the schema of the collection from the system tables
    keyspace_name = organization_name.lower()
    table_name = f"{project_name.lower()}_{collection_name.lower()}"

    schema_query = """
    SELECT column_name, type 
    FROM system_schema.columns 
    WHERE keyspace_name=%s AND table_name=%s
    """
    
    rows = session.execute(schema_query, (keyspace_name, table_name))
    schema = {row.column_name: row.type for row in rows}
    return {
        "collection_name": collection.collection_name,
        "description": collection.description,
        "tags": collection.tags,
        "creation_date":collection.creation_date,
        "schema": schema
    }

TAG="Send Data"

def validate_type(value, expected_type):
    if expected_type in ("text", "varchar"):
        return isinstance(value, str)
    elif expected_type == "int":
        return isinstance(value, int)
    elif expected_type == "float":
        return isinstance(value, (float, int))
    elif expected_type == "timestamp":
        return isinstance(value, str)  # Assume ISO 8601 format; can parse and validate further if needed
    elif expected_type == "date":
        return isinstance(value, str)  # Assume YYYY-MM-DD format; can parse and validate further if needed
    # Add more type checks as needed
    return True  # Default to True if type is not specifically handled
from typing import Dict, Any, Union, List
from confluent_kafka import Producer

# Kafka configuration constants
KAFKA_BOOTSTRAP_SERVERS = '155.207.19.243:59498'
flush_threshold = 3000

def get_kafka_producer():
    return Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'queue.buffering.max.messages': 200000
    })

@router.post("/api/organizations/{organization_name}/projects/{project_name}/collections/{collection_name}/send_data", tags=[TAG])
async def send_data_to_collection(
    organization_name: str,
    project_name: str,
    collection_name: str,
    data: Union[Dict[str, Any], List[Dict[str, Any]]],
    user: dict = Depends(get_current_user)
):
    # Step 1: Verify the user's role
    if user["role"] not in ["write", "master"]:
        raise HTTPException(status_code=403, detail="Forbidden: Insufficient permissions")

    # Step 2: Fetch the organization, project, and collection to validate existence
    org_query = "SELECT id FROM organization WHERE organization_name=%s ALLOW FILTERING"
    organization = session.execute(org_query, (organization_name,)).one()
    if not organization:
        raise HTTPException(status_code=404, detail="Organization not found")

    project_query = "SELECT id FROM project WHERE project_name=%s AND organization_id=%s ALLOW FILTERING"
    project = session.execute(project_query, (project_name, organization.id)).one()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    # Step 3: Retrieve the schema from Cassandra's system_schema
    keyspace_name = organization_name.lower()
    table_name = f"{project_name.lower()}_{collection_name.lower()}"
    
    schema_query = """
    SELECT column_name, type 
    FROM system_schema.columns 
    WHERE keyspace_name=%s AND table_name=%s
    """
    rows = session.execute(schema_query, (keyspace_name, table_name))

    schema = {row.column_name: row.type for row in rows}

    # Step 4: Validate data against the collection's schema with type checking
    if isinstance(data, Dict):
        messages = [data]
    else:
        messages = data

    for message in messages:
        # Ensure 'key' is present in the message
        if 'key' not in message:
            message['key'] = "key"
        if 'field_id' in message:
            message['key'] = message['field_id']
        # Ensure 'timestamp' is present in the message, if not, add it with the current UTC time
        if 'timestamp' not in message:
            message['timestamp'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        # Convert the 'timestamp' string to a datetime object
        timestamp_dt = datetime.fromisoformat(message['timestamp'].replace('Z', '+00:00'))

        # Extract 'day' from the 'timestamp'
        message['day'] = timestamp_dt.strftime('%Y-%m-%d')
        for column_name, column_type in schema.items():
            if column_name not in message:
                raise HTTPException(status_code=422, detail=f"Data does not match the collection's schema. Missing key: {column_name}")
            # Type checking
            if not validate_type(message[column_name], column_type):
                raise HTTPException(status_code=422, detail=f"Data type mismatch for key: {column_name}. Expected type: {column_type}")

    # Step 5: Send data to Kafka
    kafka_producer = get_kafka_producer()
    full_topic_name = f"{organization_name.lower()}.{project_name.lower()}.{collection_name.lower()}"

    for i, message_data in enumerate(messages):
        # Extract the key from the message_data
        message_key = message_data.pop('key')
    
        # Send the rest of the message data to Kafka, excluding the key
        kafka_producer.produce(full_topic_name, key=message_key, value=json.dumps(message_data))
        if (i + 1) % flush_threshold == 0:
            kafka_producer.flush()

    kafka_producer.flush()

    return {"message": f"Data sent to collection '{collection_name}' successfully."}

TAG="Get Data"
class Filter(BaseModel):
    property_name: str
    operator: str
    property_value: Any
    operands: Optional[List['Filter']] = None  # For the 'or' operator
class OrderBy(BaseModel):
    field: str
    order: str  # 'asc' or 'desc'

@router.get("/api/organizations/{organization_name}/projects/{project_name}/collections/{collection_name}/get_data", tags=[TAG])
async def get_collection_data(
    organization_name: str,
    project_name: str,
    collection_name: str,
    attributes: List[str] = Query(None),
    filters: Optional[str] = Query(None),
    order_by: Optional[str] = Query(None),
    user: dict = Depends(get_current_user)
):
    # Check if the user has at least read permissions
    if user["role"] not in ["master", "read"]:
        raise HTTPException(status_code=403, detail="Forbidden: Insufficient permissions")

    # Fetch the organization ID using the organization name
    org_query = "SELECT id FROM organization WHERE organization_name=%s ALLOW FILTERING"
    organization = session.execute(org_query, (organization_name,)).one()
    
    # If organization is not found
    if not organization:
        raise HTTPException(status_code=404, detail="Organization not found")

    # Fetch the project using the organization ID
    project_query = "SELECT id FROM project WHERE project_name=%s AND organization_id=%s ALLOW FILTERING"
    project = session.execute(project_query, (project_name, organization.id)).one()
    
    # If project is not found
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    
    # Fetch the collection's schema
    keyspace_name = organization_name.lower()
    table_name = f"{project_name.lower()}_{collection_name.lower()}"
    schema_query = """
    SELECT column_name, type 
    FROM system_schema.columns 
    WHERE keyspace_name=%s AND table_name=%s
    """
    rows = session.execute(schema_query, (keyspace_name, table_name))
    schema = {row.column_name: row.type for row in rows}

    # Validate attributes
    if attributes:
        for attr in attributes:
            if attr not in schema:
                raise HTTPException(status_code=422, detail=f"Attribute {attr} does not exist in the collection schema.")

    # Build the SELECT statement
    select_clause = ', '.join(attributes) if attributes else '*'
    query = f"SELECT {select_clause} FROM {keyspace_name}.{table_name}"
    # Add filters to the query
    conditions = []
    if filters:
        filters_list = json.loads(filters)  # Parse the filters JSON string into a Python list
        for f in filters_list:
            if f['operator'] == "or":
                or_conditions = []
                for operand in f["operands"]:
                    or_conditions.append(generate_filter_condition(operand["property_name"], operand["operator"], operand["property_value"]))
                conditions.append(f"({' OR '.join(or_conditions)})")
            else:
                conditions.append(generate_filter_condition(f['property_name'], f['operator'], f['property_value']))

    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ALLOW FILTERING"
    print(query)
    # Execute the query and fetch data
    results = session.execute(query)
    results_list = [dict(row._asdict()) for row in results]


    # Apply ordering at the application level
    if order_by:
        order_by_dict = json.loads(order_by)
        if order_by_dict['field'] not in schema:
            raise HTTPException(status_code=422, detail=f"Field {order_by_dict['field']} does not exist in the collection schema.")
        results_list.sort(key=lambda x: x.get(order_by_dict['field']), reverse=(order_by_dict['order'].lower() == "desc"))

    return results_list

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

# Update the API endpoint to properly parse the two-week interval
import pandas as pd
TAG = "Get Data Statistics"
@router.get("/api/organizations/{organization_name}/projects/{project_name}/collections/{collection_name}/statistics", tags=[TAG])
async def get_collection_statistics(
    organization_name: str,
    project_name: str,
    collection_name: str,
    attribute: str,
    stat: str = Query("avg", enum=["avg", "max", "min", "sum", "count"], description="Statistical operation to perform"),
    interval: str = "every_2_days",
    start_time: str = Query(None, description="Start time in format YYYY-MM-DDTHH:MM:SSZ"),
    end_time: str = Query(None, description="End time in format YYYY-MM-DDTHH:MM:SSZ"),
    filters: Optional[str] = Query(None),
    order: Optional[str] = Query(None, enum=["asc", "desc"]),
    group_by: Optional[str] = Query(None),
    user: dict = Depends(get_current_user)
):
    # Check if the user has the necessary role
    if user["role"] not in ["master", "read"]:
        raise HTTPException(status_code=403, detail="Forbidden: Insufficient permissions")

    # Fetch the organization ID using the organization name
    org_query = "SELECT id FROM organization WHERE organization_name=%s ALLOW FILTERING"
    organization = session.execute(org_query, (organization_name,)).one()
    
    # If organization is not found
    if not organization:
        raise HTTPException(status_code=404, detail="Organization not found")

    # Fetch the project using the organization ID
    project_query = "SELECT id FROM project WHERE project_name=%s AND organization_id=%s ALLOW FILTERING"
    project = session.execute(project_query, (project_name, organization.id)).one()
    
    # If project is not found
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    
    # Fetch the collection's schema
    keyspace_name = organization_name.lower()
    table_name = f"{project_name.lower()}_{collection_name.lower()}"
    schema_query = """
    SELECT column_name, type 
    FROM system_schema.columns 
    WHERE keyspace_name=%s AND table_name=%s
    """
    rows = session.execute(schema_query, (keyspace_name, table_name))
    schema = {row.column_name: row.type for row in rows}

    # Validate attribute
    if attribute not in schema:
        raise HTTPException(status_code=422, detail=f"Attribute {attribute} does not exist in the collection schema.") 
    if group_by and group_by not in schema:
        raise HTTPException(status_code=422, detail=f"Group by field {group_by} does not exist in the collection schema.")
    # Default group_by to 'key' if not provided
    group_by = group_by or 'key'
    # Build the SELECT statement
    query = f"SELECT {group_by}, timestamp, {attribute} FROM {keyspace_name}.{table_name}"

    # Add filters to the query
    conditions = []
    if filters:
        filters_list = json.loads(filters)  # Parse the filters JSON string into a Python list
        for f in filters_list:
            if f['operator'] == "or":
                or_conditions = []
                for operand in f["operands"]:
                    or_conditions.append(generate_filter_condition(operand["property_name"], operand["operator"], operand["property_value"]))
                conditions.append(f"({' OR '.join(or_conditions)})")
            else:
                conditions.append(generate_filter_condition(f['property_name'], f['operator'], f['property_value']))

    # Add WHERE conditions if there are any
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    # Add time conditions, starting with WHERE if there are no existing conditions
    if start_time and end_time:
        if not conditions:  # If there were no previous conditions, start with WHERE
            query += f" WHERE timestamp >= '{start_time}' AND timestamp <= '{end_time}'"
        else:
            query += f" AND timestamp >= '{start_time}' AND timestamp <= '{end_time}'"

    query += " ALLOW FILTERING"
    # Execute the query and fetch data
    results = session.execute(query)
    results_list = [dict(row._asdict()) for row in results]
    # Group and aggregate data based on the specified interval
    if interval:
        _, every_n, units = interval.split('_')
        every_n = int(every_n)
        if units not in ["minutes", "hours", "days", "weeks", "months"]:
            raise HTTPException(status_code=422, detail=f"The unit for the interval isn't supported") 
        aggregated_data = aggregate_data(results_list, every_n, units, stat, attribute, group_by)
    else:
        aggregated_data = results_list
    # Apply the renaming of the calculated stat attribute 
    if order:
        aggregated_data.sort(key=lambda x: x.get(f"{stat}_{attribute}"), reverse=(order == "desc"))
    return aggregated_data

TAG = "Get Collection Live Data"

@router.post("/api/organizations/{organization_name}/projects/{project_name}/collections/{collection_name}/live_data", tags=[TAG])
async def start_live_data_consumer(
    organization_name: str,
    project_name: str,
    collection_name: str,
    user: dict = Depends(get_current_user)
):
    # Check if the user has the necessary role
    if user["role"] not in ["master","read"]:
        raise HTTPException(status_code=403, detail="Forbidden: Master key required")

    try:
        container_name = f"{organization_name}_{project_name}_{collection_name}_{user['key']}_live_consumer"
        topic_name = f"{organization_name}.{project_name}.{collection_name}"
        keyspace_name = organization_name.lower()
        table_name = f"{project_name}_{collection_name}"
        print(user['key'])
        # Run the Docker container for the Kafka consumer
        docker_client.containers.run(
            f"kafka-live-consumer",  # Replace with your actual image name
            name=container_name,
            environment={
                "KAFKA_BOOTSTRAP_SERVERS": "155.207.19.243:59498",
                "CASSANDRA_CONTACT_POINTS": "155.207.19.242",
                "CASSANDRA_PORT": "9042",
                "COLLECTION_NAME": collection_name,
                "PROJECT_NAME": project_name,
                "ORGANIZATION_NAME": organization_name,
                "GROUP_ID": f"{collection_name}.{project_name}_{user['key']}_live_group"
            },
            detach=True,
            restart_policy={"Name": "always"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start consumer container: {str(e)}")

    return {"message": f"Consumer container started for collection '{collection_name}'."}

@router.delete("/api/organizations/{organization_name}/projects/{project_name}/collections/{collection_name}/live_data", tags=[TAG])
async def stop_live_data_consumer(
    organization_name: str,
    project_name: str,
    collection_name: str,
    user: dict = Depends(get_current_user)
):
    # Check if the user has the necessary role
    if user["role"] not in ["master","read"]:
        raise HTTPException(status_code=403, detail="Forbidden: Master key required")

    try:
        container_name = f"{organization_name}_{project_name}_{collection_name}_{user['key']}_live_consumer"
        container = docker_client.containers.get(container_name)
        container.stop()
        container.remove()
    except docker.errors.NotFound:
        raise HTTPException(status_code=404, detail="Consumer container not found.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop and remove consumer container: {str(e)}")

    return {"message": f"Consumer container for collection '{collection_name}' stopped and removed."}
import tarfile
import io, os
@router.post("/api/organizations/{organization_name}/projects/{project_name}/collections/{collection_name}/live-aggregation", tags=[TAG])
async def post_live_aggregation(
    organization_name: str,
    project_name: str,
    collection_name: str,
    attribute: str = Body(..., description="Collection’s attribute to perform computation on"),
    stat: str = Body(..., description="Statistical operation to perform", enum=["avg", "median", "max", "min", "sum", "count"]),
    interval: str = Body(..., description="Time interval to group the data by (e.g., every 1 hour)"),
    interval_type: str = Body(..., description="Type of the interval. Can be tumbling or sliding", enum=["tumbling", "sliding"]),
    sliding_factor: Optional[int] = Body(None, description="Sliding factor for the interval, required if interval_type is sliding"),
    group_by: Optional[str] = Body(None, description="Field to group the data by"),
    order_by: Optional[str] = Body(None, description="Field to order the data by and the direction (asc or desc)"),
    user: dict = Depends(get_current_user)
):
    # Check if the user has the necessary role
    if user["role"] not in ["master", "read"]:
        raise HTTPException(status_code=403, detail="Forbidden: Insufficient permissions")

    # Define parameters for Flink script generation
    topic_name = f"{organization_name}.{project_name}.{collection_name}"
    sink_topic = f"{organization_name}.{project_name}.{collection_name}.{interval}.{stat}.{attribute}"
    # Split the interval string into its components
    interval_parts = interval.split('_')

    # Check if the interval string has exactly three parts
    if len(interval_parts) != 3 or interval_parts[0] != "every":
        raise HTTPException(status_code=400, detail="Invalid interval format. Expected format: 'every_n_units'")

    # Extract the 'n' and 'units' components
    every_n = int(interval_parts[1])
    units = interval_parts[2]

    # Validate sliding factor if the interval is sliding
    if interval_type == "sliding" and not sliding_factor:
        raise HTTPException(status_code=400, detail="Sliding factor is required when interval_type is sliding.")

    # Generate the Flink script
    script_content = generate_flink_script(
        project_name=project_name,
        topic_name=topic_name,
        attribute=attribute,
        every_n=every_n,
        units=units,
        metric=stat,
        interval_type=interval_type,
        sliding_factor=sliding_factor,
        group_by=group_by,
        order_by=order_by
    )
    
    # Save the script to a file
    script_file_path = f"{organization_name}.{project_name}.{collection_name}_live_aggregation.py"
    with open(script_file_path, "w") as script_file:
        script_file.write(script_content)
    
    # Deploy the script to Flink JobManager
    try:
        exec_log = deploy_flink_script(script_file_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    return {"status": "success", "log": exec_log.output.decode('utf-8')}

def generate_flink_script(
    project_name: str,
    topic_name: str,
    attribute: str,
    every_n: int,
    units: str,
    metric: str,
    interval_type: str,
    sliding_factor: Optional[int],
    group_by: Optional[str],
    order_by: Optional[str]
) -> str:
    source_table_sql = f"""
t_env.execute_sql(\"\"\"
CREATE TABLE KafkaSource (
    `key` STRING,
    `{attribute}` DOUBLE,
    `timestamp` STRING,
    `event_time` AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
    WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '{topic_name}',
    'properties.bootstrap.servers' = '155.207.19.243:19496',
    'key.format' = 'raw',
    'key.fields' = 'key',
    'value.format' = 'json',
    'value.fields-include' = 'EXCEPT_KEY',
    'scan.startup.mode' = 'earliest-offset'
)
\"\"\")
"""
    
    sink_topic = f"{topic_name}.{every_n}{units}.{metric}.{attribute}"
    sink_table_sql = f"""
t_env.execute_sql(\"\"\"
CREATE TABLE KafkaSink (
    `key` STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    `count` BIGINT,
    {metric}_{attribute} DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = '{sink_topic}',
    'properties.bootstrap.servers' = '155.207.19.243:19496',
    'format' = 'json'
)
\"\"\")
"""
    
    if interval_type == "tumbling":
        window_start_sql = f"TUMBLE_START(`event_time`, INTERVAL '{every_n}' {units.upper()})"
        window_end_sql = f"TUMBLE_END(`event_time`, INTERVAL '{every_n}' {units.upper()})"
        window_sql = f"TUMBLE(`event_time`, INTERVAL '{every_n}' {units.upper()})"
    elif interval_type == "sliding":
        window_start_sql = f"HOP_START(event_time, INTERVAL '{sliding_factor}' {units.upper()}, INTERVAL '{every_n}' {units.upper()})"
        window_end_sql = f"HOP_END(event_time, INTERVAL '{sliding_factor}' {units.upper()}, INTERVAL '{every_n}' {units.upper()})"
        window_sql = f"HOP(event_time, INTERVAL '{sliding_factor}' {units.upper()}, INTERVAL '{every_n}' {units.upper()})"
    else:
        raise ValueError(f"Unsupported interval type: {interval_type}")
    
    aggregation_sql = f"""
t_env.execute_sql(\"\"\"
INSERT INTO KafkaSink
SELECT
    `key`,
    {window_start_sql} as window_start,
    {window_end_sql} as window_end,
    COUNT(*) as `count`,
    {metric.upper()}({attribute}) as {metric}_{attribute}
FROM KafkaSource
GROUP BY `key`, {window_sql}
\"\"\")
"""
 
    return f"""
from pyflink.table import TableEnvironment, EnvironmentSettings

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(env_settings)
table_config = t_env.get_config().set("table.exec.source.idle-timeout", "10000 ms")

{source_table_sql}
{sink_table_sql}
{aggregation_sql}
"""

def deploy_flink_script(script_file_path: str):
    client = docker.from_env()
    container = client.containers.get('test-flink-jobmanager19-1')
    
    with open(script_file_path, 'rb') as script_file:
        tar_stream = io.BytesIO()
        with tarfile.open(fileobj=tar_stream, mode='w') as tar:
            tar_info = tarfile.TarInfo(name=os.path.basename(script_file_path))
            tar_info.size = os.path.getsize(script_file_path)
            tar.addfile(tar_info, script_file)
        tar_stream.seek(0)
        container.put_archive('/opt/flink', tar_stream)
    
    exec_log = container.exec_run(f'/opt/flink/bin/flink run --python /opt/flink/{os.path.basename(script_file_path)}  --jarfile flink-sql-connector-kafka-3.0.2-1.18.jar')
    return exec_log
