from cassandra.cluster import Session
import uuid
from datetime import datetime
from utilities.cassandra_connector import get_cassandra_session
import hashlib
from fastapi import HTTPException, status
import logging

session: Session = get_cassandra_session()

# Utility function to generate a project key
def generate_key(project_id: uuid.UUID, key_type: str) -> str:
    return hashlib.sha256(f"{project_id}-{key_type}-{uuid.uuid4()}".encode()).hexdigest()

# Insert a new project key into the database
def insert_project_key(project_id: uuid.UUID, key_type: str) -> str:
    try:
        key_value = generate_key(project_id, key_type)
        key_id = uuid.uuid4()
        
        insert_query = """
        INSERT INTO metadata.api_keys (id, api_key, created_at, key_type, project_id)
        VALUES (%s, %s, %s, %s, %s)
        """
        session.execute(insert_query, (key_id, key_value, datetime.utcnow(), key_type, project_id))

        return key_value
    except Exception as e:
        logging.error(f"Error inserting project key: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create project key")

# Fetch project keys by category or all keys
def fetch_project_keys_by_category(project_id: uuid.UUID, key_category: str):
        try:
            if key_category == "all":
                query = "SELECT id, api_key, key_type, created_at, project_id FROM metadata.api_keys WHERE project_id=%s ALLOW FILTERING"
                rows = session.execute(query, (project_id,))
            else:
                query = "SELECT id, api_key, key_type, created_at, project_id FROM metadata.api_keys WHERE project_id=%s AND key_type=%s ALLOW FILTERING"
                rows = session.execute(query, (project_id, key_category))
        except:
            logging.error(f"Error fetching project keys by category: {e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to fetch project keys")
        keys = rows.all()
        if not keys:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No keys found for the specified category")
        return keys

# Update an existing project key (regenerate a new key)
def update_project_key(key_id: uuid.UUID, current_key_value: str) -> str:
    try:
        new_key_value = generate_key(key_id, current_key_value)

        update_query = """
        UPDATE metadata.api_keys SET api_key=%s, created_at=%s WHERE id=%s
        """
        session.execute(update_query, (new_key_value, datetime.utcnow(), key_id))

        return new_key_value
    except Exception as e:
        logging.error(f"Error updating project key: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update project key")

# Delete keys by category
def delete_keys_by_category(project_id: uuid.UUID, key_category: str):
    removing_keys=fetch_project_keys_by_category(project_id,key_category)
    for key in removing_keys:
         delete_key_by_value(key.id)
    return {"message": "Keys deleted successfully."}

# Delete a specific key by value
def delete_key_by_value(key_id: uuid.UUID):
    try:
        delete_query = "DELETE FROM metadata.api_keys WHERE id=%s"
        session.execute(delete_query, (key_id,))
    except Exception as e:
        logging.error(f"Error deleting key by value: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to delete project key")

# Get a project key by its value
def get_project_key_by_value(key_value: str, project_id: uuid.UUID):
    try:
        query = "SELECT id, api_key, key_type, project_id, created_at FROM metadata.api_keys WHERE api_key=%s and project_id=%s LIMIT 1 ALLOW FILTERING"
        key = session.execute(query, (key_value,project_id)).one()
    except Exception as e:
        logging.error(f"Error retrieving project key by value: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to fetch project key")
    if not key:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Key not found")
    return key
    
