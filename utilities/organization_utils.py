from cassandra.cluster import Session
from typing import List
import uuid
from models.organization_models import OrganizationUpdateRequest
from utilities.cassandra_connector import get_cassandra_session

session=get_cassandra_session()

# Insert a new organization into the database
async def insert_organization(org_id, data):
    session.execute(
        """
        INSERT INTO organization (id, organization_name, description, creation_date, tags)
        VALUES (%s, %s, %s, toTimestamp(now()), %s)
        """,
        (org_id, data.organization_name, data.description, data.tags)
    ) 

# Get an organization by its ID
def get_organization_by_id(org_id):
    query = "SELECT id, organization_name, description, creation_date, tags FROM organization WHERE id=%s LIMIT 1"
    return session.execute(query, (org_id,)).one()

# Update an organization in the database
async def update_organization_in_db(org_id: uuid.UUID, description: str = None, tags: List[str] = None):
    update_fields = []
    update_values = []

    if description:
        update_fields.append("description=%s")
        update_values.append(description)

    if tags:
        update_fields.append("tags=%s")
        update_values.append(tags)

    if not update_fields:
        raise ValueError("No fields to update")

    update_values.append(org_id)

    query = f"UPDATE organization SET {', '.join(update_fields)} WHERE id=%s"
    session.execute(query, tuple(update_values))
    return {"message": "Organization updated successfully"}

# Delete an organization from the database
async def delete_organization_from_db(org_id: uuid.UUID):
    query = "DELETE FROM organization WHERE id=%s"
    session.execute(query, (org_id,))

# Create a keyspace in the database
async def create_keyspace_in_db(organization_name: str):
    # Transform organization name: replace spaces with underscores
    keyspace_name = organization_name.replace(" ", "_")
    
    # Quote the keyspace name to preserve case sensitivity and special characters (like underscores)
    quoted_keyspace_name = f'"{keyspace_name}"'
    
    # Define replication strategy
    replication_strategy = {'class': 'SimpleStrategy', 'replication_factor': '2'}
    
    # Create the keyspace query
    query = f"CREATE KEYSPACE IF NOT EXISTS {quoted_keyspace_name} WITH replication = {replication_strategy}"
    session.execute(query)

# Delete a keyspace from the database
async def delete_keyspace_in_db(organization_name: str):
    # Transform organization name: replace spaces with underscores
    keyspace_name = organization_name.replace(" ", "_")
    
    # Quote the keyspace name to preserve case sensitivity and special characters (like underscores)
    keyspace_name = f'"{keyspace_name}"'
    query = f"DROP KEYSPACE IF EXISTS {keyspace_name}"
    session.execute(query)

# Fetch all organizations from the database
async def get_all_organizations_from_db():
    query = "SELECT id, organization_name, description, creation_date, tags FROM organization"
    organizations = session.execute(query).all()
    return organizations

# Fetch an organization by its name (ignoring case sensitivity)
async def get_organization_by_name(organization_name: str):
    query = "SELECT id FROM organization WHERE organization_name=%s LIMIT 1 ALLOW FILTERING"
    result = session.execute(query, (organization_name,)).one()
    return result