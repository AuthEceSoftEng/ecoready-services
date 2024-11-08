import uuid
from cassandra.cluster import Session
from models.project_models import ProjectCreateRequest, ProjectResponse, ProjectUpdateRequest
from utilities.cassandra_connector import get_cassandra_session
from utilities.organization_utils import get_organization_by_id

session = get_cassandra_session()

async def create_project_in_db(organization_id: uuid.UUID, project_data: ProjectCreateRequest):

    project_id = uuid.uuid4()
    query = """
    INSERT INTO project (id, organization_id, project_name, description, tags, creation_date)
    VALUES (%s, %s, %s, %s, %s, toTimestamp(now()))
    """
    session.execute(query, (project_id, organization_id, project_data.project_name, project_data.description, project_data.tags))
    
    return {"project_id": project_id}


async def update_project_in_db(project_id: uuid.UUID, project_data: ProjectUpdateRequest):

    update_query = "UPDATE project SET "
    update_params = []

    if project_data.description is not None:
        update_query += "description=%s, "
        update_params.append(project_data.description)

    if project_data.tags is not None:
        update_query += "tags=%s, "
        update_params.append(project_data.tags)

    update_query = update_query.rstrip(", ") + " WHERE id=%s"
    update_params.extend([project_id])

    session.execute(update_query, tuple(update_params))
    return True

async def delete_project_in_db(project_id: uuid.UUID):

    query = "DELETE FROM project WHERE id=%s"
    session.execute(query, (project_id,))
    return True

async def get_all_organization_projects_from_db(organization_id: uuid.UUID):

    query = "SELECT id, project_name, description, tags, creation_date, organization_id FROM project WHERE organization_id=%s ALLOW FILTERING"
    return session.execute(query, (organization_id,)).all()

def get_project_by_id(project_id: uuid.UUID, organization_id: uuid.UUID):

    query = "SELECT id, project_name, description, tags, creation_date, organization_id FROM project WHERE id=%s AND organization_id=%s LIMIT 1 ALLOW FILTERING"
    return session.execute(query, (project_id,organization_id)).one()
