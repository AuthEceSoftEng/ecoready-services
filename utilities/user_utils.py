# app/utilities/user_repository.py

from cassandra.cluster import Session
from utilities.cassandra_connector import get_cassandra_session

session: Session = get_cassandra_session()

def get_user_by_username(username: str):
    query = "SELECT * FROM user WHERE username = %s LIMIT 1 ALLOW FILTERING"
    result = session.execute(query, (username,))
    user = result.one()
    if user:
        return user
    return None

async def get_user_by_username_and_org_id(username: str, organization_id: str):
    query = "SELECT id FROM user WHERE username=%s AND organization_id=%s LIMIT 1 ALLOW FILTERING"
    result = session.execute(query, (username, organization_id)).one()
    return result

async def insert_user(user_id: str, organization_id: str, username: str, hashed_password: str):
    query = """
        INSERT INTO user (id, organization_id, username, password)
        VALUES (%s, %s, %s, %s)
    """
    session.execute(query, (user_id, organization_id, username, hashed_password))


async def delete_user_from_db(user_id: str):
    query = "DELETE FROM user WHERE id=%s"
    session.execute(query, (user_id,))


async def update_user_password_in_db(user_id: str, hashed_password: str):
    query = "UPDATE user SET password=%s WHERE id=%s"
    session.execute(query, (hashed_password, user_id))


async def get_all_users_in_organization(organization_id: str):
    query = "SELECT id, username FROM user WHERE organization_id=%s ALLOW FILTERING"
    users = session.execute(query, (organization_id,)).all()
    return [{"username": user.username} for user in users]