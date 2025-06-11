import datetime
from cassandra.cluster import Session
from fastapi import HTTPException, status
from collections.abc import Mapping, Sequence
from dependencies import contains_special_characters
from typing import Dict, Any, List
from models.collection_models import CollectionCreateRequest, CollectionUpdateRequest
from utilities.cassandra_connector import get_cassandra_session
import uuid
from confluent_kafka.admin import NewTopic
from utilities.kafka_connector import get_kafka_admin_client
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id

session = get_cassandra_session()

PYTHON_TO_CASSANDRA_TYPES = {
    int: 'int',
    float: 'float',
    str: 'text',
    bool: 'boolean'
}

def flatten_object(obj: dict, parent_key: str = '', sep: str = '$', return_value: bool=False) -> dict:
    """
    Flatten a nested dictionary and map Python types to Cassandra types for table creation.
    Also, check that all keys contain only valid characters (letters, numbers, underscores).
    """
    items = []
    simple_types = (int, float, str, bool)  # Define the simple types

    for k, v in obj.items():
        # Check if key contains invalid characters
        if contains_special_characters(k, allow_spaces=False, allow_underscores=True):
            raise HTTPException(
                status_code=400, 
                detail=f"Key '{k}' contains special characters. Only letters, numbers, and underscores are allowed."
            )
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        
        if isinstance(v, Mapping):
            # If value is a dictionary, recursively flatten it
            items.extend(flatten_object(v, new_key, sep=sep).items())
        elif isinstance(v, Sequence) and not isinstance(v, str):
            # If value is a list or sequence, check if all elements are of the same simple type
            if len(v) > 0:
                first_type = type(v[0])
                if all(isinstance(item, first_type) for item in v):
                    if first_type in simple_types:
                        # Map the Python list type to Cassandra list type
                        cassandra_type = PYTHON_TO_CASSANDRA_TYPES[first_type]
                        if return_value:
                            items.append((new_key,v))
                        else:
                            items.append((new_key, f'list<{cassandra_type}>'))
                    else:
                        raise HTTPException(
                            status_code=400, 
                            detail=f"List at {new_key} contains unsupported complex types: {first_type.__name__}"
                        )
                else:
                    raise HTTPException(
                        status_code=400, 
                        detail=f"List at {new_key} contains mixed types."
                    )
            else:
                if return_value:
                    items.append((new_key,v))
                else:
                    items.append((new_key, 'list<text>'))
        else:
            if return_value:
                items.append((new_key, v))
            else:
                # If value is not a dictionary or list, store the corresponding Cassandra type
                cassandra_type = PYTHON_TO_CASSANDRA_TYPES.get(type(v), 'text')
                items.append((new_key, cassandra_type))
    
    return dict(items)

def is_list_of_same_schema(items: list) -> bool:
    """
    Check if all items in the list follow the same schema as the first item.
    The schema includes both the structure and the types of the values.
    """
    if not items or len(items)<2:
        return True
    # Flatten the first item to get its schema
    first_item_schema = flatten_object(items[0])
    # Compare all other items against the first item's schema
    for item in items[1:]:
        item_schema = flatten_object(item)
        if item_schema != first_item_schema:
            raise HTTPException(
                    status_code=400,
                    detail="List contains items with different schemas."
            )   
    return True
# Get collection by ID
def get_collection_by_id(collection_id: uuid.UUID, project_id: uuid.UUID, organization_id: uuid.UUID):

    query = "SELECT id, collection_name, description, tags, creation_date, project_id, organization_id FROM collection WHERE id=%s AND project_id=%s AND organization_id=%s LIMIT 1 ALLOW FILTERING"
    return session.execute(query, (collection_id, project_id,organization_id)).one()

# Insert a new collection into the database
async def insert_collection(organization_id: uuid.UUID, project_id: uuid.UUID, data: CollectionCreateRequest):
    query = """
    INSERT INTO collection (id, collection_name, creation_date, description, organization_id, project_id, tags)
    VALUES (%s, %s, toTimestamp(now()), %s, %s, %s, %s)
    """
    collection_id = uuid.uuid4()
    session.execute(query, (collection_id, data.name, data.description, organization_id, project_id, data.tags))
    return collection_id

# Update an existing collection
async def update_collection_in_db(collection_id: uuid.UUID, data: CollectionUpdateRequest):
    update_query = "UPDATE collection SET "
    update_params = []
    
    if data.description:
        update_query += "description=%s, "
        update_params.append(data.description)
    
    if data.tags:
        update_query += "tags=%s, "
        update_params.append(data.tags)

    update_query = update_query.rstrip(", ") + " WHERE id=%s"
    update_params.append(collection_id)
    
    session.execute(update_query, tuple(update_params))

# Delete a collection from the database
async def delete_collection_from_db(collection_id: uuid.UUID):
    query = "DELETE FROM collection WHERE id=%s"
    session.execute(query, (collection_id,))

# Fetch a collection by its ID
async def fetch_collection_by_name(organization_id: uuid.UUID, project_id: uuid.UUID, collection_name: str):
    query = "SELECT * FROM collection WHERE collection_name=%s AND project_id=%s AND organization_id=%s ALLOW FILTERING"
    return session.execute(query, (collection_name, project_id, organization_id)).one()

async def fetch_collection_schema(organization_name: str, project_name: str, collection_name: str):
    # Fetch the schema of the collection from the system tables
    keyspace_name = organization_name
    table_name = f'"{project_name}_{collection_name}"'
    schema_query = """
    SELECT column_name, type 
    FROM system_schema.columns 
    WHERE keyspace_name=%s AND table_name=%s
    """
    rows = session.execute(schema_query, (keyspace_name, table_name))
    return {row.column_name: row.type for row in rows}

# Fetch all collections for a project
def fetch_all_collections(organization_id: uuid.UUID, project_id: uuid.UUID):
    query = "SELECT * FROM collection WHERE organization_id=%s AND project_id=%s ALLOW FILTERING"
    rows = session.execute(query, (organization_id, project_id))
    return rows.all()

# Create a Cassandra table for the collection
async def create_cassandra_table(organization_name: str, project_name: str, data: CollectionCreateRequest):
    keyspace_name = organization_name
    table_name = f'"{project_name}_{data.name}"'
    # If the schema contains lists, ensure they are consistent
    if isinstance(data.collection_schema, list):
        if not is_list_of_same_schema(data.schema):
            raise HTTPException(
                status_code=400, 
                detail="The provided schema contains lists with inconsistent types."
            )
    flattened_schema = flatten_object(data.collection_schema)
    if not any(key.lower() == 'timestamp' for key in flattened_schema):
        flattened_schema['timestamp'] = 'TIMESTAMP'
    flattened_schema['day'] = 'DATE'
    # Check for key column, if not present add it
    has_key = any(key.lower() == 'key' for key in flattened_schema)
    if not has_key:
        flattened_schema['key'] = 'TEXT'
    columns = ", ".join([f'"{col_name}" {col_type}' for col_name, col_type in flattened_schema.items()])
    primary_key = "((day, key), timestamp)"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {keyspace_name}.{table_name} (
        {columns},
        PRIMARY KEY {primary_key}
    ) WITH CLUSTERING ORDER BY (timestamp DESC)
    """
    try:
        session.execute(create_table_query)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create the Cassandra table: {str(e)}"
        )
# Delete a Cassandra table
async def delete_cassandra_table(organization_id: uuid.UUID, project_id: uuid.UUID, collection_id: uuid.UUID):
    keyspace_name = get_organization_by_id(organization_id).organization_name
    table_name = f"{get_project_by_id(project_id, organization_id).project_name}_{get_collection_by_id(collection_id, project_id, organization_id).collection_name}"
    query = f"DROP TABLE IF EXISTS {keyspace_name}.{table_name}"
    session.execute(query)

# Create a Kafka topic
async def create_kafka_topic(organization_name: str, project_name: str, collection_name: str):
    kafka_topic_name = f"{organization_name}.{project_name}.{collection_name}"
    kafka_admin_client = get_kafka_admin_client()
    print(kafka_admin_client)
    new_topic = NewTopic(kafka_topic_name, num_partitions=1, replication_factor=1)
    fs = kafka_admin_client.create_topics([new_topic])
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} created successfully")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to create Kafka topic {topic}: {str(e)}")
# Delete a Kafka topic
async def delete_kafka_topic(organization_name: str, project_name: str, collection_name: str):
    kafka_topic_name = f"{organization_name}.{project_name}.{collection_name}"
    kafka_admin_client = get_kafka_admin_client()
    try:
        kafka_admin_client.delete_topics([kafka_topic_name])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete Kafka topic: {str(e)}")
def check_collection_exists(collection_id: uuid.UUID, project_id: uuid.UUID, organization_id: uuid.UUID):
    collection = get_collection_by_id(collection_id, project_id, organization_id)
    if not collection:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Collection not found.")
    return collection

async def insert_data_into_table(
    organization_name: str, 
    project_name: str, 
    collection_name: str, 
    records: List[dict]
):
    keyspace_name = organization_name
    table_name = f'"{project_name}_{collection_name}"'
    
    # Loop through each record and insert it into the table
    for record in records:
        flattened_record = flatten_object(record, return_value=True)
        
        # Ensure 'timestamp' is in the record (set it to the current time if not present)
        if not any(key.lower() == 'timestamp' for key in flattened_record):
            flattened_record['timestamp'] = datetime.utcnow()
        flattened_record['day'] = flattened_record['timestamp'].date()
        # Extract the column names and values for insertion
        column_names = ", ".join([f'"{key}"' for key in flattened_record.keys()])
        placeholders = ", ".join(["%s" for _ in flattened_record.values()])
        values = list(flattened_record.values())
        # Insert the record into the table
        insert_query = f"""
        INSERT INTO {keyspace_name}.{table_name} ({column_names})
        VALUES ({placeholders})
        """
        try:
            session.execute(insert_query, values)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to insert data into Cassandra: {str(e)}"
            )