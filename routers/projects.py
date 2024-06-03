from fastapi import APIRouter, FastAPI, HTTPException, Depends, Query
from dependencies import get_current_user
from cassandra.cluster import Cluster
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, AclBinding, AclOperation, AclPermissionType, AclBindingFilter, ResourceType, ResourcePatternType, NewTopic
from typing import Dict, Any
import json
import threading
import time
app = FastAPI()
router = APIRouter()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:59092'
SECURITY_PROTOCOL = 'SASL_PLAINTEXT'
SASL_MECHANISMS = 'PLAIN'

# Cassandra configuration
CASSANDRA_HOSTS = ['155.207.19.242','155.207.19.243']
CASSANDRA_PORT = 9042


def get_kafka_consumer(user: dict, group_id: str):
    return Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'security.protocol': SECURITY_PROTOCOL,
        'sasl.mechanisms': SASL_MECHANISMS,
        'sasl.username': user["username"],
        'sasl.password': user["password"],
        'group.id': group_id,
        'auto.offset.reset': 'latest'
    })

def get_kafka_admin_client(user: dict):
    return AdminClient({
        'bootstrap.servers': 'localhost:19092',
        'security.protocol': SECURITY_PROTOCOL,
        'sasl.mechanisms': SASL_MECHANISMS,
        'sasl.username': user["username"],
        'sasl.password': user["password"]
    })

def get_kafka_admin_client_topic(user: dict):
    return AdminClient({
        'bootstrap.servers': 'localhost:59092',
        'security.protocol': SECURITY_PROTOCOL,
        'sasl.mechanisms': SASL_MECHANISMS,
        'sasl.username': user["username"],
        'sasl.password': user["password"]
    })

def get_cassandra_session():
    cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT)
    return cluster.connect()

def consume_and_write_to_cassandra(consumer: Consumer, session, table_name: str):
    try:
        consumer.subscribe([table_name])
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            else:
                # Parse message value
                message_data = json.loads(msg.value().decode('utf-8'))
                
                # Ensure 'prod_timestamp' is present
                if 'prod_timestamp' not in message_data:
                    print(f"Missing 'prod_timestamp' in message: {message_data}")
                    continue
                
                # Calculate 'day' from 'prod_timestamp'
                day = time.strftime("%Y-%m-%d", time.gmtime(message_data['prod_timestamp'] / 1000))  # Divide by 1000 to convert to seconds
                message_data['day'] = day
                
                # Use the key of the Kafka record as 'id'
                message_data['id'] = msg.key().decode('utf-8')
                
                # Construct insert query
                columns = ', '.join(message_data.keys())
                placeholders = ', '.join(['%s'] * len(message_data))
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                session.execute(query, list(message_data.values()))
    except Exception as e:
        print(f"Error in consumer: {e}")
        # Restart the consumer on failure
        consume_and_write_to_cassandra(consumer, session, table_name)

@router.post("/create_project/{project_name}")
async def create_project(project_name: str, username: str, user: dict = Depends(get_current_user)):
    # Step 1: Create Cassandra keyspace
    cassandra_session = get_cassandra_session()
    try:
        cassandra_session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {project_name}
            WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '2' }}
        """)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create Cassandra keyspace: {str(e)}")

    # Step 2: Assign Kafka ACLs
    kafka_admin_client = get_kafka_admin_client(user)
    topic_pattern = f"{project_name}."
    acls = [
        AclBinding(
            restype=ResourceType.TOPIC,
            name=topic_pattern,
            resource_pattern_type=ResourcePatternType.PREFIXED,
            principal=f"User:{username}",
            host="*",
            operation=AclOperation.CREATE,
            permission_type=AclPermissionType.ALLOW
        ),
        AclBinding(
            restype=ResourceType.TOPIC,
            name=topic_pattern,
            resource_pattern_type=ResourcePatternType.PREFIXED,
            principal=f"User:{username}",
            host="*",
            operation=AclOperation.ALTER,
            permission_type=AclPermissionType.ALLOW
        ),
        AclBinding(
            restype=ResourceType.TOPIC,
            name=topic_pattern,
            resource_pattern_type=ResourcePatternType.PREFIXED,
            principal=f"User:{username}",
            host="*",
            operation=AclOperation.DELETE,
            permission_type=AclPermissionType.ALLOW
        ),
        AclBinding(
            restype=ResourceType.TOPIC,
            name=topic_pattern,
            resource_pattern_type=ResourcePatternType.PREFIXED,
            principal=f"User:{username}",
            host="*",
            operation=AclOperation.DESCRIBE,
            permission_type=AclPermissionType.ALLOW
        ),
        AclBinding(
            restype=ResourceType.TOPIC,
            name=topic_pattern,
            resource_pattern_type=ResourcePatternType.PREFIXED,
            principal=f"User:{username}",
            host="*",
            operation=AclOperation.ALTER_CONFIGS,
            permission_type=AclPermissionType.ALLOW
        ),
        AclBinding(
            restype=ResourceType.TOPIC,
            name=topic_pattern,
            resource_pattern_type=ResourcePatternType.PREFIXED,
            principal=f"User:{username}",
            host="*",
            operation=AclOperation.DESCRIBE_CONFIGS,
            permission_type=AclPermissionType.ALLOW
        ),
        AclBinding(
            restype=ResourceType.TOPIC,
            name=topic_pattern,
            resource_pattern_type=ResourcePatternType.PREFIXED,
            principal=f"User:{username}",
            host="*",
            operation=AclOperation.READ,
            permission_type=AclPermissionType.ALLOW
        ),
        AclBinding(
            restype=ResourceType.TOPIC,
            name=topic_pattern,
            resource_pattern_type=ResourcePatternType.PREFIXED,
            principal=f"User:{username}",
            host="*",
            operation=AclOperation.WRITE,
            permission_type=AclPermissionType.ALLOW
        ),
        AclBinding(
            restype=ResourceType.GROUP,
            name=topic_pattern,
            resource_pattern_type=ResourcePatternType.PREFIXED,
            principal=f"User:{username}",
            host="*",
            operation=AclOperation.READ,
            permission_type=AclPermissionType.ALLOW
        )
    ]

    try:
        futures = kafka_admin_client.create_acls(acls)
        for acl, future in futures.items():
            future.result()  # The result is None if successful
        return {"message": f"Project '{project_name}' created successfully with necessary Kafka ACLs and Cassandra keyspace."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to set Kafka ACLs: {str(e)}")


@router.post("/add_user/{username}")
async def add_user(username: str, password: str, user: dict = Depends(get_current_user)):
    try:
        # Path to your JAAS configuration file
        jaas_config_path = "../kafka.jaas.conf"
        # Read the current JAAS configuration
        with open(jaas_config_path, 'r+') as file:
            jaas_config = file.read()

        # Add the new user
        new_user_entry = f'user_{username}="{password}";\n'
        jaas_config = jaas_config.replace(';\n};', f'\n    {new_user_entry}}};')

        # Write the updated configuration back to the file
        with open(jaas_config_path, 'w') as file:
            file.write(jaas_config)
        return {"message": f"User '{username}' added successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/grant_read_access/{project_name}")
async def grant_read_access(project_name: str, target_username: str, topic: str = None, user: dict = Depends(get_current_user)):
    # Step 1: Verify that the current user has the necessary rights to grant permissions
    kafka_admin_client = get_kafka_admin_client(user)
    current_user = user["username"]
    if topic is not None:
        resname=f"{project_name}.{topic}"
    else:
        resname=f"{project_name}"
    # Ensure the current user has ALTER permission on the project topics
    acl_binding = AclBindingFilter(
        restype=ResourceType.TOPIC,
        name=resname,
        resource_pattern_type=ResourcePatternType.PREFIXED,
        principal=f"User:{current_user}",
        host="*",
        operation=AclOperation.ALTER,
        permission_type=AclPermissionType.ALLOW
    )
    try:
        futures = kafka_admin_client.describe_acls(acl_binding)
        if futures.result() is None:
            raise HTTPException(status_code=403, detail="You do not have permission to grant access.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to verify permissions: {str(e)}")

    # Step 2: Grant READ access to the target user
    acl_binding = AclBinding(
        restype=ResourceType.TOPIC,
        name=resname,
        resource_pattern_type=ResourcePatternType.PREFIXED,
        principal=f"User:{target_username}",
        host="*",
        operation=AclOperation.READ,
        permission_type=AclPermissionType.ALLOW
    )

    try:
        futures = kafka_admin_client.create_acls([acl_binding])
        for acl, future in futures.items():
            future.result()  # The result is None if successful
        return {"message": f"User '{target_username}' granted READ access on project '{project_name}' and the asked topics."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to grant READ access: {str(e)}")

@router.post("/projects/{project_name}/add_topic")
async def add_topic(project_name: str, topic_name: str , message: Dict[str, Any], user: dict = Depends(get_current_user)):
    # Step 1: Create Cassandra keyspace
    cassandra_session = get_cassandra_session()
    kafka_admin_client = get_kafka_admin_client_topic(user)
    full_topic_name = f"{project_name}.{topic_name}"
    new_topic = NewTopic(full_topic_name, num_partitions=4, replication_factor=2)
    try:
        fs = kafka_admin_client.create_topics([new_topic])
        # Wait for each operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print(f"Topic {topic} created successfully")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")
                raise HTTPException(status_code=500, detail=f"Failed to create Kafka topic {topic}: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create Kafka topic: {str(e)}")
    columns = []
    for key, value in message.items():
        if isinstance(value, int):
            columns.append(f"{key} int")
        elif isinstance(value, float):
            columns.append(f"{key} float")
        elif isinstance(value, str):
            columns.append(f"{key} text")
        elif isinstance(value, bool):
            columns.append(f"{key} boolean")
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported data type for key: {key}")

    columns_def = ", ".join(columns)
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {project_name}.{topic_name} (
            id TEXT,
            day DATE,
            prod_timestamp TIMESTAMP,
            {columns_def},
            PRIMARY KEY ((id, day), prod_timestamp)
        ) WITH CLUSTERING ORDER BY (prod_timestamp DESC)
    """
    try:
        cassandra_session.execute(create_table_query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create Cassandra table: {str(e)}")
    group_id = f"{project_name}.{topic_name}_group"
    consumer = get_kafka_consumer(user, group_id=group_id)
    thread = threading.Thread(target=consume_and_write_to_cassandra, args=(consumer, cassandra_session, f"{project_name}.{topic_name}"))
    thread.daemon = True
    thread.start()


    return {"message": f"Topic '{full_topic_name}' and table '{project_name}.{topic_name}' created successfully."}

