from fastapi import APIRouter, Depends, HTTPException, FastAPI
from dependencies import get_current_user
from kafka_service import get_consumer, get_producer, delivery_report
from kafka.errors import TopicAuthorizationFailedError, UnknownTopicOrPartitionError
from kafka.admin import KafkaAdminClient,  ConfigResource
from typing import Dict, Any
import json
from confluent_kafka import KafkaError
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic
from confluent_kafka.admin import AdminClient, AclBinding, AclOperation, AclPermissionType, ResourceType, ResourcePatternType

TAG = "Topics"
app = FastAPI()
router = APIRouter()

async def get_topic_config(topic_name: str, user: dict = Depends(get_current_user)):
    config_resource = ConfigResource(ConfigResource.Type.TOPIC, topic_name)
    bootstrap_servers = 'localhost:59092'  # Consider moving this to config or environment variables
    kafka_conf = {
    'bootstrap.servers': 'localhost:59092',
     'security.protocol':'SASL_PLAINTEXT', 'sasl.mechanisms':'PLAIN', 'sasl.username':user["username"], 'sasl.password':user["password"] 
    # Include any additional required configurations here, such as security settings
    }
    try:
        admin_client = AdminClient(kafka_conf)
        configs = admin_client.describe_configs([config_resource])
        for _, f in configs.items():
            config = f.result()  # Wait for the future and get the result
            retention = config.get('retention.ms', None)
            if retention is not None:
                return {"message": f"Retention time set to {retention.value} ms for topic {topic_name}."}
            else:
                raise HTTPException(status_code=500, detail=f"Failed to confirm retention for topic {topic_name}.")
    except Exception as e:
        if e.args[0].code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
            raise HTTPException(status_code=403, detail=f"Authorization failed. Cannot get {topic_name} topic's retention settings.")
        else:
            raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get retention for topic {topic_name}: {str(e)}")

@router.post("/create-topic/", tags=[TAG])
async def create_kafka_topic(topic_name: str, partitions: int, user: dict = Depends(get_current_user)):
    bootstrap_servers = 'localhost:59092'  # Consider moving this to config or environment variables
    kafka_conf = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': user["username"],
        'sasl.password': user["password"]
    }

    admin_client = AdminClient(kafka_conf)
    try:
        new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=2)
        fs = admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None if successful
                return {"message": f"Topic '{topic}' created successfully."}
            except Exception as e:
                if e.args[0].code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
                    raise HTTPException(status_code=403, detail=f"Authorization failed. Cannot create topic {topic_name}.")
                if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    raise HTTPException(status_code=403, detail=f"Topic {topic_name} already exists.")
                else:
                    raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
@router.delete("/delete-topic/", tags=[TAG])
async def delete_kafka_topic(topic_name: str, user: dict = Depends(get_current_user)):
    kafka_conf = {
    'bootstrap.servers': 'localhost:59092',
     'security.protocol':'SASL_PLAINTEXT', 'sasl.mechanisms':'PLAIN', 'sasl.username':user["username"], 'sasl.password':user["password"]
    # Include any additional required configurations here, such as security settings
    }
    admin_client = AdminClient(kafka_conf)
    try:
        fs = admin_client.delete_topics([topic_name])
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                return {"message": f"Topic '{topic_name}' deleted successfully."}
            except Exception as e:
                print(f"Failed to delete topic '{topic_name}': {e}")
                raise HTTPException(status_code=500, detail=f"Failed to delete topic '{topic}': {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.post("/retention", tags=[TAG])
async def set_topic_retention(topic_name: str, retention_ms: int,  user: dict = Depends(get_current_user)):
    bootstrap_servers = 'localhost:59092'  # Consider moving this to config or environment variables
    kafka_conf = {
    'bootstrap.servers': 'localhost:59092',
     'security.protocol':'SASL_PLAINTEXT', 'sasl.mechanisms':'PLAIN', 'sasl.username':user["username"], 'sasl.password':user["password"] 
    # Include any additional required configurations here, such as security settings
    }
    admin_client = AdminClient(kafka_conf)
    config_resource = ConfigResource(ConfigResource.Type.TOPIC, topic_name, {'retention.ms': str(retention_ms)})

    # Attempt to alter the topic configuration
    try:
        fs = admin_client.alter_configs([config_resource])
        for _, f in fs.items():
            f.result()  # Wait for the future to complete, raises an exception on failure
        return {"message": f"Retention time set to {retention_ms} ms for topic {topic_name}."}
    except Exception as e:
        if e.args[0].code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
            raise HTTPException(status_code=403, detail=f"Authorization failed. Cannot modify {topic_name} topic's retention settings.")
        else:
            raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/get_retention/", tags=[TAG])
async def get_topic_retention(topic_name: str, user: dict = Depends(get_current_user)):
    retention_ms = await get_topic_config(topic_name, user)
    return {"retention.ms": retention_ms}

@router.post("/write/{topic_name}", tags=[TAG])
async def write_to_topic(topic_name: str, message: Dict[str, Any], user: dict = Depends(get_current_user)):
    producer = get_producer(user["username"], user["password"])
    message_str = json.dumps(message)
    print(user["username"], user["password"])
    try:
        producer.produce(topic_name, key=user["username"], value=message_str, callback=delivery_report)
        producer.flush()  # Ensure the message is sent
    except KafkaError as e:
        # Handle specific Kafka errors here
        if e.code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
            raise HTTPException(status_code=403, detail="Unauthorized to write to this topic")
        elif e.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            raise HTTPException(status_code=404, detail="Topic not found")
        else:
            raise HTTPException(status_code=500, detail="Kafka error ")
    return {"status": "message sent"}
@router.get("/read/{topic_name}", tags=[TAG])
async def read_from_topic(topic_name: str, user: dict = Depends(get_current_user)):
    consumer = get_consumer(user["username"], user["password"])

    consumer.subscribe([topic_name])
    messages = []
    try:
        for _ in range(100):  # Try to fetch up to 100 messages
            msg = consumer.poll(1.0)  # Poll for 1 second
            if msg is not None:
                if msg.error() and (msg.error().code() == KafkaError.TOPIC_AUTHORIZATION_FAILED or msg.error().code() == KafkaError.GROUP_AUTHORIZATION_FAILED):
                    raise HTTPException(status_code=403, detail="Unauthorized to read from this topic")
                elif msg.error():
                    raise HTTPException(status_code=500, detail="Internal server error")
                else:
                    message_json = json.loads(msg.value().decode('utf-8'))
                    messages.append(message_json)
                    if len(messages) >= 100:
                        break
    except KafkaError as e:
        # Print the error message
        print(f"Error reading message: {e}")
        # If you want to raise an HTTPException based on the error, you can do so here
        if e.code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
            raise HTTPException(status_code=403, detail="Unauthorized to read from this topic")
        elif e.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            raise HTTPException(status_code=404, detail="Topic not found")
        else:
            raise HTTPException(status_code=500, detail="Internal server error")
    
    return {"messages": messages}

