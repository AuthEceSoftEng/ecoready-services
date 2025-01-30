from fastapi import APIRouter, Depends, HTTPException, Query, Body
from dependencies import check_organization_exists, check_project_exists, verify_api_key_access, verify_endpoint_access, verify_write_access
from utilities.collection_utils import check_collection_exists
from typing import List, Dict, Union
import uuid
from config import settings
from confluent_kafka import Producer
from utilities.organization_utils import get_organization_by_id
from utilities.project_utils import get_project_by_id
from utilities.collection_utils import get_collection_by_id
from datetime import datetime
import json 
flush_threshold=1
router = APIRouter(dependencies=[Depends(check_organization_exists), Depends(check_project_exists)])
TAG = "Send Data"

#send data to collection
@router.post("/organizations/{organization_id}/projects/{project_id}/collections/{collection_id}/send_data", tags=[TAG], dependencies=[Depends(check_collection_exists), Depends(verify_endpoint_access)])
async def send_data_to_collection(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    collection_id: uuid.UUID,
    data: Union[Dict, List[Dict]] = Body(...)
):
    kafka_producer = Producer({
        'bootstrap.servers': settings.kafka_brokers,
        'queue.buffering.max.messages': 200000
    })
    organization_name = get_organization_by_id(organization_id).organization_name
    project_name = get_project_by_id(project_id,organization_id).project_name
    collection_name = get_collection_by_id(collection_id, project_id, organization_id).collection_name
    topic_name = f"{organization_name}.{project_name}.{collection_name}"


    # Step 4: Validate data against the collection's schema with type checking
    if isinstance(data, Dict):
        messages = [data]
    else:
        messages = data

    for message in messages:
        # Ensure 'key' is present in the message
        if 'key' not in message:
            message['key'] = "key"
        # Ensure 'timestamp' is present in the message, if not, add it with the current UTC time
        if 'timestamp' not in message:
            message['timestamp'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        # Convert the 'timestamp' string to a datetime object
        timestamp_dt = datetime.fromisoformat(message['timestamp'].replace('Z', '+00:00'))

        # Extract 'day' from the 'timestamp'
        message['day'] = timestamp_dt.strftime('%Y-%m-%d')

    for i, message_data in enumerate(messages):
        # Extract the key from the message_data
        message_key = message_data.pop('key')
    
        # Send the rest of the message data to Kafka, excluding the key
        kafka_producer.produce(topic_name, key=message_key, value=json.dumps(message_data))
        if (i + 1) % flush_threshold == 0:
            kafka_producer.flush()

    kafka_producer.flush()

    return {"message": f"Data sent to collection '{collection_name}' successfully."}