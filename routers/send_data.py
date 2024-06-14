from fastapi import FastAPI, HTTPException, Depends, Query, APIRouter, UploadFile, File
from pydantic import BaseModel
from typing import Dict, Any, List, Union
from dependencies import get_current_user
from confluent_kafka import Producer
import time
import json
import csv
from io import StringIO
router = APIRouter()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = '155.207.19.243:59092'
SECURITY_PROTOCOL = 'SASL_PLAINTEXT'
SASL_MECHANISMS = 'PLAIN'
flush_threshold = 30000

def get_kafka_producer(user: dict):
    return Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'security.protocol': SECURITY_PROTOCOL,
        'sasl.mechanisms': SASL_MECHANISMS,
        'sasl.username': user["username"],
        'sasl.password': user["password"],
        'queue.buffering.max.messages': 200000
    })

@router.post("/projects/{project_name}/send_data/{topic_name}")
async def send_data(project_name: str, topic_name: str,  data: Union[Dict[str, Any], List[Dict[str, Any]]], key: str = None, user: dict = Depends(get_current_user)):
    kafka_producer = get_kafka_producer(user)
    full_topic_name = f"{project_name}.{topic_name}"

    if isinstance(data, Dict):
        messages = [data]
    else:
        messages = data
    for i, message_data in enumerate(messages):
        for k in ['timestamp', 'Timestamp', 'TIMESTAMP']:
            if k in message_data:
                message_data['timestamp'] = message_data.pop(k)
                break
        if 'timestamp' not in message_data:
            message_data['timestamp'] = datetime.utcnow().isoformat() + 'Z'
        message_key = message_data.pop(key, user["username"])
        kafka_producer.produce(full_topic_name, key=message_key, value=json.dumps(message_data))
        if (i + 1) % flush_threshold == 0:
            kafka_producer.flush()

    kafka_producer.flush()
    
    return {"message": f"Data sent to topic '{full_topic_name}' successfully."}

@router.post("/projects/{project_name}/send_csv/{topic_name}")
async def send_csv(project_name: str, topic_name: str, file: UploadFile = File(...), user: dict = Depends(get_current_user)):
    kafka_producer = get_kafka_producer(user)
    full_topic_name = f"{project_name}.{topic_name}"

    try:
        content = await file.read()
        csv_content = content.decode('utf-8')
        csv_reader = csv.DictReader(StringIO(csv_content))

        messages = []
        for row in csv_reader:
            row["prod_timestamp"] = int(time.time() * 1000)
            messages.append(row)

        for message_data in messages:
            kafka_producer.produce(full_topic_name, key=user['username'], value=json.dumps(message_data))
        
        kafka_producer.flush()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process CSV file: {str(e)}")

    return {"message": f"CSV data sent to topic '{full_topic_name}' successfully."}
