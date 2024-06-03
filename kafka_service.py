from confluent_kafka import Producer, Consumer, KafkaError
from kafka import KafkaConsumer
from fastapi import HTTPException
consumers = {}
producers = {}

def delivery_report(err, msg):
    if err is not None:
        if err.code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
            raise HTTPException(status_code=403, detail="Unauthorized to write to this topic")
        elif err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            raise HTTPException(status_code=404, detail="Topic not found")
        else:
            raise HTTPException(status_code=500, detail="Kafka error")
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def get_producer(username: str, password: str):
    if username not in producers:
        conf = {
            'bootstrap.servers': 'localhost:59092',
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': username,
            'sasl.password': password
        }
        producers[username] = Producer(conf)
    return producers[username]

def get_consumer(username: str, password: str):
    if username not in consumers:
        conf = {
            'bootstrap.servers': 'localhost:59092',
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': username,
            'sasl.password': password,
            'group.id' : username,
            'auto.offset.reset': 'latest'
        }
        consumers[username] = Consumer(conf)
    return consumers[username]
