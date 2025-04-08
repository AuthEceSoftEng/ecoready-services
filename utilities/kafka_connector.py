from config import settings
from confluent_kafka.admin import AdminClient
from confluent_kafka import Producer, Consumer

def get_kafka_admin_client():
    return AdminClient({'bootstrap.servers': settings.kafka_brokers})

def get_kafka_producer():
    return Producer({
        'bootstrap.servers': settings.kafka_brokers,
        'queue.buffering.max.messages': 200000
    })