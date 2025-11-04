from confluent_kafka import Producer
import json
from config import KAFKA_CONFIG

def get_kafka_producer():
    """Get a configured Kafka producer instance"""
    return Producer({
        'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers'][0],
        'client.id': 'crypto-producer'
    })

def send_metric(producer, metric_data):
    """Send metric data to Kafka"""
    from config import TOPICS
    producer.produce(TOPICS['metrics'], value=json.dumps(metric_data))

def send_alert(producer, alert_data):
    """Send alert data to Kafka"""
    from config import TOPICS
    producer.produce(TOPICS['alerts'], value=json.dumps(alert_data))