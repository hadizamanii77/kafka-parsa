import configparser
import json

from kafka import KafkaConsumer, KafkaProducer

config = configparser.ConfigParser()
config.read("config.ini")

bootstrap_servers = config['kafka']['servers']


def publish_message(producer,topic, key, value):
    try:
        key = bytes(key, encoding='utf-8')
        value = json.dumps(value).encode('utf-8')
        producer.send(topic, key=key, value=value)
        producer.flush()
        print('Message published successfully.')
    except Exception as e:
        print('Exception in publishing message')
        print(e)


def get_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            api_version=(0, 10)
        )
    except Exception as e:
        print('Exception while connecting Kafka')
        print(e)
    finally:
        return _producer


def get_kafka_consumer(group_id,topic):
    _consumer = None
    try:
        _consumer = KafkaConsumer(
            auto_offset_reset='earliest',
            bootstrap_servers=bootstrap_servers,
            api_version=(0, 10),
            group_id=group_id
        )
        _consumer.subscribe([topic])

    except Exception as e:
        print('Exception while connecting Kafka')
        print(e)
    finally:
        return _consumer
