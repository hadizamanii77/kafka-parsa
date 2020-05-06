import configparser
import json

from kafka import KafkaConsumer, KafkaProducer

config = configparser.ConfigParser()
config.read("config.ini")

bootstrap_servers = config['kafka']['servers'].replace(' ', '').strip('][').split(',')
topics = config['kafka']['topics'].replace(' ', '').strip('][').split(',')


class KafkaManager:
    _kafka_manager = None

    @staticmethod
    def get_instance():
        if KafkaManager._kafka_manager is None:
            KafkaManager()
        return KafkaManager._kafka_manager

    def __init__(self):
        if KafkaManager._kafka_manager is not None:
            raise Exception("Kafka Manager class is a singleton!")
        else:
            self._consumer = KafkaConsumer(
                auto_offset_reset='latest',
                bootstrap_servers=bootstrap_servers,
                api_version=(0, 10),
            )
            self._producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                api_version=(0, 10)
            )
            self._subscribe_on_topics()
            KafkaManager._kafka_manager = self

    def _subscribe_on_topics(self):
        try:
            self._consumer.subscribe(
                topics=topics
            )
        except Exception as e:
            print(e)

    @staticmethod
    def publish_message(self, producer,topic, key, value):
        try:
            key = bytes(key, encoding='utf-8')
            value = json.dumps(value).encode('utf-8')
            producer.send(topic, key=key, value=value)
            producer.flush()
            print('Message published successfully.')
        except Exception as e:
            print('Exception in publishing message')
            print(e)

    def get_kafka_producer(self,):
        return self._producer

    def get_kafka_consumer(self):
        return self._consumer


KafkaManager.get_instance()