from kafka import KafkaConsumer, KafkaProducer
import random
import uuid
import json
import time

class ProduceTask:
    def __init__(self):
        self.bootstrap_servers = ['localhost:9092']
        self.topic_name = 'data-hub'
        self.producer = self.connect_kafka_producer()
        self.task_list = ['low','high']
        self.delay = 10

    def publish_message(self, key, value):
        try:
            key = bytes(key,encoding='utf-8')
            value = json.dumps(value).encode('utf-8')
            self.producer.send(self.topic_name, key=key, value=value)
            self.producer.flush()
            print('Message published successfully.')
        except Exception as e:
            print('Exception in publishing message')
            print(e)

    def connect_kafka_producer(self):
        _producer = None
        try:
            _producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                api_version=(0, 10)
            )
        except Exception as e:
            print('Exception while connecting Kafka')
            print(e)
        finally:
            print(_producer)
            return _producer

    def start(self):
        while True:
            value = {
                'name': random.choice(self.task_list),
                'max_retry_attemp': random.randint(1,3),
                'retry_backoff_setting': random.randint(1,10),
            }
            key = str(uuid.uuid4())
            self.publish_message(key,value)
            print ("sent message with key : {}, value : {}".format(key,value))
            time.sleep(self.delay)



if __name__ == '__main__':
    produce_task = ProduceTask()
    produce_task.start()
