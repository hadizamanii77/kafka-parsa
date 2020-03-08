from datahub.tasks.low_possible_error_task import LowPossibleErrorTask
from datahub.tasks.high_possible_error_task import HighPossibleErrorTask
from global_utils.exception import RandomError
from kafka import KafkaConsumer, KafkaProducer
import json
import time
import uuid

class DataHubWorker:
    def __init__(self):
        self.source_topic_name = 'data-hub'
        self.dest_topic_name = 'worker'
        self.bootstrap_servers = ['localhost:9092']
        self.group_id = 'data-hub'
        self.consumer = self.get_kafka_consumer()
        self.consumer.subscribe([self.source_topic_name])
        self.producer = self.connect_kafka_producer()

    def publish_message(self, key, value):
        try:
            key = bytes(key,encoding='utf-8')
            value = json.dumps(value).encode('utf-8')
            self.producer.send(self.dest_topic_name, key=key, value=value)
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

    def get_kafka_consumer(self):
        _consumer = None
        try:
            _consumer = KafkaConsumer(
                auto_offset_reset='earliest',
                bootstrap_servers=self.bootstrap_servers,
                api_version=(0, 10),
                group_id=self.group_id
            )

        except Exception as e:
            print('Exception while connecting Kafka')
            print(e)
        finally:
            return _consumer

    def start_fetching_data_from_queue(self):

        for msg in self.consumer:
            key = msg.key
            value = json.loads(msg.value.decode('utf-8'))
            print("recieved message with key {} and value {}".format(key, value))
            self.do_operation(value)

    def do_operation(self, data):

        name = data['name']
        max_retry_attemp = data['max_retry_attemp']
        retry_backoff_setting = data['retry_backoff_setting']
        task = None
        if name == 'low':
            task = LowPossibleErrorTask()
        elif name == 'high':
            task = HighPossibleErrorTask()

        for i in range(0, max_retry_attemp):
            try:
                value = task.do()
                key = str(uuid.uuid4())
                self.publish_message(key,value)
                print("result : {}",value)
                return
            except RandomError as e:
                print("happened random error")
                time.sleep(retry_backoff_setting)
            except Exception as e:
                print(e)
                time.sleep(retry_backoff_setting)
        print("not finish")
        return


if __name__ == '__main__':
    manage_tasks = DataHubWorker()
    manage_tasks.start_fetching_data_from_queue()
