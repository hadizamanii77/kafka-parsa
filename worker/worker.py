from kafka import KafkaConsumer, KafkaProducer
import json
import time
import uuid


class Worker:
    def __init__(self):
        self.source_topic_name = 'worker'
        self.bootstrap_servers = ['localhost:9092']
        self.group_id = 'worker-group'
        self.consumer = self.get_kafka_consumer()
        self.consumer.subscribe([self.source_topic_name])

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

        data = data['data']
        print(data * 5)


if __name__ == '__main__':
    manage_tasks = Worker()
    manage_tasks.start_fetching_data_from_queue()
