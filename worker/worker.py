from global_utils.kafka import get_kafka_consumer

import json
import time
import uuid


class Worker:
    def __init__(self):
        self.source_topic_name = 'worker'
        self.group_id = 'worker-group'
        self.consumer = get_kafka_consumer(group_id=self.group_id,topic=self.source_topic_name)

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
