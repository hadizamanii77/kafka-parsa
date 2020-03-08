from datahub.tasks.low_possible_error_task import LowPossibleErrorTask
from datahub.tasks.high_possible_error_task import HighPossibleErrorTask
from global_utils.exception import RandomError
from global_utils.kafka import get_kafka_consumer, publish_message, get_kafka_producer

import json
import time
import uuid


class DataHubWorker:
    def __init__(self):
        self.source_topic_name = 'data-hub'
        self.group_id = 'data-hub'
        self.consumer = get_kafka_consumer(group_id=self.group_id, topic=self.source_topic_name)
        self.task_producer = get_kafka_producer()

    def start_fetching_data_from_queue(self):

        for msg in self.consumer:
            key = msg.key
            value = json.loads(msg.value.decode('utf-8'))
            print("recieved message with key {} and value {}".format(key, value))
            self.do_operation(value)

    def do_operation(self, data):
        print(data)
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
                print("result : {}", value)
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
