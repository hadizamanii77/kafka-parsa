from datahub.tasks.task import Task
from global_utils.exception import RandomError
from global_utils.kafka import publish_message,get_kafka_producer
import uuid

import random


class HighPossibleErrorTask(Task):
    def __init__(self):
        self.begin = 0
        self.end = 1
        self.dest_topic_name = 'worker'
        self.task_producer = get_kafka_producer()
        pass

    def is_error(self,rand_num):
        return rand_num == self.end

    def do(self):
        rand_num = random.randint(self.begin, self.end)
        if self.is_error(rand_num):
            raise RandomError
        else:
            value = {
                'data': random.randint(0, 10)
            }
            key = str(uuid.uuid4())
            publish_message(self.task_producer, self.dest_topic_name, key, value)
            return value
