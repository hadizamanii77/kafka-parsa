from global_utils.kafka import get_kafka_producer,publish_message,get_kafka_consumer

import random
import uuid
import json
import time

class ProduceTask:
    def __init__(self):
        self.topic_name = 'data-hub'
        self.producer = get_kafka_producer()
        self.task_list = ['low','high']
        self.delay = 10

    def start(self):
        while True:
            value = {
                'name': random.choice(self.task_list),
                'max_retry_attemp': random.randint(1,3),
                'retry_backoff_setting': random.randint(1,10),
            }
            key = str(uuid.uuid4())
            publish_message(self.producer,self.topic_name,key,value)
            print ("sent message with key : {}, value : {}".format(key,value))
            time.sleep(self.delay)



if __name__ == '__main__':
    produce_task = ProduceTask()
    produce_task.start()
