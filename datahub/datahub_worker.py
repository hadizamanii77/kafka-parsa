from datahub.tasks.tp_task import TpTask
from global_utils.exception import RandomError
from global_utils.kafka_manager import KafkaManager

import json
import time


class DataHubWorker:

    def __init__(self):
        self.kafka_manager = KafkaManager.get_instance()

    def start_fetching_data_from_queue(self):
        consumer = self.kafka_manager.get_kafka_consumer()

        for msg in consumer:
            self._do_operation(msg)

    def _do_operation(self, msg):
        topic = msg.topic
        data = json.loads(msg.value.decode("utf-8"))
        task = None
        if topic == 'TPTopic':
            task = TpTask()
        task.do(data=data)


if __name__ == '__main__':
    manage_tasks = DataHubWorker()
    manage_tasks.start_fetching_data_from_queue()
