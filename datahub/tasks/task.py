from global_utils.kafka_manager import KafkaManager
class Task:

    def __init__(self):
        self.kafka_manager = KafkaManager.get_instance()
        pass

    def do(self, data):
        raise NotImplementedError
