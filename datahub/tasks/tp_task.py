from datahub.tasks.task import Task
from apps.tp.main import start


class TpTask(Task):
    def __init__(self):
        super().__init__()

    def do(self, data):
        print(data['instrumentID'])
        print(data['price'])
        start(data['instrumentID'], data['price'])
