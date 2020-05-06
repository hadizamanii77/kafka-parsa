from datahub.tasks.task import Task

import random
import uuid


class TpTask(Task):
    def __init__(self):
        super().__init__()

    def do(self, data):
        print(data['instrumentID'])
        print(data['price'])
