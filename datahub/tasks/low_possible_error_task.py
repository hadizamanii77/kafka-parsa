from datahub.tasks.task import Task
import random
from global_utils.exception import RandomError


class LowPossibleErrorTask(Task):
    def __init__(self):
        self.begin = 0
        self.end = 100
        pass

    def is_error(self,rand_num):
        return rand_num == self.end

    def do(self):
        rand_num = random.randint(self.begin, self.end)
        if self.is_error(rand_num):
            raise RandomError
        else:
            return {
                'data' : random.randint(0,10)
            }
