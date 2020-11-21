from contextlib import AbstractContextManager
import random


class BaseRunner(AbstractContextManager):
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        self.seed = random.seed(9001)

    def __exit__(self):
        raise NotImplementedError

    def get_random_int(self):
        return random.randint(1, 100000)

    def get_storage_size(self):
        raise NotImplementedError

    def write_one(self, data):
        raise NotImplementedError

    def prefill(self, data, n):
        for i in range(n):
            self.write_one(data)

    def __repr__(self):
        return self.__class__.__name__
