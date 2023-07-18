from abc import ABC, abstractmethod

class BaseDataWareouseAdapter(ABC):
    @abstractmethod
    def __init__(self, config):
        pass

    @abstractmethod
    def execute(self, sql):
        pass

    @abstractmethod
    def close(self):
        pass
