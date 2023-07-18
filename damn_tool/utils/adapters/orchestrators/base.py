from abc import ABC, abstractmethod

class BaseOrchestratorAdapter(ABC):
    @abstractmethod
    def __init__(self, config):
        pass

    @abstractmethod
    def execute(self, sql):
        pass
