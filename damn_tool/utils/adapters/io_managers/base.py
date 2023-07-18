from abc import ABC, abstractmethod

class BaseIOManagerAdapter(ABC):
    @abstractmethod
    def __init__(self, config):
        pass