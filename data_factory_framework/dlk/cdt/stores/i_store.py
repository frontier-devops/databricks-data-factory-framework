from abc import ABC, abstractmethod


class IStore(ABC):
    @abstractmethod
    def read(self):
        pass
