from abc import ABC, abstractmethod


class IExtractService(ABC):
    @abstractmethod
    def extract(self):
        pass
