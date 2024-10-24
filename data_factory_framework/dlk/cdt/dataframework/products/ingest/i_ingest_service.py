from abc import ABC, abstractmethod


class IIngestService(ABC):
    @abstractmethod
    def ingest(self):
        pass
