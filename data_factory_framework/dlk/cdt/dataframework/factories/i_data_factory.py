from abc import ABC, abstractmethod
from data_factory_framework.dlk.cdt.dataframework.products.extract.i_extract_service import IExtractService
from data_factory_framework.dlk.cdt.dataframework.products.ingest.i_ingest_service import IIngestService


class IDataFactory(ABC):
    @abstractmethod
    def create_extract_service(self) -> IExtractService:
        pass

    @abstractmethod
    def create_ingest_service(self) -> IIngestService:
        pass
