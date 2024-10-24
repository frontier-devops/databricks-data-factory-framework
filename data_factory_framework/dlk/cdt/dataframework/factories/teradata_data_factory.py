from data_factory_framework.dlk.cdt.dataframework.factories.i_data_factory import IDataFactory
from data_factory_framework.dlk.cdt.dataframework.products.extract.i_extract_service import IExtractService
from data_factory_framework.dlk.cdt.dataframework.products.ingest.i_ingest_service import IIngestService
from data_factory_framework.dlk.cdt.dataframework.products.extract.teradata_extract_service import TeradataExtractService
from data_factory_framework.dlk.cdt.dataframework.products.ingest.teradata.teradata_ingest_service import TeradataIngestService


class TeradataDataFactory(IDataFactory):
    def createExtractService(self) -> IExtractService:
        return TeradataExtractService()

    def createIngestService(self) -> IIngestService:
        return TeradataIngestService()
