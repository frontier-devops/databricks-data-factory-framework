from data_factory_framework.dlk.cdt.dataframework.factories.i_data_factory import IDataFactory
from data_factory_framework.dlk.cdt.dataframework.products.extract.i_extract_service import IExtractService
from data_factory_framework.dlk.cdt.dataframework.products.ingest.i_ingest_service import IIngestService
from data_factory_framework.dlk.cdt.dataframework.products.extract.as400_extract_service import AS400ExtractService
from data_factory_framework.dlk.cdt.dataframework.products.ingest.as400.as400_ingest_service import AS400IngestService


class AS400DataFactory(IDataFactory):
    def createExtractService(self) -> IExtractService:
        return AS400ExtractService()

    def createIngestService(self) -> IIngestService:
        return AS400IngestService()

    # def createTransformService(self) -> ITransformService:
    #     raise NotImplementedError("AS400DataFactory does not support transform service")
