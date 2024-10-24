from data_factory_framework.dlk.cdt.dataframework.factories.i_data_factory import IDataFactory
from data_factory_framework.dlk.cdt.dataframework.products.extract.i_extract_service import IExtractService
from data_factory_framework.dlk.cdt.dataframework.products.ingest.i_ingest_service import IIngestService
from data_factory_framework.dlk.cdt.dataframework.products.extract.databricks_extract_service import DatabricksExtractService
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.databricks_ingest_service import DatabricksIngestService


class DatabricksDataFactory(IDataFactory):
    def create_extract_service(self) -> IExtractService:
        return DatabricksExtractService()

    def create_ingest_service(self) -> IIngestService:
        return DatabricksIngestService()
