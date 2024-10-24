from data_factory_framework.dlk.cdt.dataframework.products.ingest.i_ingest_service import IIngestService


class AS400IngestService(IIngestService):
    def ingest(self, config_path: str):
        return "AS400 Ingest Services Worked!"
