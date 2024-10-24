from data_factory_framework.dlk.cdt.dataframework.products.ingest.i_ingest_service import IIngestService


class TeradataIngestService(IIngestService):
    def ingest(self, config_path: str):
        return "Teradata Ingest Services Worked!"
