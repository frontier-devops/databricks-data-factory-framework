import json
from data_factory_framework.dlk.cdt.common.constants import Ingestions
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.factories.batch.dbx_batch_ingestion_factory import \
    DbxBatchIngestionFactory
from data_factory_framework.dlk.cdt.dataframework.products.ingest.i_ingest_service import IIngestService
from data_factory_framework.dlk.cdt.common.utils import Utils


class DatabricksIngestService(IIngestService):
    def __init__(self, dataset, config):
        self._dataset = dataset
        self._config = config
        self._logger = Utils.create_logger(self._config["logger_config"])
        self._ingestion_type = self._config["ingestion_type"]

    def ingest(self):
        if self._ingestion_type == Ingestions.BATCH:
            self._logger.info("Data Ingestion for Batch data begin...")
            batch_ingestion = DbxBatchIngestionFactory(self._dataset, self._config).create_dbx_batch_ingestion()
            batch_ingestion.ingest()
        elif self._ingestion_type == Ingestions.STREAM:
            raise Exception("Stream Ingestion is not supported in the current version!")
            # stream_ingestion = DbxStreamIngestionFactory(prereq)
