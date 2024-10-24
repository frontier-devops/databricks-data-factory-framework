from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.interfaces.batch.i_dbx_batch_ingestion import \
    IDbxBatchIngestion
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.process.batch.s3.factories.s3_file_format_factory import \
    S3FileFormatFactory


class DbxS3BatchIngestion(IDbxBatchIngestion):
    def __init__(self, dataset, config):
        self._dataset = dataset
        self._config = config
        self._file_format = config["file_format"]

    def ingest(self):
        file_format_ingestion = S3FileFormatFactory(self._dataset, self._config).create_file_format_ingestion_object()
        return file_format_ingestion.ingest()
