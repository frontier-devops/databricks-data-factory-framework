from data_factory_framework.dlk.cdt.common.constants import FileFormat
from data_factory_framework.dlk.cdt.common.utils import Utils
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.input_source.csv_input_source import \
    CsvInputSource
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.input_source.parquet_input_source import \
    ParquetInputSource


class InputSources:
    def __init__(self):
        self._logger = Utils.lconfigurator()
        config_mgr = Utils.configurator()
        self._config = config_mgr.config
        self._input_source = self._create_input_source()

    @property
    def input_source(self):
        return self._input_source

    def _create_input_source(self):
        if self._config.file_format == FileFormat.CSV:
            return CsvInputSource()
        elif self._config.file_format == FileFormat.PARQUET:
            return ParquetInputSource()
        else:
            err_msg = f"Selected file format {self._config.file_format} is not supported in current version."
            self._logger.info(err_msg)
