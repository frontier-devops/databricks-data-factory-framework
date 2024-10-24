from datetime import datetime

from data_factory_framework.dlk.cdt.common.constants import MyStreams
from data_factory_framework.dlk.cdt.common.framework_exceptions import DbxIngestionFrameworkException
from data_factory_framework.dlk.cdt.common.utils import Utils
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.process.al import \
    AL
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.process.sa import \
    SA
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.process.ss import \
    SS


class ManageIngestP:
    def __init__(self):
        config_mgr = Utils.configurator()
        dataset = config_mgr.config.selected_dataset
        self._logger = Utils.lconfigurator()
        self._table_config = config_mgr.table_config(dataset)
        self._data_process_type = self._table_config.processing_type
        self._data_process_object = self._create_strategy()

    def read(self, read_options):
        try:
            self._logger.info(f"{self._data_process_type} reading started.")
            data = self._data_process_object.read(read_options)
            self._logger.info(f"{self._data_process_type} reading ends.")
            return data
        except Exception as ex:
            err_msg = f"{self._data_process_type} processing raises exception. Error Details: {ex}"
            raise DbxIngestionFrameworkException(err_msg, err_msg)

    def load(self, data):
        loaded = False
        try:
            self._logger.info(f"{self._data_process_type} loading started for table - {self._table_config.target_table} : {datetime.now()}")
            loaded = self._data_process_object.load(data)
            self._logger.info(f"{self._data_process_type} loading ends for table - {self._table_config.target_table} : {datetime.now()}")

        except Exception as ex:
            err_msg = f"{self._data_process_type} processing raises exception. Error Details: {ex}"
            raise DbxIngestionFrameworkException(err_msg, err_msg)
        finally:
            return loaded

    def _create_strategy(self):
        strategy = MyStreams
        data_process = None

        if self._data_process_type == strategy.STRUCTURED_API:
            return SA()
        elif self._data_process_type == strategy.STRUCTURED_STREAMING:
            return SS()
        elif self._data_process_type == strategy.AUTO_LOADER:
            return AL()
        else:
            return SS()
            # err_msg = f"Incorrect processing type been passed."
            # raise DbxIngestionFrameworkException(err_msg, err_msg)
