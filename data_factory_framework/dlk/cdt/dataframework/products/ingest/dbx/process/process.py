from abc import ABC, abstractmethod
from databricks.sdk.runtime import spark
from data_factory_framework.dlk.cdt.common.utils import Utils


class Process:
    def __init__(self):
        config_mgr = Utils.configurator()
        self._logger = Utils.lconfigurator()

        self._config = config_mgr.config
        self._dataset = self._config.selected_dataset
        self._framework_config = config_mgr.framework_config
        self._table_config = config_mgr.table_config(self._dataset)

        self._source_loc = self._table_config.source_loc
        self._target_table = self._table_config.target_table
        self._load_type = self._table_config.load_type
        self._input_schema = Utils.get_schema(self._target_table)
        self._process_count = spark.table(self._target_table).count()
        self._data_process_strategy = self._table_config.processing_type

    @abstractmethod
    def read(self, read_options):
        pass

    @abstractmethod
    def load(self, data, file_format) -> bool:
        pass
