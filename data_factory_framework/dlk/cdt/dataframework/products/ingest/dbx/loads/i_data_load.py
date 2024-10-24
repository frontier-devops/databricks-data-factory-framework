from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from data_factory_framework.dlk.cdt.common.utils import Utils


class IDataLoad(ABC):
    def __init__(self, src_df: DataFrame):
        self._source_count = 0
        self._src_df = src_df
        self._logger = Utils.lconfigurator()
        self._config = Utils.get_stngs()
        self._config_manager = Utils.configurator()
        self._table_config = self._config_manager.table_config(self._config.selected_dataset)

    def load_to_delta(self):
        pass
