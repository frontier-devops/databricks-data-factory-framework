import os
from abc import ABC, abstractmethod
from data_factory_framework.dlk.cdt.common.utils import Utils


class IDataSource(ABC):
    def __init__(self):
        self._config = Utils.get_stngs()
        self._logger = Utils.lconfigurator()
        self._fconfig = Utils.configurator().framework_config
        self._dconfig = Utils.configurator().table_config(self._config.selected_dataset)
        self._source_loc = self._dconfig.source_loc
        self._target_table = self._dconfig.target_table
        self._source_count = 0

    @abstractmethod
    def source_count(self):
        pass

    @abstractmethod
    def read(self):
        pass
