from abc import ABC, abstractmethod
from data_factory_framework.dlk.cdt.common.utils import Utils


class IIngestType(ABC):
    def __init__(self):
        self._config = Utils.get_stngs()
        self._logger = Utils.lconfigurator()
        self._dataset = self._config.selected_dataset

    @abstractmethod
    def ingest(self):
        pass
