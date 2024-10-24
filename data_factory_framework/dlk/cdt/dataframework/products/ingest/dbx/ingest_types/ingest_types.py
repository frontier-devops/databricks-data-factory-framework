from data_factory_framework.dlk.cdt.common.constants import Ingestions
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx import (
    BatchIngestType,
    StreamIngestType,
    IIngestType
)
from data_factory_framework.dlk.cdt.common.utils import Utils


class IngestTypes:
    def __init__(self):
        self._config = Utils.get_stngs()
        self._dataset = self._config.selected_dataset

    def build(self, ingest_type: Ingestions):
        if ingest_type == Ingestions.BATCH:
            return BatchIngestType()
