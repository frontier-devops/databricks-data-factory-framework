from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.sources.delta_data_source import DeltaDataSource
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.sources.i_data_source import IDataSource
from data_factory_framework.dlk.cdt.common.constants import Storages
from data_factory_framework.dlk.cdt.common.utils import Utils
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.sources.s3_data_source import S3DataSource


class DataSourceBuilder:
    def __init__(self, source_type=None):
        self._config = Utils.get_stngs()
        self._dataset = self._config.selected_dataset
        self._logger = Utils.lconfigurator()
        self._source_type = source_type if source_type is not None else self._config.source_type

    def build(self) -> IDataSource:
        if self._source_type == Storages.S3:
            return S3DataSource()
        elif self._source_type == Storages.DELTA:
            return DeltaDataSource()
        else:
            self._logger.info("The selected data source is not supported in the current version!")

