from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.sources.i_data_source import IDataSource
from data_factory_framework.dlk.cdt.common.constants import FileFormat, CSET
from data_factory_framework.dlk.cdt.common.utils import Utils
from databricks.sdk.runtime import *
from data_factory_framework.dlk.cdt.common.framework_exceptions import DbxIngestionFrameworkException, \
    DbxLoggerException, DbxUtilsException


class DeltaDataSource(IDataSource):
    def __init__(self):
        super().__init__()
        self._df = None

    def read(self):
        self._df = spark.read.format('delta').table(self._target_table)
        return self._df

    def count(self):
        return self._df.count() if self._df is not None else 0
