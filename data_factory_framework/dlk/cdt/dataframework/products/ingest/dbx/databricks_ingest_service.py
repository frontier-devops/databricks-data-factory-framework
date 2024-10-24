import json

from data_factory_framework.dlk.cdt.common.framework_exceptions import DbxIngestionFrameworkException
from data_factory_framework.dlk.cdt.common.utils import Utils
from data_factory_framework.dlk.cdt.common.constants import Ingestions
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx import IngestTypes
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.input_source.input_sources import \
    InputSources
from data_factory_framework.dlk.cdt.dataframework.products.ingest.i_ingest_service import IIngestService


class DatabricksIngestService(IIngestService):
    def __init__(self):
        try:
            self._config = Utils.get_stngs()
            self._logger = Utils.lconfigurator()
            self._input_source = InputSources().input_source
        except Exception as ex:
            log_msg = f"Error raised from 'DatabricksIngestService constructor' function. Details: {ex}"
            raise DbxIngestionFrameworkException(log_msg, log_msg)

    def ingest(self):
        try:
            data = self._input_source.read()
            self._input_source.load(data)
        except Exception as ex:
            err_msg = f"Error raised from 'DatabricksIngestService ingest' function. Details: {ex}"
            self._logger.info(err_msg)
            raise DbxIngestionFrameworkException(err_msg, err_msg)

