# from data_factory_framework.dlk.cdt.common.prereq.dbx_config import Dbxstngs
# from data_factory_framework.dlk.cdt.common.utils import Utils

from data_factory_framework.dlk.cdt.common.prereq.lstngs import Lstngs
from data_factory_framework.dlk.cdt.common.constants import FLOG
from data_factory_framework.dlk.cdt.common.ftrlog.frmlog import Frmlog
from data_factory_framework.dlk.cdt.common.framework_exceptions import DataFactoryFrameworkException
from data_factory_framework.dlk.cdt.common.configurator import Configurator


class Lconfigure:
    def __init__(self):
        config_manager = Configurator()
        log_config: Lstngs = config_manager.logger_config

        self._to_file = log_config.log_to_file
        self._log_file_dir = log_config.log_file_dir
        self._dataset_name = log_config.dataset_name
        self._logger_name = log_config.logger_name
        self._log_level = FLOG.get(log_config.log_level.upper())

    def clog(self):
        try:
            if self._log_level is None:
                raise ValueError(f"Invalid log level: {self._log_level}. Must be one of {list(FLOG.keys())}")

            if self._to_file == "True":
                return Frmlog(
                    log_to_file=True,
                    log_file_dir=self._log_file_dir,
                    logger_name=self._logger_name,
                    dsn=self._dataset_name,
                    lvl=self._log_level
                )
            else:
                return Frmlog(
                    log_to_file=False,
                    logger_name=self._logger_name,
                    dsn=self._dataset_name,
                    lvl=self._log_level
                )
        except Exception as ex:
            log_msg = f"Error raised from 'create_logger_object' function. Details: {ex}"
            raise DataFactoryFrameworkException(log_msg, log_msg)

# from data_factory_framework.dlk.cdt.common.config_manager import Configurator
#
# Lconfigure(Configurator(
#     r"C:\repos\ftr\data-factory-framework\tests\dlk-cdt-scorecard-state-prereq-filelog.json").logger_config)
