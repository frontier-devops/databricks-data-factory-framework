from databricks.sdk.runtime import *

import os
import logging
from data_factory_framework.dlk.cdt.common.framework_exceptions import DbxLoggerException


class Frmlog:
    _instance = None

    def __new__(
            cls,
            log_to_file: bool = False,
            log_file_dir: str = None,
            logger_name: str = None,
            dsn: str = None,
            lvl=logging.INFO
    ):
        try:
            if cls._instance is None:
                cls._instance = super(Frmlog, cls).__new__(cls)
                cls._instance._logger = logging.getLogger(logger_name)
                cls._instance._logger.setLevel(lvl)
                cls._logger_name = logger_name
                cls._logger_dataset_name = dsn

                if log_to_file and (log_file_dir == "" or log_to_file):
                    cls._instance._seth(log_file_dir)
                else:
                    cls._instance._run()
            return cls._instance
        except Exception as ex:
            err_msg = f"Error raised from 'Framework Logger'. Details: {ex}"
            raise DbxLoggerException(err_msg, err_msg)

    def _seth(self, log_file_dir: str = ""):
        try:
            self._log_file_name = f'controller-{self._logger_dataset_name}.log'
            self._log_file_path = os.path.join(log_file_dir, self._log_file_name)

            fileh = logging.FileHandler(self._log_file_path)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            fileh.setFormatter(formatter)
            self._logger.addHandler(fileh)
        except Exception as ex:
            err_msg = f"Error raised from 'set file handler' function. Details: {ex}"
            raise DbxLoggerException(err_msg, err_msg)

    def _run(self):
        try:
            logging.basicConfig(level=logging.INFO,
                                format='%(asctime)s %(levelname)s %(message)s',
                                handlers=[logging.StreamHandler()])
            __name__ = self._logger_name if self._logger_name is not None else __name__
            self._log = logging.getLogger(__name__)
        except Exception as ex:
            raise DbxLoggerException("Error raised!", "Error raised!")

    def info(self, message):
        try:
            if hasattr(self, '_log'):
                self._log.info(message)
            else:
                self._logger.info(message)
        except Exception as ex:
            raise DbxLoggerException("Error raised!", "Error raised!")

