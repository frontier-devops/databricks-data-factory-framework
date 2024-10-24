from abc import abstractmethod, ABC
from databricks.sdk.runtime import *
from databricks.sdk.service import *


class DataFactoryFrameworkException(Exception):
    """Base exception interface for all exceptions in Ftr."""

    def __init__(self, log_message=None, user_message=None, error_code=None):
        self._log_message = log_message
        self._user_message = user_message
        self._error_code = error_code
        super().__init__(log_message)

    @property
    def error_code(self):
        return self._error_code

    @property
    def log_message(self):
        return self._log_message

    @property
    def user_message(self):
        return self._user_message

    def __str__(self):
        return f"{self.__class__.__name__}: {self.log_message} [Error Code: {self.error_code}]"


class DbxUtilsException(DataFactoryFrameworkException, ABC):
    def __init__(self,
                 log_message='Error in Databricks utility functions!',
                 user_message='Error in Databricks utility functions!',
                 error_code='UTILS_ERROR'
                 ):
        super().__init__(log_message, user_message, error_code)


class DbxIngestionFrameworkException(DataFactoryFrameworkException, ABC):
    def __init__(self,
                 log_message='Error in Databricks Ingestion Framework',
                 user_message='Error in Databricks Ingestion Framework',
                 error_code='DATABRICKS_INGESTION_FRAMEWORK_ERROR'
                 ):
        super().__init__(log_message, user_message, error_code)


class DbxLoggerException(DataFactoryFrameworkException, ABC):
    def __init__(self,
                 log_message='Error in Databricks Ingestion Framework Logger Module',
                 user_message='Error in Databricks Ingestion Framework Logger Module',
                 error_code='Error in Databricks Ingestion Framework Logger Module'
                 ):
        super().__init__(log_message, user_message, error_code)


class DbxDataProcessingException(DataFactoryFrameworkException, ABC):
    def __init__(self,
                 log_message='Error in processing data.',
                 user_message='Error in processing data.',
                 error_code='DATABRICKS_DATA_PROCESS_ERROR'
                 ):
        super().__init__(log_message, user_message, error_code)


class DbxInputSourceException(DataFactoryFrameworkException, ABC):
    def __init__(self,
                 log_message='Error in input source.',
                 user_message='Error in input source.',
                 error_code='DATABRICKS_INPUT_SOURCE_ERROR'
                 ):
        super().__init__(log_message, user_message, error_code)


class DbxConfigurationException(DataFactoryFrameworkException, ABC):
    def __init__(self,
                 log_message='Error in configuration data',
                 user_message='Error in configuration data',
                 error_code='DATABRICKS_CONFIG_ERROR'
                 ):
        super().__init__(log_message, user_message, error_code)
