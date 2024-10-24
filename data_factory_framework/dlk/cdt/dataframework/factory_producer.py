from data_factory_framework.dlk.cdt.common.framework_exceptions import DbxIngestionFrameworkException
from data_factory_framework.dlk.cdt.common.utils import Utils
from data_factory_framework.dlk.cdt.dataframework.factories.teradata_data_factory import TeradataDataFactory
from data_factory_framework.dlk.cdt.dataframework.factories.as400_data_factory import AS400DataFactory
from data_factory_framework.dlk.cdt.dataframework.factories.databricks_data_factory import DatabricksDataFactory
from data_factory_framework.dlk.cdt.common.constants import Factories
from data_factory_framework.dlk.cdt.dataframework.factories.i_data_factory import IDataFactory

DATA_FACTORY_MAP = {
    Factories.DATABRICKS: DatabricksDataFactory,
    Factories.TERADATA: TeradataDataFactory,
    Factories.AS400: AS400DataFactory,
}


class FactoryProducer:
    def __init__(self, factory_type: Factories = Factories.DATABRICKS):
        try:
            self._factory_type = factory_type
            self._logger = Utils.lconfigurator()
        except Exception as ex:
            err_msg = f"Error raised from 'FactoryProducer constructor' function. Details: {ex}"
            self._logger.info(err_msg)
            raise DbxIngestionFrameworkException(err_msg, err_msg)

    def get_factory(self) -> IDataFactory:
        try:
            self._logger.info(f"Data factory '{self._factory_type}' service started...")
            factory_class = DATA_FACTORY_MAP[self._factory_type]
            return factory_class()
        except KeyError as ex:
            err_msg = f" Factory not available for {self._factory_type}"
            self._logger.info(err_msg)
            raise ValueError(err_msg)
        except Exception as ex:
            err_msg = f"Error raised from 'FactoryProducer constructor' function. Other details: {ex}"
            self._logger.info(err_msg)
            raise DbxIngestionFrameworkException(err_msg, err_msg)
