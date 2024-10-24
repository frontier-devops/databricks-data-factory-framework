from data_factory_framework.dlk.cdt.common.framework_exceptions import DbxIngestionFrameworkException
from data_factory_framework.dlk.cdt.dataframework.factory_producer import FactoryProducer
from data_factory_framework.dlk.cdt.common.constants import Factories, ServiceType
from data_factory_framework.dlk.cdt.common.utils import Utils
from data_factory_framework.dlk.cdt.common.configurator import Configurator
from data_factory_framework.dlk.cdt.common.lconfigure import Lconfigure
from data_factory_framework.metadata import METADATA


class DataFactoryFramework:
    def __init__(self,
                 config_path=r"C:\repos\ftr\data-factory-framework\tests\dlk-cdt-scorecard-state-prereq-filelog.json",
                 factory_type: Factories = Factories.DATABRICKS):
        self._config_manager = Configurator(config_path)
        self._log_config = self._config_manager.logger_config
        self._log_manager = Lconfigure()
        factory_producer = FactoryProducer(factory_type)
        self._factory = factory_producer.get_factory()
        self._logger = Utils.lconfigurator()

    def ingest(self):
        """
        Ingest data from the external sources to Databricks.

        """
        try:
            self._factory.create_ingest_service().ingest()
        except Exception as ex:
            err_msg = f"Error raised from 'FactoryProducer constructor' function. Details: {ex}"
            self._logger.info(err_msg)
            raise DbxIngestionFrameworkException(err_msg, err_msg)

    @property
    def dataset(self):
        """
        Retrieve dataset name

        Returns:
            string: The dataset
        """
        return self._config_manager.selected_dataset

    @dataset.setter
    def dataset(self, value):
        """
        Set selected dataset

        Args:
            value (str): The dataset name
        """            
        self._config_manager.selected_dataset = value

    @property
    def config(self):
        """
        Retrieve prereq

        Returns:
            dict: The prereq
        """        
        return self._config_manager.config

    # @property
    # def config_manager(self):
    #     return self._config_manager

    @property
    def logger(self):
        """
        Retrieve ftrlog

        Returns:
            Logger: The ftrlog
        """             
        return Utils.lconfigurator()


    @property
    def version(self):
        """
        Retrieve data factory framework's version number

        Returns:
            string: The version number           
        """         
        return METADATA.get("version")

    @property
    def supported_file_formats(self):
        """
        Retrieve data factory framework's supported file formats

        Returns:
            string: The supported file formats    
        """   
        return METADATA.get("supported_file_format")

    @property
    def supported_input_storage(self):
        """
        Retrieve data factory framework's supported input storage

        Returns:
            string: The supported input storage
        """   
        return METADATA.supported_input_storage

    @property
    def supported_feature(self):
        """
        Retrieve data factory framework's supported features

        Returns:
            List:   The supported features  
        """   
        return METADATA.get("supported_feature")
    
    @property
    def release_date(self):
        """
        Retrieve data factory framework's release date

        Returns:
            string: The release date
        """   
        return METADATA.get("release_date")
