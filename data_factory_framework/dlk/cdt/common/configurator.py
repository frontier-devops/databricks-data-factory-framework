import json

from data_factory_framework.dlk.cdt.common.prereq.dbxstngs import Dbxstngs
from data_factory_framework.dlk.cdt.common.framework_exceptions import DbxConfigurationException


# from data_factory_framework.dlk.cdt.common.utils import Utils


class Configurator:
    _instance = None

    def __new__(cls, config_path: str = None):
        try:
            if cls._instance is None:
                cls._instance = super(Configurator, cls).__new__(cls)
                cls._instance._config_path = config_path
                cls._instance._config = None
                cls._instance.set_config(config_path)

            return cls._instance
        except Exception as ex:
            err_msg = f"Error in instantiating config manager class. {ex}"
            raise DbxConfigurationException(err_msg, err_msg)

    @property
    def config(self):
        return self._config

    @property
    def selected_dataset(self):
        return self.config.selected_dataset

    @selected_dataset.setter
    def selected_dataset(self, value):
        if not isinstance(value, str):
            raise ValueError("Name must be a string.")
        self.config.selected_dataset = value
        # self.set_config(self._config_path)

    @property
    def logger_config(self):
        return self.config.logger_config

    @property
    def framework_config(self):
        return self.config.framework_config

    @property
    def tables_config(self):
        return self.config.tables_config

    @property
    def table_names(self):
        tables = []
        for table in self.tables_config:
            tables.append(table)
        return tables

    def table_config(self, table_name):
        data = self.tables_config[table_name] if table_name in self.table_names else None
        return data

    def set_config(self, config_path: str):
        with open(config_path) as f:
            config_data = json.load(f)
        self._config = Dbxstngs.from_dict(config_data)

