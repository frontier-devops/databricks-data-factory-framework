from data_factory_framework.dlk.cdt.dataframework.products.extract.i_extract_service import IExtractService


class DatabricksExtractService(IExtractService):
    def __init__(self, dataset, config):
        self._dataset = dataset
        self._config = config

    def extract(self):
        return "Databricks Extract Services Worked!"
