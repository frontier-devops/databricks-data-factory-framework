from data_factory_framework.dlk.cdt.dataframework.products.extract.i_extract_service import IExtractService


class TeradataExtractService(IExtractService):
    def extract(self):
        return "Teradata Extract Services Worked!"
