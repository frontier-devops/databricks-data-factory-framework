from data_factory_framework.dlk.cdt.dataframework.products.extract.i_extract_service import IExtractService

class AS400ExtractService(IExtractService):
    def extract(self):        
        return "AS400 Extract Services Worked!"
