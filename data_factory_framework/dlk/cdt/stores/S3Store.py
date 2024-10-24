from data_factory_framework.dlk.cdt.stores.i_store import IStore


class ISourceType(IStore):
    def read(self):
        pass

