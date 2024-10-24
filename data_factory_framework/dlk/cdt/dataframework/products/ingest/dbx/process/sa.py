from data_factory_framework.dlk.cdt.common.constants import Loads
from data_factory_framework.dlk.cdt.common.framework_exceptions import DbxDataProcessingException
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.process.process import \
    Process
from databricks.sdk.runtime import spark


class SA(Process):
    def read(self, read_options):
        try:
            df = (spark
                  .read
                  .format(self._config.file_format)
                  .schema(self._input_schema)
                  .options(**read_options)
                  .load(self._source_loc)
                  )
            return df
        except Exception as ex:
            err_msg = f"Error while reading data using Structured API. More details: {ex}"
            raise DbxDataProcessingException(err_msg, err_msg)

    def load(self, data, file_format: str = "delta"):
        loaded = False
        try:
            mode = "append" if self._load_type == Loads.INCREMENTAL else "overwrite"
            data \
                .write \
                .mode(mode) \
                .option("mergeSchema", "true") \
                .saveAsTable(self._table_config.target_table)
            loaded = True
        except Exception as ex:
            err_msg = f"Error while loading data using Structured API. More details: {ex}"
            raise DbxDataProcessingException(err_msg, err_msg)
        finally:
            return loaded
