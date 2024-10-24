from databricks.sdk.runtime import *
from data_factory_framework.dlk.cdt.common.constants import FileFormat, Loads
from data_factory_framework.dlk.cdt.common.framework_exceptions import DbxDataProcessingException
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.process.process import \
    Process


class SS(Process):
    def read(self, read_options):
        try:
            if self._input_schema:
                stream = (spark
                          .readStream
                          .format(self._config.file_format)
                          .options(**read_options)
                          .schema(self._input_schema)
                          .load(self._source_loc)
                          )
                return stream
            else:
                stream = (spark
                          .readStream
                          .format(self._config.file_format)
                          .options(**read_options)
                          .load(self._source_loc)
                          )
                return stream
        except Exception as ex:
            err_msg = f"Error while reading data using Structured Streaming. More details: {ex}"
            raise DbxDataProcessingException(err_msg, err_msg)

    def load(self, data, file_format: str = "delta"):
        loaded = False
        mode = "append" if self._load_type.lower() == Loads.INCREMENTAL else "append"
        checkpoint_loc = self._table_config.source_loc + "/checkpoint/"

        if self._load_type.lower() == Loads.FULL:
            dbutils.fs.rm(checkpoint_loc, recurse=True)
            self._logger.info(
                f"Checkpoint directory cleared for table - {self._table_config.target_table}, as user have requested full_load")

        try:
            mode = "append" if self._load_type == Loads.INCREMENTAL else "append" #"complete"
            write_stream = (data
                            .writeStream
                            .outputMode(mode)
                            .format(file_format)
                            .option("mergeSchema", "true")
                            # .option("path", self._table_config.target_table)
                            .option("checkpointLocation", self._table_config.source_loc + "/checkpoint/")
                            .trigger(once=True)
                            .toTable(self._table_config.target_table)
                            )
            write_stream.awaitTermination()
            loaded = True
        except Exception as ex:
            err_msg = f"Error while loading data using Structured Streaming. More details: {ex}"
            raise DbxDataProcessingException(err_msg, err_msg)
        finally:
            return loaded
