from datetime import datetime

from data_factory_framework.dlk.cdt.common.constants import Loads
from data_factory_framework.dlk.cdt.common.framework_exceptions import DbxDataProcessingException
from data_factory_framework.dlk.cdt.common.utils import Utils
from databricks.sdk.runtime import spark, dbutils

from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.process.process import \
    Process


class AL(Process):
    def read(self, read_options):
        try:
            if self._input_schema:
                read_options.pop("header", None)
                read_options.pop("inferSchema", None)
                stream = (spark.readStream
                          .format("cloudFiles")
                          .schema(self._input_schema)
                          .option("cloudFiles.format", self._config.file_format)
                          .option("cloudFiles.schemaLocation", self._source_loc + "/checkpoint/")
                          .options(**read_options)
                          .load(self._source_loc))
            else:
                stream = (spark.readStream.format("cloudFiles")
                          .option("cloudFiles.format", self._config.file_format)
                          .format("cloudFiles")
                          .option("cloudFiles.schemaLocation", self._source_loc + "/checkpoint/")
                          .options(**read_options)
                          .load(self._source_loc))
            return stream
        except Exception as ex:
            err_msg = f"Error while reading data using Auto Loader. More details: {ex}"
            raise DbxDataProcessingException(err_msg, err_msg)

    def load(self, data, file_format: str = "delta"):
        loaded = False
        try:
            mode = "append" if self._load_type.lower() == Loads.INCREMENTAL else "append"
            checkpoint_loc = self._table_config.source_loc + "checkpoint/"

            if self._load_type.lower() == Loads.FULL:
                dbutils.fs.rm(checkpoint_loc, recurse=True)
                self._logger.info(
                    f"Checkpoint directory cleared for table - {self._table_config.target_table}, as user have requested full_load")

            write_stream = (data
                            .writeStream
                            .format(file_format)
                            .option("checkpointLocation", checkpoint_loc)
                            .option("mergeSchema", "true")
                            .outputMode(mode)
                            .trigger(availableNow=True)
                            .toTable(self._table_config.target_table)
                            )
            write_stream.awaitTermination()
            loaded = True
        except Exception as ex:
            err_msg = f"Error while loading data using Auto Loader. More details: {ex}"
            self._logger.info(err_msg)
            raise DbxDataProcessingException(err_msg, err_msg)
        finally:
            return loaded
