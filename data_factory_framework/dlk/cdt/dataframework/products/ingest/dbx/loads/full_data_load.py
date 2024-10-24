from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.loads.i_data_load import IDataLoad
from data_factory_framework.dlk.cdt.common.constants import MyStreams

processing_type = MyStreams.STRUCTURED_API


class FullDataLoad(IDataLoad):
    def load_to_delta(self):

        if processing_type == MyStreams.STRUCTURED_API:
            result = (self
                      ._src_df
                      .write
                      .mode("overwrite")
                      .option("mergeSchema", "true")
                      .saveAsTable(self._table_config.target_table)
                      )
            return result
        elif processing_type == MyStreams.AUTO_LOADER:
            self.write_autoloader()
        elif processing_type == MyStreams.STRUCTURED_STREAMING:
            self.write_structured_streaming()

        self._logger.info(f"Full data loaded to delta table at location: {self._table_config.target_table}")

    def write_autoloader(self, df, file_format):
        write_stream = df \
            .writeStream \
            .format(file_format) \
            .option("path", self._table_config.target_table) \
            .option("checkpointLocation", self._table_config.source_loc + "/checkpoint/") \
            .trigger(availableNow=True) \
            .toTable(self._table_config.target_table)
        write_stream.awaitTermination()

    def write_structured_streaming(self):
        pass
