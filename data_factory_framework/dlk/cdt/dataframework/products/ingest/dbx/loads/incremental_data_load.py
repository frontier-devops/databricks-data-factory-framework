from pyspark.sql import DataFrame

from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.loads.i_data_load import IDataLoad
from data_factory_framework.dlk.cdt.common.constants import MyStreams

processing_type = MyStreams.AUTO_LOADER
source_count = 0


class IncrementalDataLoad(IDataLoad):
    def load_to_delta(self):
        if processing_type == MyStreams.STRUCTURED_API:
            self.write_structured_api()
        elif processing_type == MyStreams.AUTO_LOADER:
            self.write_autoloader(self._config.file_format)
        elif processing_type == MyStreams.STRUCTURED_STREAMING:
            self.write_structured_streaming()
        self._logger.info(f"Incremental data loaded to delta table at location: {self._table_config.target_table}")

    def write_structured_api(self):
        self \
            ._src_df \
            .write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(self._table_config.target_table)

    def process_batch(self, batch_df: DataFrame, batch_id: int):
        if not batch_df.isEmpty:
            self._logger.info(f"Garg batch id: {batch_id}")
        # batch_count = df.count()
        # self._source_count += batch_count
        # self._logger.info(f"Batch ID: {epoch_id}, Batch Count: {batch_count}, Total Count: {self._source_count}")

    def write_autoloader(self, file_format="delta"):
        write_stream = (self._src_df
                        .writeStream
                        .outputMode("append")
                        .format(file_format)
                        .option("path", self._table_config.target_table)
                        .option("checkpointLocation", self._table_config.source_loc + "/checkpoint/")
                        .trigger(once=True)
                        .toTable(self._table_config.target_table)
                        )
        write_stream.awaitTermination()

    def write_structured_streaming(self):
        pass
