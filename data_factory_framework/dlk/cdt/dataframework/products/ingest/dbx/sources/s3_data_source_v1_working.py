from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.sources.i_data_source import IDataSource
from data_factory_framework.dlk.cdt.common.constants import FileFormat, CSET
from data_factory_framework.dlk.cdt.common.utils import Utils
from databricks.sdk.runtime import *
from data_factory_framework.dlk.cdt.common.constants import MyStreams
from data_factory_framework.dlk.cdt.common.framework_exceptions import DbxIngestionFrameworkException, \
    DbxLoggerException, DbxUtilsException

processing_strategy = MyStreams.AUTO_LOADER
source_count = 0


class S3DataSource(IDataSource):
    def __init__(self):
        super().__init__()

        self._config = Utils.get_stngs()
        self._logger = Utils.lconfigurator()
        self._fconfig = Utils.configurator().framework_config
        self._dconfig = Utils.configurator().table_config(self._config.selected_dataset)
        self._source_loc = self._dconfig.source_loc
        self._target_table = self._dconfig.target_table
        self._source_count = 0

    def read(self):
        file_format = Utils.get_stngs().file_format
        if file_format == FileFormat.CSV:
            return self._read_csv()
        elif file_format == FileFormat.PARQUET:
            return self._read_parquet()

    def source_count(self):
        return self._source_count  #if self._source_count is not None else 0

    def read_data_autoloader(self, file_format, schema, read_options=None, input_path=None):
        if schema:
            read_options.pop("header", None)
            read_options.pop("inferSchema", None)
            df = (spark.readStream
                  .schema(schema)
                  .format("cloudFiles")
                  .option("cloudFiles.format", file_format)
                  .options(**read_options)
                  .load(input_path))
        else:
            df = (spark.readStream.format("cloudFiles")
                  .option("cloudFiles.format", file_format)
                  .option("cloudFiles.schemaLocation", self._source_loc + "/checkpoint/")
                  .options(**read_options)
                  .load(input_path))

        return df

    def write_data_autoloader(self, data, file_format="delta", write_options=None, input_path=None):
        writeStream = data \
            .writeStream \
            .format(file_format) \
            .option("path", self._dconfig.target_table) \
            .option("checkpointLocation", self._source_loc + "/checkpoint/") \
            .trigger(availableNow=True) \
            .toTable(self._target_table)
        writeStream.awaitTermination()

    def process_batch(self, batch_df, batch_id):
        global source_count

        batch_count = batch_df.count()
        source_count += batch_count
        self._logger.info(f"Batch ID: {batch_id}, Batch Count: {batch_count}, Total Count: {source_count}")

    def write_autoloader(self, df, file_format="delta"):
        write_stream = (df
            .writeStream
            # .foreachBatch(self.process_batch)
            .outputMode("append")
            .format(file_format)
            .option("path", self._dconfig.target_table)
            .option("checkpointLocation", self._dconfig.source_loc + "/checkpoint/")
            # .trigger(availableNow=True)
            .trigger(once=True)
            .toTable(self._dconfig.target_table)
            # .start(self._table_config.target_table)
        )
        write_stream.awaitTermination()

    def read_structured_streaming(self, file_format, schema, read_options=None, input_path=None):
        stream = None
        # if schema:
        #     stream = spark \
        #         .readStream \
        #         .format(file_format) \
        #         .options(**read_options) \
        #         .schema(schema) \
        #         .load(input_path)
        # else:
        #     stream = spark \
        #         .readStream \
        #         .format(file_format) \
        #         .options(**read_options) \
        #         .schema(schema) \
        #         .load(input_path)
        # return stream

        if schema:
            stream = spark \
                .readStream \
                .format(file_format) \
                .options(**read_options) \
                .schema(schema) \
                .load(input_path) \
                .writeStream \
                .option("path", self._dconfig.target_table) \
                .option("checkpointLocation", self._dconfig.target_table + "/checkpoint/") \
                .trigger(availableNow=True) \
                .start()
            stream.awaitTermination()
        else:
            stream = spark \
                .readStream \
                .format(file_format) \
                .options(**read_options) \
                .load(input_path) \
                .writeStream \
                .option("path", self._dconfig.target_table) \
                .option("checkpointLocation", input_path + "/checkpoint/") \
                .trigger(availableNow=True) \
                .start()

        return stream

    def _create_options_csv(self):
        dconfig_keys = self._dconfig.to_dict().keys()
        delimiter = self._dconfig.delimiter if 'delimiter' in dconfig_keys else CSET.get("delimiter")
        header = self._dconfig.header if 'header' in dconfig_keys else CSET.get("header")
        compression = self._dconfig.compression if 'compression' in dconfig_keys else CSET.get(
            "compression")
        quote = self._dconfig.quote if 'quote' in dconfig_keys else CSET.get("quote")
        escape = self._dconfig.escape_character if 'escape_character' in dconfig_keys else CSET.get(
            "escape")
        multiline = self._dconfig.multiline if 'multiline' in dconfig_keys else CSET.get("multiline")

        options = {
            "header": header,
            "delimiter": delimiter,
            "compression": compression,
            "quote": quote,
            "escape": escape,
            "multiline": multiline
        }

        return options

    def _create_options(self):
        dconfig_keys = self._dconfig.to_dict().keys()
        delimiter = self._dconfig.delimiter if 'delimiter' in dconfig_keys else CSET.get("delimiter")
        header = self._dconfig.header if 'header' in dconfig_keys else CSET.get("header")
        compression = self._dconfig.compression if 'compression' in dconfig_keys else CSET.get(
            "compression")
        quote = self._dconfig.quote if 'quote' in dconfig_keys else CSET.get("quote")
        escape = self._dconfig.escape_character if 'escape_character' in dconfig_keys else CSET.get(
            "escape")
        multiline = self._dconfig.multiline if 'multiline' in dconfig_keys else CSET.get("multiline")

        options = {
            "header": header,
            "delimiter": delimiter,
            "compression": compression,
            "quote": quote,
            "escape": escape,
            "multiline": multiline
        }

        return options

    def _read_csv(self):
        try:
            src_df = None
            self._logger.info(f"Reading input CSV file from {self._source_loc}...")
            schema = Utils.get_schema(self._target_table)
            options = self._create_options()

            if processing_strategy == MyStreams.STRUCTURED_API:
                # src_df = self.read_data_autoloader("csv", schema, options, input_path=self._source_loc)
                src_df = (spark.read
                          .format(FileFormat.CSV)
                          .schema(schema)
                          .options(**options)
                          .load(self._source_loc))
            elif processing_strategy == MyStreams.AUTO_LOADER:
                src_df = self.read_data_autoloader("csv", schema, options, input_path=self._source_loc)
                # self.write_data_autoloader(src_df)
                self.write_autoloader(src_df)
            elif processing_strategy == MyStreams.STRUCTURED_STREAMING:
                src_df = self.read_structured_streaming("csv", schema, options, input_path=self._source_loc)

            # self._source_count = src_df.count()
            self._source_count = source_count
            src_df = Utils.trim_trailing_spaces(src_df) if (
                    'trim_columns' in self._dconfig.to_dict().keys() and
                    self._dconfig.trim_columns == 'yes'
            ) else src_df
            # self.write_data_autoloader(src_df)

            # src_maintenance_df = Utils.add_maintenance_column(src_df, self._fconfig.sp_user)
            self._process_count = (spark.read.format('delta').table(self._target_table).count())
            return src_df
        except DbxLoggerException as ex:
            raise DbxIngestionFrameworkException(ex.log_message, ex.user_message)
        except DbxUtilsException as ex:
            raise DbxIngestionFrameworkException(ex.log_message, ex.user_message)
        except Exception as ex:
            log_msg = f"Error raised at 'S3DataSource._read_csv' function. Details: {ex}"
            raise DbxIngestionFrameworkException(log_msg, log_msg)

    def _read_parquet(self):
        pass