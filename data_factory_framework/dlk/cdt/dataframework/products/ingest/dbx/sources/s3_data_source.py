from pyspark.sql import DataFrame
from pyspark.sql.functions import count

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
    def read(self):
        file_format = Utils.get_stngs().file_format
        if file_format == FileFormat.CSV:
            return self._read_csv()
        elif file_format == FileFormat.PARQUET:
            return self._read_parquet()

    def source_count(self):
        return self._source_count

    def read_data_autoloader(self, file_format, schema, read_options=None, input_path=None):
        if schema:
            read_options.pop("header", None)
            read_options.pop("inferSchema", None)
            stream = (spark.readStream
                      .schema(schema)
                      .format("cloudFiles")
                      .option("cloudFiles.format", file_format)
                      .options(**read_options)
                      .load(input_path))
        else:
            stream = (spark.readStream.format("cloudFiles")
                      .option("cloudFiles.format", file_format)
                      .option("cloudFiles.schemaLocation", self._source_loc + "/checkpoint/")
                      .options(**read_options)
                      .load(input_path))
        return stream

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
        if schema:
            stream = (spark
                      .readStream
                      .format(file_format)
                      .options(**read_options)
                      .schema(schema)
                      .load(input_path)
                      )
        else:
            stream = (spark
                      .readStream
                      .format(file_format)
                      .options(**read_options)
                      .load(input_path)
                      )

        return stream

    def write_structured_streaming(self, df, file_format="delta"):
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

    def read_structured_api(self, schema, options):
        df = (spark
              .read
                 .format(FileFormat.CSV)
                 .schema(schema)
                 .options(**options)
                 .load(self._source_loc)
        )
        return df

    def write_structured_api(self, df):
        (df
            .write
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(self._dconfig.target_table))

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

            self._process_count = spark.read.format('delta').table(self._target_table).count()
            if processing_strategy == MyStreams.STRUCTURED_API:
                src_df = self.read_structured_api(schema, options)
                self._source_count = src_df.count()
                trimmed_src_df = Utils.trim_trailing_spaces(src_df) if (
                        'trim_columns' in self._dconfig.to_dict().keys() and
                        self._dconfig.trim_columns == 'yes'
                ) else src_df
                maint_src_df = Utils.insert_col(trimmed_src_df, self._fconfig.sp_user)
                self.write_structured_api(maint_src_df)

            elif processing_strategy == MyStreams.AUTO_LOADER:
                stream = self.read_data_autoloader("csv", schema, options, input_path=self._source_loc)
                trimmed_stream = Utils.trim_trailing_spaces(stream) if (
                        'trim_columns' in self._dconfig.to_dict().keys() and
                        self._dconfig.trim_columns == 'yes'
                ) else stream
                maint_stream = Utils.insert_col(trimmed_stream, self._fconfig.sp_user)
                return maint_stream
                self.write_autoloader(maint_stream)
            elif processing_strategy == MyStreams.STRUCTURED_STREAMING:
                stream = self.read_structured_streaming("csv", schema, options, input_path=self._source_loc)
                trimmed_stream = Utils.trim_trailing_spaces(stream) if (
                        'trim_columns' in self._dconfig.to_dict().keys() and
                        self._dconfig.trim_columns == 'yes'
                ) else stream
                maint_stream = Utils.insert_col(trimmed_stream, self._fconfig.sp_user)
                self.write_structured_streaming(maint_stream)

            # self._source_count = src_df.count()
            # Count the number of records

            # src_df = Utils.trim_trailing_spaces(src_df) if (
            #         'trim_columns' in self._dconfig.to_dict().keys() and
            #         self._dconfig.trim_columns == 'yes'
            # ) else src_df
            # self.write_data_autoloader(src_df)

            # src_maintenance_df = Utils.add_maintenance_column(src_df, self._fconfig.sp_user)
            # self._process_count = (spark.read.format('delta').table(self._target_table).count())
            # return src_df
        except DbxLoggerException as ex:
            raise DbxIngestionFrameworkException(ex.log_message, ex.user_message)
        except DbxUtilsException as ex:
            raise DbxIngestionFrameworkException(ex.log_message, ex.user_message)
        except Exception as ex:
            log_msg = f"Error raised at 'S3DataSource._read_csv' function. Details: {ex}"
            raise DbxIngestionFrameworkException(log_msg, log_msg)

    def _read_parquet(self):
        pass
