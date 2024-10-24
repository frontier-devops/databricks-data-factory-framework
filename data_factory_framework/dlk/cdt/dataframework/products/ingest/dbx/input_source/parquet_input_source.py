from datetime import datetime

from databricks.sdk.runtime import spark, dbutils
from pytz import timezone

from data_factory_framework.dlk.cdt.common.adtmtrcs import Adtmetrcs
from data_factory_framework.dlk.cdt.common.constants import MyStreams
from data_factory_framework.dlk.cdt.common.framework_exceptions import DbxInputSourceException
from data_factory_framework.dlk.cdt.common.utils import Utils
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.input_source.input_source import InputSource


class ParquetInputSource(InputSource):
    def __init__(self):
        try:
            super().__init__()
            self._target_count, self._source_count = 0, 0
            self._logger = Utils.lconfigurator()
            self._audit_metrics = Adtmetrcs(self._static_audit_data)
            self._dlk_created_time = datetime.now().replace(microsecond=0)
            self._dlk_updated_time = datetime.now().replace(microsecond=0)
            self._archive_file_timezone = datetime.now(timezone('US/Central')).strftime("%m%d%Y%H%M%S")
        except Exception as ex:
            err_msg = f"Error raised from 'ParquetInputSource read' function. Details: {ex}"
            self._logger.info(err_msg)
            raise DbxInputSourceException(err_msg, err_msg)
    
    @property
    def archive_file_timezone(self):
        return self._archive_file_timezone
    
    def _is_parquet_present(self):
        try:
            return any(file.path.endswith('.parquet') for file in dbutils.fs.ls(self._source_loc))
        except Exception as e:
            err = f"Error raised from 'is_parquet_present' function. Details: {e}"
            self._logger.info(err)
            raise DbxInputSourceException(err, err) 


    def read(self):
        try:
            if self._is_parquet_present():
                read_options = self._create_read_options()
                self._source_count = spark.read.options(**read_options).parquet(self._source_loc).count()
                self._start_time = datetime.now().replace(microsecond=0)
                data = self._fetch_data(read_options)
                return data
            else:
                msg = f"No Parquet files found in {self._source_loc}."
                raise DbxInputSourceException(msg, msg)
        
        except DbxInputSourceException as ex:
            self._logger.info(ex.log_message)

        except Exception as ex:
            err_msg = f"Error raised from 'read' function. Details: {ex}"
            self._logger.info(err_msg)
            raise DbxInputSourceException(err_msg, err_msg)


    def load(self, data):
        loaded = False
        try:
            self._data_process_manager.load(data)
            self._end_time = datetime.now().replace(microsecond=0)
            self._load_post_processing()
        except Exception as ex:
            err_msg = f"Error raised from 'ParquetInputSource load' function. Details: {ex}"
            self._logger.info(err_msg)
            raise DbxInputSourceException(err_msg, err_msg)

    def _load_post_processing(self):
        try:
            self._target_count = spark.table(self._table_config.target_table).count()
            self._generate_ingest_metrics()
            audit_table_updated = self._update_audit_table()
            archived = self._archive_files() if self._execution_status == 'success' else None
        except Exception as ex:
            err_msg = f"Error raised from 'ParquetInputSource load_post_processing' function. Details: {ex}"
            self._logger.info(err_msg)
            raise DbxInputSourceException(err_msg, err_msg)

    def _create_read_options(self) -> dict:
        try:
            keys = self._table_config.to_dict().keys()
            if self._table_config.processing_type == MyStreams.AUTO_LOADER:
                read_options = {
                    "cloudFiles.format": "parquet",
                    "mergeSchema": "true"
                }
                return read_options
            else:
                read_options = {
                    "mergeSchema": "true"
                }
                return read_options
        except Exception as ex:
            err_msg = f"Error raised from 'ParquetInputSource create_read_options' function. Details: {ex}"
            self._logger.info(err_msg)
            raise DbxInputSourceException(err_msg, err_msg)

    def _fetch_data(self, read_options):
        try:
            self._logger.info(f"Reading input Parquet file from {self._source_loc}...")
            data = self._data_process_manager.read(read_options)
            data = self._trim_trailing_spaces(data)
            data = self._add_maintenance_cols(data)
            return data
        except Exception as ex:
            err_msg = f"Error raised from 'fetch_data' function. Details: {ex}"
            self._logger.info(err_msg)
            raise DbxInputSourceException(err_msg, err_msg)

    def _trim_trailing_spaces(self, data):
        try:
            keys = self._table_config.to_dict().keys()
            trim_data = Utils.trim_trailing_spaces(data) if (
                    ('trim_columns' in keys) and (self._table_config.trim_columns == 'yes')
            ) else data
            return trim_data
        except Exception as ex:
            err_msg = f"Error raised from 'trim_trailing_spaces' function. Details: {ex}"
            self._logger.info(err_msg)
            raise DbxInputSourceException(err_msg, err_msg)

    def _update_audit_table(self):
        updated = False
        try:
            processed_files = Utils.get_all_processed_file_names(self._source_loc)
            self._audit_metrics.execution_status = self._execution_status
            self._audit_metrics.start_time = self._start_time
            self._audit_metrics.end_time = self._end_time
            self._audit_metrics.source_count = self._source_count
            self._audit_metrics.processed_count = self._processed_count
            self._audit_metrics.target_count = self._target_count
            self._audit_metrics.dlk_created_by = self._framework_config.created_by
            self._audit_metrics.dlk_created_time = self._dlk_created_time
            self._audit_metrics.dlk_updated_by = self._framework_config.updated_by
            self._audit_metrics.dlk_updated_time = self._dlk_updated_time

            audit_table_updated = self._update_audit_data()
            self._logger.info(f"Audit table got updated successfully!")
            updated = True
        except Exception as ex:
            err_msg = f"Error raised from 'update_audit_table' function. Details: {ex}"
            self._logger.info(err_msg)
            raise DbxInputSourceException(err_msg, err_msg)
        finally:
            return updated    

    def _archive_files(self):
        archived = False
        success_msg = f"Archiving processed files completed successfully for '{self._table_config.source_loc}'"
        unsuccessful_msg = f"Archiving not done for '{self._table_config.source_loc}."

        try:
            archive_loc_base = self._table_config.archive_loc
            archive_loc_base = archive_loc_base+'/' if not archive_loc_base.endswith('/') else archive_loc_base
            archive_loc = f"{archive_loc_base}{self._archive_file_timezone}/"
            self._logger.info("Archiving begins...")
            files = dbutils.fs.ls(self._table_config.source_loc)       
            if files is not None:
                moved = []
                for file in files:                    
                    if file.path.endswith(".parquet") and "checkpoint" not in file.path:
                        source_loc = file.path
                        target_loc = archive_loc + file.name
                        self._logger.info(f"Source file to archive:\n{source_loc}")
                        self._logger.info(f"Target file:\n{target_loc}")
                        
                        source_loc = source_loc.replace("s3://", "s3a://")
                        target_loc = target_loc.replace("s3://", "s3a://")
                        
                        success = dbutils.fs.mv(source_loc, target_loc, True)
                        moved.append(success)
                archived = True if all(moved) else False
            log_msg = success_msg if archived else unsuccessful_msg
            self._logger.info(log_msg)
        except Exception as ex:
            err_msg = f"Error raised from 'ParquetInputSource archive_files' function. Details: {ex}"
            self._logger.info(err_msg)
            raise DbxInputSourceException(err_msg)
        finally:
            return archived

    def _add_maintenance_cols(self, data):
        try:
            data_with_audit_cols = Utils.insert_col(data, self._framework_config.sp_user)
            return data_with_audit_cols
        except Exception as ex:
            err_msg = f"Error raised from 'add_maintenance_cols' function. Details: {ex}"
            self._logger.info(err_msg)
            raise DbxInputSourceException(err_msg, err_msg)

    def _generate_ingest_metrics(self):
        try:
            self._logger.info("Data loaded from source to Databricks successfully.")
            self._end_time = datetime.now().replace(microsecond=0)
            self._dlk_created_time = datetime.now().replace(microsecond=0)
            self._dlk_updated_time = datetime.now().replace(microsecond=0)
            self._processed_count = abs(self._process_count - self._target_count)
            success_condition = (self._source_count == self._target_count) or (self._source_count < self._target_count)
            self._execution_status = 'success' if success_condition else 'failure'

            self._logger.info(f"""
                **** Data Ingestion Summary ****
                ---------------------------------------------------------------
                {self._table_config.load_type} finished with status: {self._execution_status}
                Number of records found at source: {self._source_count}
                Number of records at target before processing starts: {self._process_count}
                Number of records at target after processing ends: {self._target_count}
                Number of records moved to target: {self._processed_count}
            """)
        except Exception as ex:
            err_msg = f"Error raised from 'ParquetInputSource generate_ingest_metrics' function. Details: {ex}"
            self._logger.info(err_msg)
            raise DbxInputSourceException(err_msg, err_msg)
