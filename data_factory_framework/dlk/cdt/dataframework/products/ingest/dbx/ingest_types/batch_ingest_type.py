import os
from datetime import datetime
from databricks.sdk.runtime import *
from data_factory_framework.dlk.cdt.common.utils import Utils
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx import IIngestType
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.process.manage_ingest_p import \
    ManageIngestP
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.input_source.csv_input_source import \
    CsvInputSource
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.loads.full_data_load import FullDataLoad
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.loads.incremental_data_load import \
    IncrementalDataLoad
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.sources.data_source_builder import \
    DataSourceBuilder


class BatchIngestType(IIngestType):
    def __init__(self):
        super().__init__()

        self._config_manager = Utils.configurator()
        self._config = self._config_manager.config

        self._initialize_config_data()

    def ingest(self):
        self._start_time = datetime.now().replace(microsecond=0)
        self._logger.info(f"{self._load_type} begin at {self._start_time} for {self._source_loc} file.")

        data = CsvInputSource().read()
        process = ManageIngestP()
        process.load(data)
        self._generate_ingest_metrics()


    def _initialize_config_data(self):
        self._table_config = self._config_manager.table_config(self._config.selected_dataset)
        self._fconfig = self._config_manager.framework_config
        self._source_type = self._config.source_type
        self._file_format = self._config.file_format
        self._load_type = self._table_config.load_type
        self._tgt_table = spark.table(self._table_config.target_table)
        self._process_count = self._tgt_table.count()
        self._data_source = DataSourceBuilder().build()
        self._source_loc = self._table_config.source_loc
        self._app_name = os.path.basename(self._source_loc)
        self._logger = Utils.lconfigurator()

        self._source_count, self._target_count, self._processed_count = 0, 0, 0

    def _read_source_data_old(self):
        src_df = self._data_source.read()
        self._source_count = self._data_source.source_count()
        self._logger.info(f"Reading source data operation finished successfully.")
        return src_df

    def _preprocessing(self, src_df):
        # mnt_src_df = Utils.add_maintenance_column(src_df, self._fconfig.sp_user)
        self._logger.info(f"Maintenance column added to read source data successfully.")
        return src_df

    def _load_data(self, src_df):
        is_load_success = False
        try:
            data_loader = FullDataLoad(src_df) if self._table_config.load_type == "full_load" else IncrementalDataLoad(
                src_df)
            data_loader.load_to_delta()
            self._target_count = spark.table(self._table_config.target_table).count()
            is_load_success = True
        except Exception as ex:
            err_msg = "Error in loading the data to the target."
            raise Exception(err_msg)
        finally:
            return is_load_success

    def _generate_ingest_metrics(self):
        self._logger.info("Data loaded from source to Databricks successfully.")
        self._end_time = datetime.now().replace(microsecond=0)
        self._dlk_created_time = datetime.now().replace(microsecond=0)
        self._dlk_updated_time = datetime.now().replace(microsecond=0)
        self._processed_count = abs(self._process_count - self._target_count)
        self._execution_status = 'success' if (self._source_count == self._target_count) else 'fail'

        self._logger.info(f"""
                {self._load_type} finished with status: {self._execution_status}
                Number of records at target before processing starts: {self._process_count} 
                Number of records at source: {self._source_count}
                Number of records moved to target: {self._target_count}        
                Number of records at target after processing ends: {self._processed_count}               
            """)

    def _post_processing(self):
        self._update_audit_table()
        self._archive_files() if self._execution_status == 'success' else None

    def _archive_files(self):
        Utils.archive_files(self._source_loc, self._fconfig)
        self._logger.info(f"Processed files got archived successfully!")

    def _update_audit_table(self):
        processed_files = Utils.get_all_processed_file_names(self._source_loc)
        Utils.update_audit_table(
            self._dataset,
            self._execution_status,
            self._start_time,
            self._end_time,
            self._source_count,
            self._processed_count,
            self._target_count,
            self._source_loc,
            processed_files,
            self._target_table,
            self._load_type,
            self._fconfig.created_by,
            self._dlk_created_time,
            self._fconfig.updated_by,
            self._dlk_updated_time,
            self._table_config,
            self._fconfig.audit_table
        )
        self._logger.info(f"Audit table got updated successfully!")
