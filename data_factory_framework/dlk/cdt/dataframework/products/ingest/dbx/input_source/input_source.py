from abc import ABC, abstractmethod
from datetime import datetime
from urllib.parse import urlparse

from databricks.sdk.runtime import spark, dbutils

from data_factory_framework.dlk.cdt.common.framework_exceptions import DbxInputSourceException
from data_factory_framework.dlk.cdt.common.utils import Utils
from data_factory_framework.dlk.cdt.dataframework.products.ingest.dbx.process.manage_ingest_p import \
    ManageIngestP
from data_factory_framework.dlk.cdt.common.adtmtrcs import Adtmetrcs
from data_factory_framework.dlk.cdt.common.adt import Adt


class InputSource(ABC):
    def __init__(self):
        try:
            config_mgr = Utils.configurator()

            self._config = config_mgr.config
            self._dataset = self._config.selected_dataset
            self._framework_config = config_mgr.framework_config
            self._table_config = config_mgr.table_config(self._dataset)

            self._logger = Utils.lconfigurator()

            self.__initialize_utils_vars()
            self.__initialize_audit_data()
        except Exception as ex:
            err_msg = f"Error raised from 'InputSource constructor' function. Details: {ex}"
            self._logger.info(err_msg)
            raise DbxInputSourceException(err_msg, err_msg)

    @abstractmethod
    def read(self):
        pass

    def __initialize_utils_vars(self):
        self._source_loc = self._table_config.source_loc
        self._target_table = self._table_config.target_table
        self._input_schema = Utils.get_schema(self._target_table)
        self._process_count = spark.table(self._target_table).count()
        self._data_process_strategy = self._table_config.processing_type
        self._data_process_manager = ManageIngestP()

    def __initialize_audit_data(self):
        self._static_audit_data = {
            "dsn": self._dataset,
            "source_loc": self._source_loc,
            "processed_file": "",
            "target_table": self._target_table,
            "load_type": self._table_config.load_type,
            "dlk_created_by": self._framework_config.sp_user,
            "config_used": self._config.to_dict()
        }

    def _update_audit_data(self):
        """ Update the audit data with the runtime audit data """

        audit_data_updated = False
        try:
            audit_config = self._static_audit_data
            audit_table_name = self._framework_config.audit_table

            audit_metrics = Adtmetrcs(audit_config)
            config_audit_data = audit_metrics.get_config_metrics()
            runtime_audit_data = audit_metrics.get_runtime_metrics()

            merged_data = {**config_audit_data, **runtime_audit_data}
            audit_data = Adt(**merged_data).dict()
            audit_schema = spark.table(audit_table_name).schema
            audit_df = spark.createDataFrame([audit_data], audit_schema)

            audit_df.write.format("delta").insertInto(audit_table_name)
            audit_data_updated = True
        except Exception as ex:
            print(f"Error while updating the audit table: {ex}")
        finally:
            return audit_data_updated

    def __is_file(self, path):
        parsed_path = urlparse(path).path
        return parsed_path.split('/')[-1].count('.') > 0

    def __extract_file_name(self, path):
        parsed_path = urlparse(path).path
        return parsed_path.split('/')[-1].split('.')[0]

    def __extract_file_extension(self, path):
        parsed_path = urlparse(path).path
        return parsed_path.split('/')[-1].split('.')[1]

    def __extract_file_name_with_ext(self, path):
        parsed_path = urlparse(path).path
        return parsed_path.split('/')[-1]

    def _get_archive_path(self, src_loc, archive_loc):
        current_time = datetime.now().strftime('%Y%m%d%H%M%S')   

        if self.__is_file(src_loc):        
            file_name = self.__extract_file_name(src_loc)
            file_extension = self.__extract_file_extension(src_loc)
            archive_path = archive_loc+file_name+'_'+current_time+'.'+file_extension
            return archive_path
        else:
            archive_path = archive_loc+current_time+'/'
            return archive_path

    def _archive_data(self, src_loc, archive_loc, extensions):
        if not self.__is_file(src_loc):
            files = dbutils.fs.ls(src_loc)
            archive_fully_qualified_path = self._get_archive_path(src_loc, archive_loc)
            files_to_move = [file for file in files if any(file.path.endswith(ext) for ext in extensions)]
            for file in files_to_move:
                final_archive_loc = archive_fully_qualified_path+file.name
                dbutils.fs.mv(file.path, final_archive_loc, True)
                self._logger.info(file.path+" --> archived to --> "+final_archive_loc)
        else:
            archive_fully_qualified_path = self._get_archive_path(src_loc, archive_loc)
            dbutils.fs.mv(src_loc, archive_fully_qualified_path, True)
            self._logger.info(src_loc+" --> archived to --> "+archive_fully_qualified_path)

