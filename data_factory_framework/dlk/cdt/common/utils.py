import re
import json
from pytz import timezone
from databricks.sdk.runtime import *
from databricks.sdk.service import *
from pyspark.sql.types import StructType
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, rtrim, current_timestamp, to_date
from data_factory_framework.dlk.cdt.common.ftrlog.frmlog import Frmlog
from data_factory_framework.dlk.cdt.common.constants import FLOG
from data_factory_framework.dlk.cdt.common.framework_exceptions import DbxUtilsException
from data_factory_framework.dlk.cdt.common.configurator import Configurator
from data_factory_framework.dlk.cdt.common.prereq.lstngs import Lstngs
from data_factory_framework.dlk.cdt.common.lconfigure import Lconfigure


class Utils:

    @staticmethod
    def configurator():
        return Configurator()

    @staticmethod
    def get_stngs():
        config_mgr = Configurator()
        return config_mgr.config

    @staticmethod
    def lconfigurator():
        l = Lconfigure()
        logger = l.clog()
        return logger

    @staticmethod
    def insert_col(df, fuser):
        try:
            df_with_new_cols = (df.withColumn("env", lit(fuser))
                                .withColumn("dw_load_dttm", current_timestamp())
                                .withColumn("dlk_created_by", lit(fuser))
                                .withColumn("dlk_created_time", current_timestamp())
                                .withColumn("dlk_updated_by", lit(fuser))
                                .withColumn("dlk_updated_time", current_timestamp())
                                )
            return df_with_new_cols
        except Exception as ex:
            log_msg = f"Error raised"
            raise DbxUtilsException(log_msg, log_msg)

    @staticmethod
    def t2d(src_df: DataFrame, tgt_tbl_name):
        try:
            tgt_df = spark.sql(f"select * from {tgt_tbl_name} limit 10")

            for s, t in zip(src_df.schema, tgt_df.schema):
                if s.name.lower() == t.name.lower() and str(s.dataType) == 'TimestampType()' and str(
                        t.dataType) == 'DateType()':
                    src_df = src_df.withColumn(s.name, to_date(col(s.name)))
            return src_df
        except Exception as ex:
            log_msg = f"Error raised"
            raise DbxUtilsException(log_msg, log_msg)

    @staticmethod
    def get_schema(tgt_tbl: str) -> StructType:
        try:
            return spark.table(tgt_tbl).schema
        except Exception as ex:
            log_msg = f"Error raised"
            raise DbxUtilsException(log_msg, log_msg)

    @staticmethod
    def is_directory(path: str) -> bool:
        try:
            dbutils.fs.ls(path)
            return True
        except Exception as ex:
            log_msg = f"Error raised "
            raise DbxUtilsException(log_msg, log_msg)

    @staticmethod
    def is_data_file_exist(source_location: str):
        try:
            if Utils.is_directory(source_location):
                files = dbutils.fs.ls(source_location)
                for file in files:
                    if file.isFile():
                        return True
                    else:
                        return False
        except Exception as ex:
            log_msg = f"Error raised from 'is_data_file_exist' function. Details: {ex}"
            raise DbxUtilsException(log_msg, log_msg)

    @staticmethod
    def get_all_processed_file_names(source_location: str):
        try:
            files = dbutils.fs.ls(source_location)
            files_as_comma_separated_string = ''
            for file in files:
                files_as_comma_separated_string += file.name + ', '
            return files_as_comma_separated_string
        except Exception as ex:
            log_msg = f"Error raised from 'get_all_part_file_names' function. Details: {ex}"
            raise DbxUtilsException(log_msg, log_msg)

    @staticmethod
    def is_file_exists(path: str) -> bool:
        try:
            if Utils.is_directory(path):
                files = dbutils.fs.ls(path)
                for file in files:
                    if file.isFile():
                        return True
                    else:
                        return False
        except Exception as ex:
            log_msg = f"Error raised from 'is_file_exists' function. Details: {ex}"
            raise DbxUtilsException(log_msg, log_msg)

    @staticmethod
    def trim_trailing_spaces(source_df):
        try:
            for column_name in source_df.schema:
                if str(column_name.dataType) == 'StringType()':
                    source_df = source_df.withColumn(column_name.name, rtrim(col(column_name.name)))
            return source_df
        except Exception as ex:
            log_msg = f"Error raised from 'trim_trailing_spaces' function. Details: {ex}"
            raise DbxUtilsException(log_msg, log_msg)

    #
    @staticmethod
    def max_date():
        try:
            max_datetime = datetime.max.replace(microsecond=0)
            return max_datetime
        except Exception as ex:
            log_msg = f"Error raised from 'max_date' function. Details: {ex}"
            raise DbxUtilsException(log_msg, log_msg)

    @staticmethod
    def current_date():
        try:
            current_time = datetime.now().replace(microsecond=0)
            return current_time
        except Exception as ex:
            log_msg = f"Error raised from 'current_date' function. Details: {ex}"
            raise DbxUtilsException(log_msg, log_msg)

    @staticmethod
    def format_str(query):
        try:
            for k in query.split("\n"):
                return "'" + re.sub(r"[^a-zA-Z0-9]+", ' ', k) + "'"
        except Exception as ex:
            log_msg = f"Error raised from 'format_str' function. Details: {ex}"
            raise DbxUtilsException(log_msg, log_msg)

    @staticmethod
    def join_condition(pkey):
        try:
            if not pkey:
                return "No Primary Key"
            else:
                trailing_word_regex = re.compile(r"\s+\w+$")
                pkeystring = ' '.join(['hist.' + str(b.strip()) + ' = inc.' + str(b.strip()) + ' and' for b in pkey])
                pkeystring = trailing_word_regex.sub("", pkeystring)
                return pkeystring
        except Exception as ex:
            log_msg = f"Error raised from 'join_condition' function. Details: {ex}"
            raise DbxUtilsException(log_msg, log_msg)

    @staticmethod
    def add_current_record_columns(df):
        try:
            df_with_new_cols = df.withColumn("iscurrent", lit('Y')) \
                .withColumn("effective_start_date", lit(Utils.current_date())) \
                .withColumn("effective_end_date", lit(Utils.max_date()))
            return df_with_new_cols
        except Exception as ex:
            log_msg = f"Error raised from 'add_current_record_columns' function. Details: {ex}"
            raise DbxUtilsException(log_msg, log_msg)

    @staticmethod
    def check_if_column_exists(df, column_name):
        try:
            exists = column_name.upper() in (name.upper() for name in df.columns)
            return True if exists else False
        except Exception as ex:
            log_msg = f"Error raised from 'check_if_column_exists' function. Details: {ex}"
            raise DbxUtilsException(log_msg, log_msg)

    @staticmethod
    def deleteS3Files(s3_loc):
        pass

    @staticmethod
    def update_audit_table(dataset_name, execution_status, start_time, end_time, source_count, processed_count,
                           target_count, source_loc, processed_file, target_table, load_type, dlk_created_by,
                           dlk_created_time, dlk_updated_by, dlk_updated_time, config_used, audit_table):
        try:
            audit_schema = Utils.get_schema(audit_table)
            audit_df = spark.createDataFrame([(dataset_name, execution_status, start_time, end_time, source_count,
                                               processed_count, target_count, source_loc, processed_file, target_table,
                                               load_type, dlk_created_by, dlk_created_time, dlk_updated_by,
                                               dlk_updated_time, config_used)], schema=audit_schema)
            audit_df.write.format("delta").mode("append").saveAsTable(audit_table)
        except Exception as ex:
            log_msg = f"Error raised from 'update_audit_table' function. Details: {ex}"
            raise DbxUtilsException(log_msg, log_msg)

    @staticmethod
    def archive_files(source_path, framework_config):
        try:
            time_now = datetime.now(timezone('US/Central'))
            time_now = time_now.strftime("%m%d%Y%H%M%S")
            dataset = source_path.split(framework_config.s3_path)[1]
            dest_dir = framework_config.s3_path + f"/archive/{dataset}"
            mv_dir = f"{dest_dir}{time_now}/"
            dbutils.fs.mkdirs(mv_dir)
            files = dbutils.fs.ls(source_path)
            for file in files:
                if file.isDir():
                    continue
                source_path = file.path
                dbutils.fs.mv(source_path, mv_dir)
        except Exception as ex:
            log_msg = f"Error raised from 'archive_files' function. Details: {ex}"
            raise DbxUtilsException(log_msg, log_msg)

    @staticmethod
    def read_json(config_path):
        try:
            with open(config_path, "r") as file:
                return json.load(file)
        except Exception as ex:
            log_msg = f"Error raised from 'read_json' function. Details: {ex}"
            raise DbxUtilsException(log_msg, log_msg)
