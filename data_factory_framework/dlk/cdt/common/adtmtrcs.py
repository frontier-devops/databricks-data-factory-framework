from datetime import datetime
import json

class Adtmetrcs:
    _instance = None

    def __new__(cls, config=None):
        if cls._instance is None:
            if config is None:
                raise ValueError("Configuration must be provided for the first instance.")
            cls._instance = super(Adtmetrcs, cls).__new__(cls)
            cls._instance._initialize(config)
        return cls._instance
    
    def _initialize(self, config):
        self._config_audit_metrics = {
            "dsn"  : config.get("dsn"),
            "source_loc"    : config.get("source_loc"),
            "processed_file": config.get("processed_file"),
            "target_table"  : config.get("target_table"),
            "load_type"     : config.get("load_type"),
            "dlk_created_by": config.get("config_user"),
            "config_used"   : json.dumps(config)
        }
        
        self._runtime_audit_metrics = {
            "execution_status": "fail",
            "start_time"      : datetime(2024, 8, 3, 10, 0),
            "end_time"        : datetime(2024, 8, 3, 12, 0),
            "source_count"    : 0,
            "processed_count" : 0,
            "target_count"    : 0,
            "dlk_created_by"  : "",
            "dlk_created_time": datetime(2024, 8, 3, 10, 0),
            "dlk_updated_by"  : "",
            "dlk_updated_time": datetime(2024, 8, 3, 12, 0)
        }      
        

    # Config Audit Metrics Properties
    @property
    def dataset_name(self):
        return self._config_audit_metrics['dsn']

    @dataset_name.setter
    def dataset_name(self, value):
        self._config_audit_metrics['dsn'] = value

    @property
    def source_loc(self):
        return self._config_audit_metrics['source_loc']

    @source_loc.setter
    def source_loc(self, value):
        self._config_audit_metrics['source_loc'] = value

    @property
    def processed_file(self):
        return self._config_audit_metrics['processed_file']

    @processed_file.setter
    def processed_file(self, value):
        self._config_audit_metrics['processed_file'] = value

    @property
    def target_table(self):
        return self._config_audit_metrics['target_table']

    @target_table.setter
    def target_table(self, value):
        self._config_audit_metrics['target_table'] = value

    @property
    def load_type(self):
        return self._config_audit_metrics['load_type']

    @load_type.setter
    def load_type(self, value):
        self._config_audit_metrics['load_type'] = value

    @property
    def dlk_created_by(self):
        return self._config_audit_metrics['dlk_created_by']

    @dlk_created_by.setter
    def dlk_created_by(self, value):
        self._config_audit_metrics['dlk_created_by'] = value

    @property
    def config_used(self):
        return self._config_audit_metrics['config_used']

    @config_used.setter
    def config_used(self, value):
        self._config_audit_metrics['config_used'] = json.dumps(value)

    # Runtime Audit Metrics Properties
    @property
    def execution_status(self):
        return self._runtime_audit_metrics['execution_status']

    @execution_status.setter
    def execution_status(self, value):
        if value not in ['success', 'failure']:
            raise ValueError("Execution status must be 'success' or 'failure'.")
        self._runtime_audit_metrics['execution_status'] = value

    @property
    def start_time(self):
        return self._runtime_audit_metrics['start_time']

    @start_time.setter
    def start_time(self, value):
        if not isinstance(value, datetime):
            raise ValueError("Start time must be a datetime object.")
        self._runtime_audit_metrics['start_time'] = value

    @property
    def end_time(self):
        return self._runtime_audit_metrics['end_time']

    @end_time.setter
    def end_time(self, value):
        if not isinstance(value, datetime):
            raise ValueError("End time must be a datetime object.")
        self._runtime_audit_metrics['end_time'] = value

    @property
    def source_count(self):
        return self._runtime_audit_metrics['source_count']

    @source_count.setter
    def source_count(self, value):
        if not isinstance(value, int) or value < 0:
            raise ValueError("Source count must be a non-negative integer.")
        self._runtime_audit_metrics['source_count'] = value

    @property
    def processed_count(self):
        return self._runtime_audit_metrics['processed_count']

    @processed_count.setter
    def processed_count(self, value):
        if not isinstance(value, int) or value < 0:
            raise ValueError("Processed count must be a non-negative integer.")
        self._runtime_audit_metrics['processed_count'] = value

    @property
    def target_count(self):
        return self._runtime_audit_metrics['target_count']

    @target_count.setter
    def target_count(self, value):
        if not isinstance(value, int) or value < 0:
            raise ValueError("Target count must be a non-negative integer.")
        self._runtime_audit_metrics['target_count'] = value

    @property
    def dlk_created_by(self):
        return self._runtime_audit_metrics['dlk_updated_by']

    @dlk_created_by.setter
    def dlk_created_by(self, value):
        if not isinstance(value, str):
            raise ValueError("DLK created by must be a string.")
        self._runtime_audit_metrics['dlk_created_by'] = value

    @property
    def dlk_created_time(self):
        return self._runtime_audit_metrics['dlk_created_time']

    @dlk_created_time.setter
    def dlk_created_time(self, value):
        if not isinstance(value, datetime):
            raise ValueError("DLK created time must be a datetime object.")
        self._runtime_audit_metrics['dlk_created_time'] = value

    @property
    def dlk_updated_by(self):
        return self._runtime_audit_metrics['dlk_updated_by']

    @dlk_updated_by.setter
    def dlk_updated_by(self, value):
        if not isinstance(value, str):
            raise ValueError("DLK updated by must be a string.")
        self._runtime_audit_metrics['dlk_updated_by'] = value

    @property
    def dlk_updated_time(self):
        return self._runtime_audit_metrics['dlk_updated_time']

    @dlk_updated_time.setter
    def dlk_updated_time(self, value):
        if not isinstance(value, datetime):
            raise ValueError("DLK updated time must be a datetime object.")
        self._runtime_audit_metrics['dlk_updated_time'] = value

    def get_config_metrics(self):
        return self._config_audit_metrics

    def get_runtime_metrics(self):
        return self._runtime_audit_metrics