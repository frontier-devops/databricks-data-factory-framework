import logging


class Ingestions:
    STREAM = "stream"
    BATCH = "batch"


class Loads:
    FULL = "full_load"
    INCREMENTAL = "incremental_load"


class LoadFrequency:
    SCHEDULED = "scheduled"
    HISTORICAL = "historical"



class FileFormat:
    CSV = "csv"
    PARQUET = "parquet"


class ServiceType:
    INGEST = "ingest"
    EXTRACT = "extract"
    CURATE = "curate"


class Factories:
    AS400 = "as400"
    TERADATA = "teradata"
    DATABRICKS = "databricks"


class Storages:
    S3 = "s3"
    DELTA = "delta"
    KAFKA = "kafka"
    ORACLE = "oracle"
    TERADATA = "teradata"
    REDPANDA = "redpanda"



CSET = {
    "header": "false",
    "delimiter": ",",
    "compression": "none",
    "quote": "\"",
    "escape": "\\",
    "multiline": "false"
}

FLOG = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}


class MyStreams:
    STRUCTURED_API = "structured_api"
    STRUCTURED_STREAMING = "structured_streaming"
    AUTO_LOADER = "auto_loader"


