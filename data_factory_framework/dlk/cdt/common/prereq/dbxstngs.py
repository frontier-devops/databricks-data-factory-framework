import dataclasses
from dataclasses import dataclass
from typing import Dict

from data_factory_framework.dlk.cdt.common.prereq.fcnfg import Fcnfg
from data_factory_framework.dlk.cdt.common.prereq.lstngs import Lstngs
from data_factory_framework.dlk.cdt.common.prereq.tcnfg import Tcnfg


@dataclass
class Dbxstngs:
    logger_config: Lstngs
    ingestion_type: str
    source_type: str
    file_format: str
    selected_dataset: str
    framework_config: Fcnfg
    tables_config: Dict[str, Tcnfg]

    def to_dict(self):
        return {
            "logger_config": self.logger_config.to_dict(),
            "ingestion_type": self.ingestion_type,
            "source_type": self.source_type,
            "file_format": self.file_format,
            "selected_dataset": self.selected_dataset,
            "framework_config": self.framework_config.to_dict(),
            "tables_config": {name: table_config.to_dict() for name, table_config in self.tables_config.items()},
        }

    @classmethod
    def from_dict(cls, config_data: Dict):
        logger_config = Lstngs(**config_data["logger_config"])
        framework_config = Fcnfg(**config_data["framework_config"])
        tables_config = {name: Tcnfg(**table) for name, table in config_data["tables_config"].items()}
        return cls(
            logger_config=logger_config,
            ingestion_type=config_data["ingestion_type"],
            source_type=config_data["source_type"],
            file_format=config_data["file_format"],
            selected_dataset=config_data["selected_dataset"],
            framework_config=framework_config,
            tables_config=tables_config
        )