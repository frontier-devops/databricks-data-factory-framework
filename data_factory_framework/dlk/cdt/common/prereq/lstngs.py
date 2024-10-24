import json
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional


@dataclass
class Lstngs:
    log_to_file: bool
    log_file_dir: str
    logger_name: str
    dataset_name: str
    log_level: str

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(**data)