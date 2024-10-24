import dataclasses
import json
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional


@dataclass
class Fcnfg:
    env: str
    s3_log_path: str
    sp_user: str
    log_path: str
    log_fname: str
    s3_path: str
    catalog_name: str
    raw_catalog_name: str
    silver_catalog_name: str
    audit_table: str
    process_control_table: str
    created_by: str
    updated_by: str

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(**data)
