import dataclasses
import json
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional


@dataclass
class Tcnfg:
    load_type: str
    processing_type: str
    source: str
    target: str
    source_loc: str
    archive_loc: str
    target_table: str
    copy_sql: str
    s3_delete: str
    primaryKey: str
    time_between_consecutive_runs: str
    file_format: str
    tgt_schema: str
    header: Optional[str] = None
    compression: Optional[str] = None
    delimiter: Optional[str] = None
    escape_character: Optional[str] = None
    quote: Optional[str] = None
    multiline: Optional[str] = None

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict):
        return cls()
