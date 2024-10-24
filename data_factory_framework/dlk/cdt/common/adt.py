from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import json
from databricks.sdk.runtime import *


class Adt(BaseModel):
    dataset_name: str
    execution_status: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    source_count: Optional[int] = None
    processed_count: Optional[int] = None
    target_count: Optional[int] = None
    source_loc: Optional[str] = None
    processed_file: Optional[str] = None
    target_table: str
    load_type: str
    dlk_created_by: str
    dlk_created_time: Optional[datetime] = None
    dlk_updated_by: Optional[str] = None
    dlk_updated_time: Optional[datetime] = None
    config_used: str

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }