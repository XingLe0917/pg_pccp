from typing import Optional, Literal
from pydantic import BaseModel

class QueryParams(BaseModel):
    host_name: Optional[str]
    db_name: Optional[str]
    add_status: Optional[Literal["PENDING", "RUNNING", "SUCCEED", "FAILED"]]
    drop_status: Optional[Literal["PENDING", "RUNNING", "SUCCEED", "FAILED"]]

class PartitionJobSchema(BaseModel):
    host_name: str
    db_name: str
    status: Literal["PENDING", "RUNNING", "FAILED", "SUCCEED"]
    optype: Literal["ADD", "DROP"]
    jobtype: Literal["BEGIN", "END"]
    log: str