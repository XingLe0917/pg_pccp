from typing import Optional
from pydantic import BaseModel

class DBStatusQueryParams(BaseModel):
    db_name: Optional[str]
    db_type: Optional[str]