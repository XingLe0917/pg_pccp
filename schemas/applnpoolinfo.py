from pydantic import BaseModel


class CreateApplnPoolInfoData(BaseModel):
    db_name: str
    appln_support_code: str
    schemaname: str
    password: str
    password_vault_path:str
    created_by: str
    modified_by: str
    schematype: str