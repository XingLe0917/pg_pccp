import logging
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")

def getVaultPath(db_name,schema):
    logger.info("getVaultPath, db_name=%s,schema=%s" %(db_name,schema))
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        vo = dao.getVaultPath(str(db_name).lower(),schema)
        if vo:
            item = dict(vo)
            password_vault_path = item['password_vault_path']
            schemaname = item['schemaname']
            schematype = item['schematype']
            new_password_vault_path = password_vault_path
            if str(schema).lower()=="wbxdba":
                new_password_vault_path += "/"+schemaname
            elif str(schema).lower().startswith("splex_"):
                new_password_vault_path += "/splex_deny"
            else:
                new_password_vault_path += "/"+schematype
            item['password_vault_path'] = new_password_vault_path
            daoManager.commit()
            res['data'] = item
        else:
            daoManager.rollback()
            res['status'] = "FAILED"
            res['errormsg'] = "Do not find vault path, db_name=%s,schema=%s" %(db_name,schema)
            return res
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res