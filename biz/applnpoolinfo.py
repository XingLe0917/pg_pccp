import logging
from datetime import datetime
from schemas.applnpoolinfo import CreateApplnPoolInfoData
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")


def get_daoManager():
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    return daoManager

def creata_appln_pool_info(db_name,appln_support_code,schemaname,password,password_vault_path,created_by,modified_by,schematype):
    logger.info("creata_appln_pool_info")
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManager = get_daoManager()
    try:
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        appln_data = dao.getApplnPoolInfo(db_name,schemaname)
        if len(appln_data)>0:
            dao.update_appln_pool_info(db_name, appln_support_code, schemaname, password, password_vault_path,
                                       modified_by, schematype)
        else:
            dao.insert_appln_pool_info(db_name, appln_support_code, schemaname, password, password_vault_path,
                                       created_by, modified_by, schematype)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res


def insert_appln_pool_info(data: CreateApplnPoolInfoData):
    message = ""
    daomanager = get_daoManager()
    session = daomanager.startTransaction()
    try:
        sql = """
            insert into 
            appln_pool_info(db_name,appln_support_code,schemaname,password,password_vault_path,
                            created_by,modified_by,schematype,createtime,lastmodifiedtime)
            values('{db_name}','{appln_support_code}','{schemaname}','{password}','{password_vault_path}',
                '{created_by}','{modified_by}','{schematype}','{createtime}','{lastmodifiedtime}');
        """.format(**data.dict(), createtime=datetime.now(), lastmodifiedtime=datetime.now())
        session.execute(sql)
        session.commit()
        message = "SUCCEED"
    except Exception as e:
        session.rollback()
        message = "FAILED"
        raise e
    else:
        session.close()
    return message


def get_appln_pool_info():
    result = []
    daomanager = get_daoManager()
    session = daomanager.startTransaction()
    sql = """
    select db_name,appln_support_code,schemaname,password,
           created_by,modified_by,schematype,
           to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
           to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from appln_pool_info;
    """
    result_cur = session.execute(sql)
    for row in result_cur.fetchall():
        result.append(dict(
            db_name=row[0],
            appln_support_code=row[1],
            schemaname=row[2],
            password=row[3],
            created_by=row[4],
            modified_by=row[5],
            schematype=row[6],
            createtime=row[7],
            lastmodifiedtime=row[8],
        ))
    session.close()
    return result