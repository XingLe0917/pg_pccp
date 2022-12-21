import logging

from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.wbxdaomanager import DaoKeys

logger = logging.getLogger("DBAMONITOR")

def save_BackupStatus(host_name,db_name,full_backup_status,incr_backup_status):
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    if not db_name:
        res['status'] = "FAILED"
        res['errormsg'] = "db_name is null"
    if not host_name:
        res['status'] = "FAILED"
        res['errormsg'] = "host_name is null"
        return res
    if full_backup_status and incr_backup_status:
        res['status'] = "FAILED"
        res['errormsg'] = "Both full_backup_status and incr_backup_status have values"
        return res
    if full_backup_status and full_backup_status not in ["RUNNING","SUCCEED","FAILED"]:
        res['status'] = "FAILED"
        res['errormsg'] = "full_backup_status:['RUNNING','SUCCEED','FAILED']"
        return res
    if incr_backup_status and incr_backup_status not in ["RUNNING","SUCCEED","FAILED"]:
        res['status'] = "FAILED"
        res['errormsg'] = "incr_backup_status:['RUNNING','SUCCEED','FAILED']"
        return res
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = depotdbDao.getBackupStatusList(host_name)
        if len(list) == 0:
            logger.info("insert backupStatus")
            depotdbDao.insertBackupStatus(host_name,db_name,full_backup_status,incr_backup_status)
        else:
            logger.info("update backupStatus")
            depotdbDao.updateBackupStatus(host_name,db_name,full_backup_status,incr_backup_status)
        daoManager.commit()
    except Exception as e:
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
        daoManager.rollback()
        logger.error("save_BackupStatus error occurred", exc_info=e, stack_info=True)
    return res

def getBackupStatusList(host_name):
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = depotdbDao.getBackupStatusList(host_name)
        daoManager.commit()
        return [dict(vo) for vo in list]
    except Exception as e:
        daoManager.rollback()
        logger.error("getBackupStatusList error occurred", exc_info=e, stack_info=True)