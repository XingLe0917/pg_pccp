import logging

from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.wbxdaomanager import DaoKeys

logger = logging.getLogger("DBAMONITOR")

def get_dbcommon_list(**kargs):
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = depotdbDao.getdbcommonlist(**kargs)
        daoManager.commit()
        res["data"]=[dict(vo) for vo in list]
    except Exception as e:
        daoManager.rollback()
        logger.error("getdbcommonlist error occurred", exc_info=e, stack_info=True)
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
    return res["data"]