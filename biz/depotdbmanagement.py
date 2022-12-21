import logging

from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.wbxdaomanager import DaoKeys

logger = logging.getLogger("DBAMONITOR")

def depot_manage_add_DB(database_info_vo):
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        rows = depotdbDao.get_database_info_by_dbname(database_info_vo.db_name)
        if len(rows) == 0:
            depotdbDao.insert_database_info(database_info_vo)
        else:
            depotdbDao.update_database_info(database_info_vo)
        daoManager.commit()
    except Exception as e:
        logger.error(str(e))
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = "depot_manage_add_DB error occurred with error %s" % (str(e))
        return res
    return res


def depot_manage_add_host(host_info_vo):
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        rows = depotdbDao.get_host_info_by_cname(host_info_vo.cname)
        if len(rows) == 0:
            depotdbDao.insert_host_info(host_info_vo)
        else:
            depotdbDao.update_host_info(host_info_vo)
        daoManager.commit()
    except Exception as e:
        logger.error(str(e))
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = "depot_manage_add_host error occurred with error %s" % (str(e))
        return res
    return res

def get_depot_manage_list(host_name,db_name,cname):
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        # db_list = depotdbDao.get_depot_db_list(cname,db_name)
        # host_list = depotdbDao.get_depot_host_list(cname,host_name)
        list = depotdbDao.get_depot_manage_list(host_name,db_name,cname)
        daoManager.commit()
        data = {}
        # data['host_info'] = [dict(vo) for vo in host_list]
        # data['db_info'] = [dict(vo) for vo in db_list]
        res["data"] = [dict(vo) for vo in list]
    except Exception as e:
        logger.error(str(e))
        daoManager.rollback()
        logger.error("get_depot_manage_list error occurred", exc_info=e, stack_info=True)
        res["status"] = "FAILED"
        res["errormsg"] = "get_depot_manage_list error occurred with error %s" % (str(e))
        return res
    return res
