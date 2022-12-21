import logging
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys
from common.wbxinfluxdb import wbxinfluxdb


logger = logging.getLogger("DBAMONITOR")


def get_homepage_db_version_count():
    status = "SUCCEED"
    errormsg = ""
    logger.info("Starting to get_homepage_db_version_count...")
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    data = {}
    try:
        depotDaoManager.startTransaction()
        data = depotdbDao.get_homepage_server_info()
        data.update({"class": "grid-con-1",
                    "name": "DB Version Count"})
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAILED"
        logger.info("failed to get_homepage_server_count by %s" % errormsg)
    finally:
        depotDaoManager.close()
        return {"status": status,
                "data": data,
                "errormsg": errormsg}


def get_homepage_db_count():
    status = "SUCCEED"
    errormsg = ""
    logger.info("Starting to get_homepage_db_count...")
    data = {}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        data = depotdbDao.get_db_count_info()
        data.update({"class": "grid-con-2",
                    "name": "DB Count"})
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAILED"
        logger.info("failed to get_homepage_db_count by %s" % errormsg)
    finally:
        depotDaoManager.close()
        return {"status": status,
                "data": data,
                "errormsg": errormsg}


def get_homepage_db_type_count():
    status = "SUCCEED"
    errormsg = ""
    logger.info("Starting to get_homepage_db_type_count...")
    data = {}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        data = depotdbDao.get_db_type_count_info()
        data.update({"class": "grid-con-3",
                    "name": "DB Type Count"})
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAILED"
        logger.info("failed to get_homepage_db_type_count by %s" % errormsg)
    finally:
        depotDaoManager.close()
        return {"status": status,
                "data": data,
                "errormsg": errormsg}


def get_shareplex_count():
    status = "SUCCEED"
    errormsg = ""
    logger.info("Starting to get_shareplex_count...")
    data = {
        "name": "Splex Count",
        "value": "splex_count",
        "class": "grid-con-4",
        "total": 0,
        "data": []
    }
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        data["data"] = depotdbDao.get_shareplex_count_info()
        data["total"] = depotdbDao.get_shareplex_count_total()
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAILED"
        logger.info("failed to get_shareplex_count by %s" % errormsg)
    finally:
        depotDaoManager.close()
        return {"status": status,
                "data": data,
                "errormsg": errormsg}


def get_rencent_alert_info():
    status = "SUCCEED"
    errormsg = ""
    logger.info("Starting to get_rencent_alert_info...")
    data = {"rencet_alert": []}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    depotDaoManager = daoManagerFactory.getDefaultDaoManager()
    depotdbDao = depotDaoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        depotDaoManager.startTransaction()
        data["rencet_alert"] = depotdbDao.get_rencent_alert()
        depotDaoManager.commit()
    except Exception as e:
        depotDaoManager.rollback()
        errormsg = str(e)
        status = "FAILED"
        logger.info("failed to get_rencent_alert_info by %s" % errormsg)
    finally:
        depotDaoManager.close()
        return {"status": status,
                "data": data,
                "errormsg": errormsg}


def get_top_active_session_db_count():
    status = "SUCCEED"
    errormsg = ""
    logger.info("Starting to get_top_active_session_db_count...")
    data = {"top_active_session_db": []}
    try:
        data["top_active_session_db"] = wbxinfluxdb().get_active_session_count()
    except Exception as e:
        errormsg = str(e)
        status = "FAILED"
        logger.info("failed to get_top_active_session_db_count by %s" % errormsg)
    finally:
        return {"status": status,
                "data": data,
                "errormsg": errormsg}
