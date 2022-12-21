import logging

from prettytable import PrettyTable

from common.wbxmail import wbxemailtype, wbxemailmessage, sendemail
from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.wbxdaomanager import DaoKeys

logger = logging.getLogger("DBAMONITOR")

def getCheckCronStatus():
    logger.info("getCheckCronStatus")
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    ls = []
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = depotdbDao.getCheckCronStatus()
        daoManager.commit()
        for vo in list:
            item = dict(vo)
            if (item['status'] and "running" in item['status']) or item['db_agent_exist'] == "1":
                item['check_result'] = "1"
            else:
                item['check_result'] = "0"
            if item['status'] and "running" in item['status']:
                item['cron_status'] = "1"
            else:
                item['cron_status'] = "0"
            ls.append(item)
        return ls
    except Exception as e:
        daoManager.rollback()
        logger.error("getCheckCronStatus error occurred", exc_info=e, stack_info=True)
    return None


def getDBCount():
    daomanagerfactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daomanager = daomanagerfactory.getDefaultDaoManager()
    try:
        daomanager.startTransaction()
        dao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        dbvo = dao.getDatabaseInfoByDBName("pgbweb")
        print(dbvo.db_name)
        daomanager.commit()
    except Exception as e:
        daomanager.rollback()

def getCronStatusList(host_name):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = depotdbDao.getCronStatusList(host_name)
        stopls = []
        runningls = []
        for item in list:
            vo = dict(item)
            if "running" in vo['status']:
                runningls.append(vo)
            else:
                stopls.append(vo)
        newlist = stopls+runningls
        daoManager.commit()
        return newlist
    except Exception as e:
        daoManager.rollback()
        logger.error("getCronStatusList error occurred", exc_info=e, stack_info=True)

def save_CronStatus(host_name):
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = depotdbDao.getCronStatusList(host_name)
        if len(list) == 0:
            logger.info("insert")
            depotdbDao.insertCronStatus(host_name)
        else:
            logger.info("update")
            depotdbDao.updateCronStatus(host_name)
        daoManager.commit()
    except Exception as e:
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
        daoManager.rollback()
        logger.error("save_CronStatus error occurred", exc_info=e, stack_info=True)
    return res

def JobForCronStatus():
    logger.info("JobForCronStatus")
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = depotdbDao.get_cronStatusForAlertList()
        logger.info("list:%s" %(len(list)))
        daoManager.commit()
        alert_ls = [dict(vo) for vo in list]
        if len(alert_ls)>0:
            x = PrettyTable()
            title = ["#", "Host name", "Last update time"]
            index = 1
            for data in alert_ls:
                host_name = data['host_name']
                lastupdatetime = data['lastupdatetime']
                line_content = [index, host_name, lastupdatetime]
                x.add_row(line_content)
                index += 1
            x.field_names = title
            x.format = True
            emailcontent = x.get_html_string(attributes={"id": "my_table", "class": "table table-hover"})
            msg = wbxemailmessage(emailtopic=wbxemailtype.EMAILTYPE_CRONJOB_MONITOR, emailcontent=emailcontent,
                                  receiver="lexing@cisco.com", emailformat=wbxemailtype.EMAIL_FORMAT_HTML)
            sendemail(msg)
    except Exception as e:
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
        daoManager.rollback()
        logger.error("JobForCronStatus error occurred", exc_info=e, stack_info=True)
    return res
