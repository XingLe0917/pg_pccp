import logging
import datetime
import uuid
import requests
from common.wbxutil import wbxutil

from dao.vo.wbxmonitoralertdetailVo import WbxmonitoralertdetailVo
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys

logger = logging.getLogger("DBAMONITOR")

def add_wbxmonitoralertdetail(**kwargs):
    logger.info("add_wbxmonitoralertdetail, kwargs=%s" % (kwargs))
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    pd = ""
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        result = getWbxmonitoralertdetailVo(**kwargs)
        if result['status'] == "SUCCESS":
            wbxmonitoralertdetailVo = result['data']
            pd = result['pd']
            if pd and "1"==pd:
                send_PD(result)
            dao.add_alertdetail(wbxmonitoralertdetailVo)
            daoManager.commit()
        else:
            return result
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def getWbxmonitoralertdetailVo(**kwargs):
    res = {"status": "SUCCESS", "errormsg": "", "data": None,"pd":""}
    if "task_type" not in kwargs or kwargs['task_type'] =="":
        res['status'] = 'FAILED'
        res['errormsg'] = 'Do not find task_type in parameters or task_type is null'
        return res
    task_type = kwargs['task_type']
    db_name = ""
    host_name = ""
    splex_port = 0
    parameter = ""
    priority = 10
    job_name = ""
    pd = ""
    if "db_name" in kwargs:
        db_name = kwargs['db_name']
    if "host_name" in kwargs:
        host_name = kwargs['host_name']
        if '.webex.com' in host_name:
            host_name = str(host_name).split(".")[0]
    if "splex_port" in kwargs and kwargs['splex_port']:
        splex_port = kwargs['splex_port']
    if "priority" in kwargs and kwargs['priority']:
        priority = int(kwargs['priority'])
    if "parameter" in kwargs:
        parameter = kwargs['parameter']
    if "pd" in kwargs and kwargs['pd']:
        res['pd'] = kwargs['pd']
    if "job_name" in kwargs and kwargs['job_name']:
        job_name = kwargs['job_name']
    if not db_name and not host_name and not splex_port:
        res['status'] = 'FAILED'
        res['errormsg'] = 'At least one of db_name,host_name,splex_port needs to have a value'
        return res

    alert_title = task_type
    if db_name:
        alert_title += "_" + db_name
    if host_name:
        alert_title += "_" + host_name
    if splex_port:
        alert_title += "_" + str(splex_port)
    jobvo = WbxmonitoralertdetailVo(alertdetailid=uuid.uuid4().hex, alerttitle=alert_title, db_name=db_name,
                                    host_name=host_name,splex_port=splex_port, parameter=parameter,priority=priority,
                                    alert_type=task_type,job_name=job_name)
    res['data'] = jobvo
    return res

def get_wbxmonitoralert_type():
    logger.info("get_wbxmonitoralert_type ")
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        rows = dao.get_wbxmonitoralert_type()
        list = [dict(vo)['alert_type'] for vo in rows]
        res['data'] = list
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        return res
    finally:
        if daoManager is not None:
            daoManager.close()
    return res

def get_wbxmonitoralert(db_name,status,host_name,alert_type,start_date,end_date):
    logger.info("get_wbxmonitoralert")
    res = {"status": "SUCCESS", "errormsg": "","data":None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        date1 = wbxutil.convertStringToDate(end_date)
        end_date_next_day = (date1 + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        rows = dao.getWbxmonitoralert(db_name,status,host_name,alert_type,start_date,end_date_next_day)
        res['data'] = [dict(vo) for vo in rows]
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        return res
    finally:
        if daoManager is not None:
            daoManager.close()
    return res

def get_wbxmonitoralertdetail(alertid):
    logger.info("get_wbxmonitoralertdetail, alertid={0}" .format(alertid))
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        rows = dao.getWbxmonitoralertdetail(alertid)
        res['data'] = [dict(vo) for vo in rows]
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res["status"] = "FAILED"
        res["errormsg"] = str(e)
        return res
    finally:
        if daoManager is not None:
            daoManager.close()
    return res

def send_PD(result):
    logger.info("send pd, %s" .format(result))
    url = "https://events.pagerduty.com/v2/enqueue"
    wbxmonitoralertdetailVo = result['data']
    splex_port = wbxmonitoralertdetailVo.splex_port
    component = "os"
    if splex_port:
        component = "spareplex"
    if wbxmonitoralertdetailVo.db_name:
        component = "db"
    payload = {
        "payload": {
            "summary": "test send pd in IL5 by Events API V2, please ignore it",
            "severity": "critical",
            "source": wbxmonitoralertdetailVo.host_name,  # hostname
            "component": component,  # db os spareplex
            "group": "pgdb",
            "class": "",
            "custom_details": {
                "message": wbxmonitoralertdetailVo.parameter
            }
        },
        "routing_key": "f61b951985a84e09d06e06a9bd0375e5",
        "dedup_key": "",
        "event_action": "trigger",
        "client": wbxmonitoralertdetailVo.job_name,  # jobname
        "client_url": "",  # set
        "links": [
            {
                "href": "http://pagerduty.example.com",  # pccp alert page
                "text": "An example link."
            }
        ]
        # ,
        # "images": [
        #   {
        #     "src": "https://chart.googleapis.com/chart?chs=600x400&chd=t:6,2,9,5,2,5,7,4,8,2,1&cht=lc&chds=a&chxt=y&chm=D,0033FF,0,0,5,1",
        #     "href": "https://google.com",
        #     "alt": "An example link with an image"
        #   }
        # ]
    }
    headers = {"Content-Type": "application/json"}
    response = requests.request("POST", url, json=payload, headers=headers)
    logger.info(response.text)
