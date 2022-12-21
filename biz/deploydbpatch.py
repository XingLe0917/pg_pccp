import logging

from biz.cmcrestworker import get_dbcommon_list
from biz.dbstatus import CallDB
from common.wbxdbconnection import wbxdbconnection

logger = logging.getLogger("DBAMONITOR")

def deploy_schema(c_schemaType,c_SchemaName,c_deployStep,envType,webdomain,hostName):
    # {"envType": "{{envType}}", "appCode": "WEB", "webdomain": "{{webDomainName}}", "schemaType": "all"}
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    logger.info("deploy_schema, c_schemaType=%s, c_SchemaName=%s, c_deployStep=%s, envType=%s, webdomain=%s, hostName=%s" % (
    c_schemaType, c_SchemaName, c_deployStep, envType, webdomain,hostName))
    kargs = {
        "db_type": envType,
        "appln_support_code": "WEB",
        "application_type": None,
        "webdomain": webdomain,
        "pool": None,
        "schematype": "all"
    }

    PG_WebDBWithMultiSchema = get_dbcommon_list(**kargs)
    for db in PG_WebDBWithMultiSchema:
        print(db)
        d_schemaType = db['schematype']
        d_schemaName = db['schemaname']
        if d_schemaType == c_schemaType and d_schemaName == c_SchemaName:
            # c_dbName = db['dbname']
            # c_UserName = db['username']
            # c_PassWord = db['password']
            # c_connect_ip = db['private_ip']
            # c_port_number = db['port']
            c_dbName = "gfewd"
            c_UserName = "test"
            c_PassWord = "il5test"
            c_connect_ip = db['private_ip']
            c_port_number = "5432"
            # c_number =$(($c_number+1))
            checkConnResult = precheck_dbinfo(c_connect_ip, c_port_number, c_UserName, c_dbName, c_PassWord,c_SchemaName,hostName)
            if checkConnResult!="OK":
                errormsg = "Failure: Fail to check DB connection for [%s] [%s]" %(c_dbName,c_UserName)
                logger.error(errormsg)
                res['status'] = "FAILED"
                res['errormsg'] = errormsg
                return res

def orchestrate(c_connect_ip,c_port_number,c_UserName,c_dbName,c_PassWord,c_SchemaName):
    # ===== 2. /tmp/pgdascript/dbpatch/orchestrate.sh  192.168.190.29 5432 test gfewd il5test qa WEBDB app /tmp/50002/release.xml 1 test =====
    logger.info(
        "==== orchestrate, c_connect_ip=%s, c_port_number=%s, c_UserName=%s, c_dbName=%s, c_PassWord=%s, c_SchemaName=%s ==== " % (
            c_connect_ip,c_port_number,c_UserName,c_dbName,c_PassWord,c_SchemaName))


def precheck_dbinfo(c_connect_ip, c_port_number, c_UserName, c_dbName, c_PassWord,c_SchemaName,hostName):
    # ===== 1. /tmp/pgdascript/common/precheck_dbinfo.sh  192.168.190.29 5432 test gfewd il5test test =====
    logger.info("==== precheck_dbinfo, c_connect_ip=%s, c_port_number=%s, c_UserName=%s, c_dbName=%s, c_PassWord=%s, c_SchemaName=%s, hostName=%s ==== " % (
        c_connect_ip, c_port_number, c_UserName, c_dbName, c_PassWord,c_SchemaName,hostName))

    status = "SUCCESS"
    try:
        dbconnect = wbxdbconnection(c_UserName, c_PassWord, c_dbName, c_connect_ip, c_port_number)
        dbconnect.connect()
        dbconnect.startTransaction()
        sql = "set search_path=%s,oracle,wbxcommon,public,pg_catalog; select 'OK' from dual;" % (c_SchemaName)
        rows = dbconnect.execute(sql)
        print(rows)
        dbconnect.commit()
    except Exception as e:
        logger.error("Failed to precheck DB Info! e=%s" %(e))
        status = "FAILED"
    return status



