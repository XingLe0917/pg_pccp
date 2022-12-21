import json
import logging
import os
from base64 import b64encode
import datetime

from flask import jsonify
from flask_session import Session

from biz.applnpoolinfo import creata_appln_pool_info
from biz.deploydbpatch import deploy_schema
from biz.shareplex import getShareplexInfoList, addShareplexInfoList, addShareplexInfo, get_wbxadbmon
from biz.vault import getVaultPath
from biz.wbxalert import add_wbxmonitoralertdetail, get_wbxmonitoralert_type, get_wbxmonitoralert, \
    get_wbxmonitoralertdetail
from schemas.wbxpartitionjobstatus import QueryParams
from biz.UserUtil import getTokenByCode, getUserByToken, checkUserInPCCP, getTokeninfo, getCCPUser,  getAuthorizationForMA
from biz.backupjobmonitor import save_BackupStatus, getBackupStatusList
from biz.cronjobmonitor import getCheckCronStatus, save_CronStatus, getCronStatusList, \
    JobForCronStatus
from biz.checkChannel import check_single_channel
from biz.dbstatus import get_db_status, get_db_info_list
from schemas.dbstatus import DBStatusQueryParams

from flask import Flask, redirect, Response, request
from flask_apscheduler import APScheduler

from biz.depotdbmanagement import depot_manage_add_DB, depot_manage_add_host, get_depot_manage_list
from biz.permissionmanagement import add_role_to_page_to_depot, get_user_role_dict_from_depot, \
    get_role_page_dict_from_depot, identify_user_access_from_depot, change_role_to_page_to_depot, \
    assign_role_to_user_to_depot, delete_user_from_role_to_depot
from dao.vo.depotdbvo import wbxdatabase, wbxserver
from biz.permissionmanagement import check_login_user_from_depot, get_role_list_from_depot, get_access_dir_from_depot, \
    get_favourit_page_by_username, add_favourite_page, delete_favourite_page, get_health
from biz.partitionjobmonitor import PartitionJobSchema, list_partition_job_status, persist_partition_job_status
# from biz.AWSReport import get_pg_database_summary_tablelist, get_pg_statement_summary_tablelist, \
#     get_pg_checkpoint_performance_tablelist, get_wbxdba_stat_all_tables_tablelist, get_pg_stat_statements_tablelist, \
#     get_pg_vacuum_stat_tablelist, get_pg_stat_bgwriter_tablelist
# from biz.Homepage import get_homepage_db_version_count, get_homepage_db_count , get_homepage_db_type_count , get_shareplex_count, get_rencent_alert_info, get_top_active_session_db_count
from biz.cmcrestworker import get_dbcommon_list
from biz.depotdbManagement.oracle.applnPoolInfo import create_appln_pool_info, list_appln_pool_info
from biz.depotdbManagement.oracle.databaseInfo import create_database_info
from biz.depotdbManagement.oracle.hostInfo import create_host_info
from biz.depotdbManagement.oracle.instanceInfo import create_instance_info
from biz.depotdbManagement.oracle.shareplexInfo import create_shareplex_info, list_shareplex_info
from biz.depotdbManagement.oracle.applnPoolInfo import (
    create_appln_pool_info,
    list_appln_pool_info,
    list_depotdb_appln_pool_infos as list_oracle_appln_pool_infos
)
from biz.depotdbManagement.oracle.databaseInfo import (
    create_database_info,
    list_depotdb_database_info as list_oracle_database_infos
)
from biz.depotdbManagement.oracle.hostInfo import (
    create_host_info,
    list_depotdb_host_info as list_oracle_host_infos
)
from biz.depotdbManagement.oracle.instanceInfo import (
    create_instance_info,
    list_depotdb_instance_info as list_oracle_instance_infos
)
from biz.depotdbManagement.oracle.shareplexInfo import (
    create_shareplex_info,
    list_shareplex_info,
    list_depotdb_shareplex_info as list_oracle_shareplex_infos
)
from biz.depotdbManagement.pg.shareplexInfo import list_depotdb_shareplex_info
from biz.depotdbManagement.pg.applnPoolInfo import list_depotdb_appln_pool_info
from biz.depotdbManagement.pg.databaseInfo import list_depotdb_database_info
from biz.depotdbManagement.pg.hostInfo import list_depotdb_host_info
from biz.cmcrestworker import get_dbcommon_list
from biz.applnmappinginfo import create_appln_mapping_info

class Config():
    SCHEDULER_API_ENABLED = True

app = Flask(__name__)
app.secret_key = 'my unobvious secret key'
app.config.from_pyfile("config.cfg")
app.config.from_object(Config())
# initialize scheduler
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()
Session(app)

logger = logging.getLogger("DBAMONITOR")

scopes=["identity:myprofile_read","sms_identity|sms_api", "other_identity|other_api", "front_identity|front_api"]
whitelist=["/api/testapi","/api/getAuthorizationForMA","/api/get_role_list"]

@app.before_request
def myredirect():
    logger.info("myredirect")
    # if request.path.startswith('/api') and request.path not in whitelist:
    #     auth = request.authorization
    #     if auth:
    #         tok = b64encode((auth.username + ':' + auth.password).encode('utf-8')).decode('utf-8')
    #         if tok == "YXBwOlczNkpNJUYwdlR1WQ==":
    #             logger.info("call api with fixed token")
    #         else:
    #             # api authentication with CI
    #             try:
    #                 flag = check_auth(auth.username, auth.password, request.path)
    #                 if not flag:
    #                     return not_authenticated()
    #             except Exception as e:
    #                 logger.error("check_auth error,auth={0}".format(auth))
    #     else:
    #         logger.error("no authorization")
    #         return not_authenticated()

def check_auth(username,password,path):
    logger.info("check_auth, username={0}, path={1}".format(username,path))
    resDict = getTokeninfo(password)
    logger.info("resDict={0}" .format(resDict))
    if 'scope' in resDict:
        scope=resDict['scope'][0]
        if scope in scopes:
            if 'user_id' in resDict:
                user_id = resDict['user_id']
                logger.info("user_id={0}, scope={1}, path={2}".format(user_id, scope, path))
                user_name = str(user_id).split("@")[0]
                if 'machine_type' in resDict and str(resDict['machine_type'])=='bot':
                    return True
                else:
                    flag = checkUserInPCCP(user_name)
                    if flag:
                        return True
                    else:
                        logger.info("{0} is not PCCP user, please contact DBA team.".format(user_name))
                        return False
            else:
                logger.info("call api from other")
                return True
        else:
            return False
    else:
        logger.info("no scope.")
        return False

def not_authenticated():
    return Response(
    'Could not verify your access level for that URL.\n'
    'You have to login with proper credentials', 401,
    {'WWW-Authenticate': 'Basic realm="Login Required"'})

@app.route("/loginRedirection", methods=['GET', 'POST'])
def loginRedirection():
    logger.info("loginRedirection")
    logger.info(request.args)
    code = request.args.get("code")
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    pccp_config_file = os.path.join(PROJECT_ROOT, "conf/","pccp.json")
    logger.info("pccp_config_file:%s" % (pccp_config_file))
    f = open(pccp_config_file, "r")
    configDict = json.load(f)
    deploy_ip = configDict['deploy_ip']
    logger.info("deploy_ip=%s" %(deploy_ip))
    PCCP_INDEX_URL = "https://%s/#/home/index" %(deploy_ip)
    PCCP_ERROR_URL = "https://%s/#/401" %(deploy_ip)
    cec = request.args.get("cec")
    fullName = request.args.get("fullName")
    if code:
        response = getTokenByCode(code)
        logger.info(response)
        access_token = response['access_token']
        r = getUserByToken(access_token)
        if r.status_code == 200:
            user = r.json()
            userName = user['userName']
            logger.info(userName)
            cec = str(userName).split("@")[0]
            fullName = str(user['name']['givenName']) +" "+str(user['name']['familyName'])
            logger.info(fullName)
            flag = checkUserInPCCP(cec)
            if flag:
                logger.info("check userName={0} is OK." .format(userName))
                # api authentication with CI
                token = 'Basic ' + b64encode((cec + ':' + access_token).encode('utf-8')).decode('utf-8')
                redirect_url = PCCP_INDEX_URL + "?user=" + cec + "&token=" + token + "&fullName=" + fullName
                logger.info("redirect_url={0}" .format(redirect_url))
                return redirect(redirect_url)
            else:
                message = "The userName={0} not in PCCP user role" .format(userName)
                logger.info(message)
                return redirect(PCCP_ERROR_URL+"?user="+cec)
        else:
            logger.error("Error: get user by access_token")
            return redirect(PCCP_ERROR_URL)
    elif cec and fullName:
        logger.info("cec=%s" %(cec))
        logger.info("fullName=%s" %(fullName))
        token = 'Basic ' + "YXBwOlczNkpNJUYwdlR1WQ=="
        redirect_url = PCCP_INDEX_URL + "?user=" + cec + "&token=" + token + "&fullName=" + fullName
        logger.info("redirect_url={0}" .format(redirect_url))
        return redirect(redirect_url)
    else:
        logger.error("Do not get code.")
        return redirect(PCCP_ERROR_URL)

# @scheduler.task('cron', id='do_job_1', minute='*/5')
# def cronStatusForAlert():
#     logger.info("Job cronStatusForAlert executed")
#     JobForCronStatus()

@app.route("/api/getAuthorizationForMA", methods=['POST'])
def authorizationForMA():
    if request.method == 'POST':
        res = {"status": "SUCCESS", "errormsg": "", "authorization": None}
        res["authorization"] = "Basic YXBwOlczNkpNJUYwdlR1WQ=="
        # return jsonify(getAuthorizationForMA())
        return jsonify(res)

@app.route("/api/get_user_role_dict", methods=['GET', 'POST'])
def get_user_role_dict():
    return json.dumps(get_user_role_dict_from_depot())


@app.route("/api/get_role_page_dict", methods=['POST'])
def get_role_page_dict():
    if request.method == 'POST':
        url_list = request.json["urlArray"]
    return json.dumps(get_role_page_dict_from_depot(url_list))


@app.route("/api/identify_user_access", methods=['POST'])
def identify_user_access():
    if request.method == 'POST':
        username = request.json["username"].strip()
        logger.info("username:%s" %(username))
    else:
        logger.info(request)
    return json.dumps(identify_user_access_from_depot(username))


@app.route("/api/change_role_to_page", methods=['POST'])
def change_role_to_page():
    if request.method == 'POST':
        route_list = request.json["routeArray"]
    return json.dumps(change_role_to_page_to_depot(route_list))


@app.route("/api/add_role_to_page", methods=['POST'])
def add_role_to_page():
    if request.method == 'POST':
        role_name = request.json["roleName"].strip()
    return json.dumps(add_role_to_page_to_depot(role_name))


@app.route("/api/assign_role_to_user", methods=['POST'])
def assign_role_to_user():
    if request.method == 'POST':
        username = request.json["userName"].strip()
        role_name = request.json["roleName"].strip()
        return json.dumps(assign_role_to_user_to_depot(username, role_name))


@app.route("/api/delete_user_from_role", methods=['POST'])
def delete_user_from_role():
    if request.method == 'POST':
        username = request.json["userName"].strip()
        role_name = request.json["roleName"].strip()
        return json.dumps(delete_user_from_role_to_depot(username, role_name))

@app.route("/api/checkCronStatus", methods=['POST'])
def checkCronStatus():
    if request.method == 'POST':
        return jsonify(getCheckCronStatus())

@app.route("/api/get_ccp_user", methods=['GET', 'POST'])
def getccpuser():
    if request.method == 'POST':
        return jsonify(getCCPUser())

@app.route("/api/testapi", methods=['GET', 'POST'])
def checkTestApi():
    return {"result": "SUCCESS", "msg": "ok"}

@app.route("/api/cronStatusList", methods=['POST'])
def cronStatusList():
    if request.method == 'POST':
        host_name = request.json["host_name"].strip().lower()
        return jsonify(getCronStatusList(host_name))

@app.route("/api/saveCronStatus", methods=['POST'])
def saveCronStatus():
    if request.method == 'POST':
        host_name = request.json["host_name"].strip().lower()
        return save_CronStatus(host_name)

# @app.route("/api/cronStatusForAlert", methods=['GET', 'POST'])
# def cronStatusForAlert():
#     logger.info("Job cronStatusForAlert executed")
#     return jsonify(JobForCronStatus())

@app.route("/api/saveBackupStatus", methods=['POST'])
def saveBackupStatus():
    if request.method == 'POST':
        host_name = request.json["host_name"].strip().lower()
        db_name = request.json["db_name"].strip().lower()
        full_backup_status = request.json["full_backup_status"]
        incr_backup_status = request.json["incr_backup_status"]
        return save_BackupStatus(host_name,db_name,full_backup_status,incr_backup_status)

@app.route("/api/backupStatusList", methods=['POST'])
def backupStatusList():
    if request.method == 'POST':
        host_name = request.json["host_name"].strip().lower()
        return jsonify(getBackupStatusList(host_name))

@app.route("/api/depot_manage_add_db", methods=['POST'])
def depotManage_add_DB():
    if request.method == 'POST':
        database_info_vo = wbxdatabase
        database_info_vo.db_name = str(request.json["db_name"]).lower()
        database_info_vo.host_name = request.json["host_name"]
        database_info_vo.cluster_name = request.json["cluster_name"]
        database_info_vo.db_vendor = request.json["db_vendor"]
        database_info_vo.db_version = request.json["db_version"]
        database_info_vo.db_type = request.json["db_type"]
        database_info_vo.application_type = request.json["application_type"]
        database_info_vo.appln_support_code = str(request.json["appln_support_code"]).upper()
        database_info_vo.db_home = request.json["db_home"]
        database_info_vo.listener_port = request.json["listener_port"]
        database_info_vo.monitored = request.json["monitored"]
        database_info_vo.wbx_cluster = request.json["wbx_cluster"]
        database_info_vo.web_domain = request.json["web_domain"]
        return jsonify(depot_manage_add_DB(database_info_vo))

@app.route("/api/depot_manage_add_host", methods=['POST'])
def depotManage_add_host():
    if request.method == 'POST':
        host_info_vo = wbxserver
        host_info_vo.cname = request.json["cname"]
        host_info_vo.host_name = request.json["host_name"]
        host_info_vo.domain = request.json["domain"]
        host_info_vo.site_code = request.json["site_code"]
        host_info_vo.region_name = request.json["region_name"]
        host_info_vo.public_ip = request.json["public_ip"]
        host_info_vo.private_ip = request.json["private_ip"]
        host_info_vo.os_type_code = request.json["os_type_code"]
        host_info_vo.processor = request.json["processor"]
        host_info_vo.kernel_release = request.json["kernel_release"]
        host_info_vo.hardware_platform = request.json["hardware_platform"]
        host_info_vo.physical_cpu = request.json["physical_cpu"]
        host_info_vo.cores = request.json["cores"]
        host_info_vo.cpu_model = request.json["cpu_model"]
        host_info_vo.flag_node_virtual = request.json["flag_node_virtual"]
        host_info_vo.comments = request.json["comments"]
        host_info_vo.ssh_port = request.json["ssh_port"]
        # host_info_vo.install_date = datetime.datetime.strptime(request.json["install_date"], "%Y-%m-%dT%H:%M:%SZ")
        host_info_vo.install_date = datetime.datetime.strptime(request.json["install_date"], "%Y-%m-%dT%H:%M:%SZ")
        return jsonify(depot_manage_add_host(host_info_vo))

@app.route("/api/depot_manage_list", methods=['POST'])
def depot_manage_list():
    if request.method == 'POST':
        host_name = request.json["host_name"]
        db_name = request.json["db_name"]
        cname = request.json["cname"]
        return jsonify(get_depot_manage_list(host_name,db_name,cname))


@app.route("/api/check_login_user", methods=['POST'])
def check_login_user():
    if request.method == 'POST':
        username = request.json["username"]
        return jsonify(check_login_user_from_depot(username))


@app.route("/api/get_role_list", methods=['GET', 'POST'])
def get_role_list():
    return jsonify(get_role_list_from_depot())


@app.route("/api/get_access_dir", methods=['POST'])
def get_access_dir():
    if request.method == 'POST':
        username = request.json["username"]
    return jsonify(get_access_dir_from_depot(username))


@app.route("/api/get_favourit_page_by_username", methods=['POST'])
def getfavouritpagebyusername():
    username = request.json["username"]
    return jsonify(get_favourit_page_by_username(username))


@app.route("/api/add_favourite_page", methods=['POST'])
def addfavouritepage():
    username = request.json["username"]
    page_name = request.json["page_name"]
    url = request.json["url"]
    return jsonify(add_favourite_page(username, page_name, url))


@app.route("/api/delete_favourite_page", methods=['POST'])
def deletefavouritepage():
    username = request.json["username"]
    page_name = request.json["page_name"]
    url = request.json["url"]
    return jsonify(delete_favourite_page(username, page_name, url))


@app.route("/api/health", methods=['POST', 'GET'])
def gethealth():
    return get_health()

@app.route("/api/partition_job_status", methods=["GET"])
def api_partition_job_status_list():
    """
    pagination job status info list
    query_params:
        - host_name
        - db_name
        - add_status
        - drop_status
    """
    query_dict = QueryParams(**dict(request.args))
    data = list_partition_job_status(params=query_dict.dict(exclude_unset=True))
    return jsonify({"status": "SUCCESS", "data": data})

@app.route("/api/partition_job_status", methods=["POST"])
def api_persist_partition_job_status():
    data = PartitionJobSchema(**request.json)
    persist_partition_job_status(data=data)
    return jsonify({"status": "SUCCESS"})

@app.route("/api/getChannelList", methods=['POST', 'GET'])
def get_shareplex_info_list():
    return jsonify(getShareplexInfoList())

@app.route("/api/addChannelList", methods=['POST'])
def add_shareplex_info_list():
    if request.method == 'POST':
        src_host = request.json["src_host"]
        src_db = request.json["src_db"]
        port = request.json["port"]
        tgt_host = request.json["tgt_host"]
        tgt_db = request.json["tgt_db"]
        qname = request.json["qname"]
        src_splex_sid = request.json["src_splex_sid"]
        tgt_splex_sid = request.json["tgt_splex_sid"]
        src_schema = request.json["src_schema"]
        tgt_schema = request.json["tgt_schema"]
        return jsonify(addShareplexInfoList(src_host, src_db, port, tgt_host, tgt_db, qname,
                                 src_splex_sid, tgt_splex_sid, src_schema, tgt_schema))

@app.route("/api/get_db_status", methods=["GET"])
def api_get_db_status():
    params = DBStatusQueryParams(**dict(request.args))
    result = get_db_status(params=params)
    return jsonify(result)

@app.route("/api/db_info_list", methods=["GET"])
def api_get_db_info_list():
    return jsonify(get_db_info_list())

# for test
@app.route("/api/get_appln_pool_info", methods=["GET"])
def api_get_appln_pool_info():
    from biz.applnpoolinfo import get_appln_pool_info
    result = get_appln_pool_info()
    return jsonify({"status": "SUCCEED", "data": result})


@app.route("/api/create_appln_pool_info", methods=["POST"])
def api_create_appln_pool_info():
    if request.method == 'POST':
        db_name = request.json["db_name"]
        appln_support_code = request.json["appln_support_code"]
        schemaname = request.json["schemaname"]
        password = request.json["password"]
        password_vault_path = request.json["password_vault_path"]
        created_by = request.json["created_by"]
        modified_by = request.json["modified_by"]
        schematype = request.json["schematype"]
        return jsonify(creata_appln_pool_info(db_name,appln_support_code,schemaname,password,password_vault_path,created_by,modified_by,schematype))

@app.route("/api/addChannelInfo", methods=['POST'])
def add_shareplex_info():
    if request.method == 'POST':
        src_db = request.json["src_db"]
        tgt_db = request.json["tgt_db"]
        port = request.json["port"]
        return jsonify(addShareplexInfo(str(src_db).lower(), str(tgt_db).lower(),port))

@app.route("/api/getVaultPath", methods=['POST'])
def get_VaultPath():
    if request.method == 'POST':
        db_name = request.json["db_name"]
        schema = request.json["schema"]
        return jsonify(getVaultPath(db_name,schema))

# @app.route("/api/get_pg_database_summary_info", methods=["POST"])
# def api_get_pg_database_summary_info():
#     db_name = request.json["db_name"]
#     start_time = request.json['start_time']
#     end_time = request.json['end_time']
#     return jsonify(get_pg_database_summary_tablelist(db_name,start_time,end_time))
#
# @app.route("/api/get_pg_statement_summary_info", methods=["GET","POST"])
# def api_get_pg_statement_summary_info():
#     db_name = request.json["db_name"]
#     start_time = request.json['start_time']
#     end_time = request.json['end_time']
#     return jsonify(get_pg_statement_summary_tablelist(db_name,start_time,end_time))
#
# @app.route("/api/get_pg_checkpoint_performance_info", methods=["GET","POST"])
# def api_get_pg_checkpoint_performance_info():
#     db_name = request.json["db_name"]
#     start_time = request.json['start_time']
#     end_time = request.json['end_time']
#     return jsonify(get_pg_checkpoint_performance_tablelist(db_name,start_time,end_time))
#
# @app.route("/api/get_wbxdba_stat_all_tables_info", methods=["GET","POST"])
# def api_get_wbxdba_stat_all_tables_info():
#     db_name = request.json["db_name"]
#     start_time = request.json['start_time']
#     end_time = request.json['end_time']
#     return jsonify(get_wbxdba_stat_all_tables_tablelist(db_name,start_time,end_time))
#
# @app.route("/api/get_pg_stat_statements_info", methods=["GET","POST"])
# def api_get_pg_stat_statements_info():
#     db_name = request.json["db_name"]
#     start_time = request.json['start_time']
#     end_time = request.json['end_time']
#     return jsonify(get_pg_stat_statements_tablelist(db_name,start_time,end_time))
#
# @app.route("/api/get_pg_vacuum_stat_info", methods=["GET","POST"])
# def api_get_pg_vacuum_stat_info():
#     db_name = request.json["db_name"]
#     start_time = request.json['start_time']
#     end_time = request.json['end_time']
#     return jsonify(get_pg_vacuum_stat_tablelist(db_name,start_time,end_time))
#
# @app.route("/api/get_pg_stat_bgwriter_info", methods=["GET","POST"])
# def api_get_pg_stat_bgwriter_info():
#     db_name = request.json["db_name"]
#     start_time = request.json['start_time']
#     end_time = request.json['end_time']
#     return jsonify(get_pg_stat_bgwriter_tablelist(db_name,start_time,end_time))

@app.route("/api/wbxadbmon", methods=["POST"])
def api_wbxadbmon_list():
    src_db, tgt_db, port = None, None, None
    if request.json:
        src_db = request.json.get("src_db", None)
        tgt_db = request.json.get("tgt_db", None)
        port = request.json.get("port", None)
    results = get_wbxadbmon(src_db=src_db, tgt_db=tgt_db, port=port)
    return jsonify({"status": "SUCCESS", "errormsg": "", "data": results})


@app.route("/api/wbxadbmon/check", methods=["POST"])
def api_check_replication_channel():
    if not request.json:
        return jsonify({"status": "FAILED", "errormsg": "empty request paras", "data": []})
    src_host = request.json.get("src_host", None)
    src_db = request.json.get("src_db", None)
    port = request.json.get("port", None)
    tgt_host = request.json.get("tgt_host", None)
    tgt_db = request.json.get("tgt_db", None)
    res = check_single_channel(src_host=src_host, src_db=src_db,port=port,tgt_host=tgt_host, tgt_db=tgt_db)
    return jsonify(res)

# database info
@app.route("/api/oracleDatabaseInfo/create", methods=["POST"])
def api_oracle_create_database_info():
    if request.json:
        res = create_database_info(request.json)
        return jsonify(res)
    return jsonify({"status": "FAILED", "msg": "empty paras", "data": ""})


# host info
@app.route("/api/oracleHostInfo/create", methods=["POST"])
def api_oracle_create_host_info():
    if request.json:
        res = create_host_info(request.json)
        return jsonify(res)
    return jsonify({"status": "FAILED", "msg": "empty paras", "data": ""})


# instance info
@app.route("/api/oracleInstanceInfo/create", methods=["POST"])
def api_oracle_create_instance_info():
    if request.json:
        res = create_instance_info(request.json)
        return jsonify(res)
    return jsonify({"status": "FAILED", "msg": "empty paras", "data": ""})


# appln pool info
@app.route("/api/oracleApplnPoolInfo/create", methods=["POST"])
def api_oracle_create_appln_pool_info():
    if request.json:
        res = create_appln_pool_info(request.json)
        return jsonify(res)
    return jsonify({"status": "FAILED", "msg": "empty paras", "data": ""})

@app.route("/api/oracleApplnPoolInfos", methods=["POST"])
def api_oracle_list_appln_pool_info():
    if request.json:
        res = list_appln_pool_info(**request.json)
    else:
        res = list_appln_pool_info()
    return jsonify(res)


# shareplex info
@app.route("/api/oracleShareplexInfo/create", methods=["POST"])
def api_oracle_create_shareplex_info():
    if request.json:
        res = create_shareplex_info(request.json)
        return jsonify(res)
    return jsonify({"status": "FAILED", "msg": "empty paras", "data": ""})


@app.route("/api/oracleShareplexInfos", methods=["POST"])
def api_oracle_list_shareplex_info():
    if request.json:
        res = list_shareplex_info(**request.json)
    else:
        res = list_shareplex_info()
    return jsonify(res)

@app.route("/api/postgresqlDepotDBManagementInfos", methods=["POST"])
def api_postgresql_list_depotdb_management_info():
    res = {
        "host_info": [],
        "database_info": [],
        "appln_pool_info": [],
        "shareplex_info": []
    }
    if request.json:
        res["database_info"] = list_depotdb_database_info(**request.json)
        res["host_info"] = list_depotdb_host_info(**request.json)
        res["appln_pool_info"] = list_depotdb_appln_pool_info(**request.json)
        res["shareplex_info"] = list_depotdb_shareplex_info(**request.json)

    return jsonify(res)

@app.route("/api/oracleDepotDBManagementInfos", methods=["POST"])
def api_oracle_list_depotdb_management_info():
    res={
        "host_info": [],
        "database_info": [],
        "appln_pool_info": [],
        "instance_info": [],
        "shareplex_info": [],
    }
    if request.json:
        res["host_info"] = list_oracle_host_infos(**request.json)
        res["database_info"] = list_oracle_database_infos(**request.json)
        res["instance_info"] = list_oracle_instance_infos(**request.json)
        res["shareplex_info"] = list_oracle_shareplex_infos(**request.json)
        res["appln_pool_info"] = list_oracle_appln_pool_infos(**request.json)

    return jsonify(res)


@app.route("/api/add_wbxmonitoralertdetail", methods=['POST'])
def addWbxmonitoralertdetail():
    if request.method == 'POST':
        kwargs = request.json
        return jsonify(add_wbxmonitoralertdetail(**kwargs))

@app.route("/api/get_wbxmonitoralert_type", methods=['POST', 'GET'])
def getWbxmonitoralerttype():
    return jsonify(get_wbxmonitoralert_type())

@app.route("/api/get_wbxmonitoralert", methods=['POST', 'GET'])
def getWbxmonitoralert():
    if request.method == 'POST':
        status = ""
        if "status" in request.json and request.json["status"] != "ALL":
            status = request.json["status"]
        db_name = request.json["db_name"]
        host_name = request.json["host_name"]
        alert_type = request.json["alert_type"]
        start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).date().strftime('%Y-%m-%d')
        end_date = datetime.datetime.now().date().strftime('%Y-%m-%d')
        if "start_date" in request.json and "end_date" in request.json:
            start_date = request.json["start_date"]
            end_date = request.json["end_date"]
        return jsonify(get_wbxmonitoralert(db_name,status,host_name,alert_type,start_date,end_date))

@app.route("/api/get_wbxmonitoralertdetail", methods=['POST'])
def getWbxmonitoralertdetail():
    if request.method == 'POST':
        alertid = request.json["alertid"]
        return jsonify(get_wbxmonitoralertdetail(alertid))

@app.route("/api/applnMappingInfo/create", methods=["POST"])
def api_create_appln_mapping_info():
    if request.json:
        data = create_appln_mapping_info(request.json)
        return jsonify(data)
    return jsonify({"status": "FAILED", "msg": "empty payload", "data": []})

@app.route("/api/v1/profile", methods=["POST"])
def api_getdbcommon_list():
    if not request.json:
        return jsonify({"status": "FAILED", "errormsg": "empty request paras", "data": []})
    db_type = request.json.get("envType", None)
    appln_support_code = request.json.get("appCode", None)
    application_type = request.json.get("appType", None)
    webdomain = request.json.get("webdomain", None)
    pool = request.json.get("pool", None)
    schematype = request.json.get("schematype", "all")
    kargs={
        "db_type":db_type,
        "appln_support_code" : appln_support_code,
        "application_type" : application_type,
        "webdomain" : webdomain,
        "pool" : pool,
        "schematype": schematype
    }
    res = get_dbcommon_list(**kargs)
    return jsonify(res)


# # get_homepage_server_count, get_homepage_db_count, get_homepage_db_type_count, get_shareplex_count, get_rencent_alert_info, get_top_active_session_db_count
# @app.route("/api/get-homepage-db-version-count", methods=['POST', 'GET'])
# def gethomepagedbversioncount():
#     return jsonify(get_homepage_db_version_count())
#
#
# @app.route("/api/get-homepage-db-count", methods=['POST', 'GET'])
# def gethomepagedbcount():
#     return jsonify(get_homepage_db_count())
#
#
# @app.route("/api/get-homepage-db-type-count", methods=['POST', 'GET'])
# def gethomepagedbtypecount():
#     return jsonify(get_homepage_db_type_count())
#
#
# @app.route("/api/get-shareplex-count", methods=['POST', 'GET'])
# def getshareplexcount():
#     return jsonify(get_shareplex_count())
#
#
# @app.route("/api/get-rencent-alert-info", methods=['POST', 'GET'])
# def getrencentalertinfo():
#     return jsonify(get_rencent_alert_info())
#
#
# @app.route("/api/get-top-active-session-db-count", methods=['POST', 'GET'])
# def gettopactivesessiondbcount():
#     return jsonify(get_top_active_session_db_count())



@app.route("/api/deploydbSchema", methods=['POST'])
def deploySchema():
    if request.method == 'POST':
        c_schemaType = request.json["c_schemaType"]
        c_SchemaName = request.json["c_SchemaName"]
        c_deployStep = request.json["c_deployStep"]
        envType = request.json["envType"]
        webdomain = request.json["webdomain"]
        hostName = request.json["hostName"]
        return jsonify(deploy_schema(c_schemaType,c_SchemaName,c_deployStep,envType,webdomain,hostName))

def loadFlask():
    app.run(host='0.0.0.0', port=9000, debug=False)


