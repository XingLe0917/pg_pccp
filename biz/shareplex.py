import logging

from dao.wbxdaomanager import wbxdaomanagerfactory
from dao.wbxdaomanager import DaoKeys
from dateutil.parser import parse

logger = logging.getLogger("DBAMONITOR")

def getShareplexInfoList():
    logger.info("getShareplexInfoList")
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        list = dao.get_shareplexInfoList()
        key_dict = {}
        for vo in list:
            item = dict(vo)
            key = item['src_db'] + ":" + item['tgt_db'] + ":" + str(item['port'])
            if key in key_dict:
                ls = key_dict[key]
                ls.append(item)
                key_dict[key] = ls
            else:
                key_dict[key] = []
                key_dict[key].append(item)
        data_list = []
        for k, ls in key_dict.items():
            data = {}
            data['environment'] = ""
            data['src_vault_path'] = ls[0]['src_vault_path']
            data['tgt_vault_path'] = ls[0]['tgt_vault_path']
            data['src_site_code'] = ls[0]['src_site_code']
            data['tgt_site_code'] = ls[0]['tgt_site_code']
            data['publication_name'] = "repl_publication"
            data['queue_name'] = ls[0]['queue_name']
            data['src_db'] = k.split(":")[0]
            data['src_web_domain'] = ls[0]['src_web_domain']
            data['src_db_type'] = ls[0]['src_db_type']
            data['src_application_type'] = ls[0]['src_application_type']
            data['src_appln_support_code'] = ls[0]['src_appln_support_code']
            data['tgt_db'] = k.split(":")[1]
            data['tgt_web_domain'] = ls[0]['tgt_web_domain']
            data['tgt_db_type'] = ls[0]['tgt_db_type']
            data['tgt_application_type'] = ls[0]['tgt_application_type']
            data['tgt_appln_support_code'] = ls[0]['tgt_appln_support_code']
            port = k.split(":")[2]
            data['port'] = port
            schemas = []
            for index, v in enumerate(ls):
                schema_obj = {}
                schema_obj['src_schema'] = v['src_schema']
                schema_obj['src_schematype'] = v['src_schematype']
                schema_obj['tgt_schema'] = v['tgt_schema']
                schema_obj['tgt_schematype'] = v['tgt_schematype']
                monitor_table = "rep_monitor_adb_%s_default" %(port)
                monitor_table_ls = []
                monitor_table_ls.append(monitor_table)
                schema_obj['repl_list_table_name'] = ""
                schema_obj['tables'] = monitor_table_ls
                schemas.append(schema_obj)
            data['schemas'] = schemas
            data_list.append(data)
        res['data'] = data_list
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def addShareplexInfoList(src_host, src_db, port, tgt_host, tgt_db, qname,
                             src_splex_sid, tgt_splex_sid, src_schema, tgt_schema):
    logger.info(
        "addShareplexInfoList, src_host=%s,src_db=%s,port=%s,tgt_host=%s,tgt_db=%s,qname=%s,src_splex_sid=%s,tgt_splex_sid=%s,src_schema=%s,tgt_schema=%s" % (
        src_host, src_db, port, tgt_host, tgt_db, qname,
        src_splex_sid, tgt_splex_sid, src_schema, tgt_schema))
    res = {"status": "SUCCESS", "errormsg": ""}
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        dao.add_shareplexInfoList(src_host, src_db, port, tgt_host, tgt_db, qname,
                             src_splex_sid, tgt_splex_sid, src_schema, tgt_schema)
        daoManager.commit()
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def addShareplexInfo(src_db,tgt_db,port):
    res = {"status": "SUCCESS", "msg": "", "errormsg": ""}
    logger.info("addShareplexInfo, src_db=%s,tgt_db=%s,port=%s" % (src_db, tgt_db, port))
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    qname = "default"
    src_splex_sid = "dcass"
    tgt_splex_sid = "dcass"
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        channel_info = dao.getReplicationChannel(src_db,tgt_db,port)
        if len(channel_info)>0:
            res['msg'] = "The channel has exist in depot."
            return res
        src_db_info = dao.getDBInfo(src_db)
        tgt_db_info = dao.getDBInfo(tgt_db)
        if not src_db_info or not tgt_db_info:
            res['status'] = "FAILED"
            res['errormsg'] = "The DB information is incomplete in database_info, src_db_info[%s]=%s, tgt_db_info[%s]=%s " % (src_db,src_db_info,tgt_db,tgt_db_info)
            return res
        src_appln_support_code = dict(src_db_info[0])['appln_support_code']
        tgt_appln_support_code = dict(tgt_db_info[0])['appln_support_code']
        src_host = dict(src_db_info[0])['host_name']
        tgt_host = dict(tgt_db_info[0])['host_name']

        if str(src_appln_support_code).upper()=='MEDIATE' and str(tgt_appln_support_code).upper()=='TEO':
            dao.add_shareplexInfoList(src_host, src_db, port, tgt_host, tgt_db, qname, src_splex_sid, tgt_splex_sid,
                                      "test", "xxrpth")
            dao.add_shareplexInfoList(src_host, src_db, port, tgt_host, tgt_db, qname, src_splex_sid, tgt_splex_sid,
                                      "xxrpth", "xxrpth")
        elif str(src_appln_support_code).upper()=="DEPOT" and str(tgt_appln_support_code).upper()=="DEPOT":
            dao.add_shareplexInfoList(src_host, src_db, port, tgt_host, tgt_db, qname, src_splex_sid, tgt_splex_sid,
                                      "depot", "depot")
        else:
            dao.add_shareplexInfoList(src_host, src_db, port, tgt_host, tgt_db, qname, src_splex_sid, tgt_splex_sid,
                                      "test", "test")
        # add wbxdba record for monitor
        dao.add_shareplexInfoList(src_host, src_db, port, tgt_host, tgt_db, qname, src_splex_sid, tgt_splex_sid,
                                  "wbxdba", "wbxdba")
        daoManager.commit()

    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def insert_shareplex_info(src_db,tgt_db,src_host,tgt_host,port,qname,src_schema,tgt_schema,src_splex_sid, tgt_splex_sid):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    res = {"status": "SUCCESS", "msg": "", "errormsg": ""}
    try:
        daoManager.startTransaction()
        dao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        # src_schema_info = dao.getSchemaNameByDB(src_db, src_schematype)
        # tgt_schema_info = dao.getSchemaNameByDB(tgt_db, tgt_schematype)
        # if not src_schema_info:
        #     res['status'] = "FAILED"
        #     res['errormsg'] = "The schema information is incomplete in appln_pool_info, db=%s,src_schema_info=%s" % (
        #     src_db, src_schematype)
        #     return res
        # if not tgt_schema_info:
        #     res['status'] = "FAILED"
        #     res['errormsg'] = "The schema information is incomplete in appln_pool_info, db=%s,tgt_schema_info=%s" % (
        #     tgt_db, tgt_schematype)
        #     return res
        # logger.info("src_schema_info:%s" % (len(src_schema_info)))
        # src_schema = dict(src_schema_info)['schemaname']
        # logger.info(src_schema)
        # logger.info("tgt_schema_info:%s" % (len(tgt_schema_info)))
        # tgt_schema = dict(tgt_schema_info)['schemaname']
        # logger.info(tgt_schema)
        channel_ls = dao.get_channel_info(src_host, src_db, port, tgt_host, tgt_db, qname, src_schema, tgt_schema)
        if len(channel_ls) == 0:
            dao.add_shareplexInfoList(src_host, src_db, port, tgt_host, tgt_db, qname, src_splex_sid, tgt_splex_sid,
                                      src_schema, tgt_schema)
    except Exception as e:
        daoManager.rollback()
        res['status'] = "FAILED"
        res['errormsg'] = str(e)
    return res

def get_wbxadbmon(src_db = None, tgt_db = None, port = None):
    sql = """
    select
        distinct
        wam.src_host,
        wam.src_db,
        wam.port,
        wam.replication_to,
        wam.tgt_host,
        wam.tgt_db,
        to_char(wam.lastreptime, 'yyyy-MM-dd HH24:mi:ss') lastreptime,
        to_char(wam.montime, 'yyyy-MM-dd HH24:mi:ss') montime,
	    ROUND(EXTRACT(EPOCH FROM(wam.montime::timestamp - wam.lastreptime::timestamp))) as lag_by,
        CASE 
            when EXTRACT(EPOCH FROM(wam.montime::timestamp - wam.lastreptime::timestamp)) > (10 * 60) THEN 1
            else 0
        END
        as is_delay
    from wbxadbmon wam, shareplex_info spi
    where 
        wam.src_host=spi.src_host
        and wam.src_db=spi.src_db 
        and wam.port=to_char(spi.port)
        and wam.tgt_host=spi.tgt_host 
        and wam.tgt_db=spi.tgt_db
    """
    if src_db:
        sql += f"and src_db='{src_db}'"
    
    if tgt_db:
        sql += f"and tgt_db='{tgt_db}'"
    
    if port:
        sql+= f"and port='{port}'"

    sql += "order by lag_by desc;"

    datas = []
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    session = daoManager.startTransaction()
    rows = session.execute(sql).fetchall()
    if rows:
        for row in rows:
            lasreptime = parse(row[6])
            montime = parse(row[7])
            d = (montime-lasreptime).days
            times = int(row[8])
            m,_ = divmod(times, 60)
            h,m = divmod(m, 60)
            lag_by = f"{d}:{h}:{m}"
            datas.append(dict(
                source_host=row[0],
                source_db=row[1],
                port=row[2],
                replication_to=row[3],
                target_host=row[4],
                target_db=row[5],
                last_replication_time=row[6],
                monitor_time=row[7],
                lag_by=lag_by,
                is_delay=row[9],
            ))
    session.close()
    return datas