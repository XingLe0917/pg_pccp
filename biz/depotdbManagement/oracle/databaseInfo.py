from copy import deepcopy
from biz.depotdbManagement import get_session, process_filter_integer_sql, logger


def create_database_info(data):
    update_data=deepcopy(data)
    update_data.pop("db_name", None)
    update_data.pop("createtime", None)
    key_val_string = [f"{key}=excluded.{key}" for key,_ in update_data.items()]
    key_val_string = ",".join(key_val_string)
    res = {
        "status": "SUCCESS",
        "msg": "",
        "data": None,
    }

    sess = get_session()
    fields_string = ",".join(list(data.keys()))
    values_temp = "','".join([str(i) for i in list(data.values())])
    values_string = f"'{values_temp}'"
    sql = f"""
    insert into wbxora.database_info({fields_string})
    values
    ({values_string})
    on conflict(db_name) do update
    set
    {key_val_string};
    """
    try:
        sess.execute(sql)
    except Exception as e:
        res["status"] = "FAILED"
        res["msg"] = str(e)
        logger.error(f"create database_info error: {str(e)}")
        sess.rollback()
    else:
        sess.commit()
        sess.close()
        res["data"] = get_database_info(db_name=data["db_name"])
    return res

def list_database_info(**filter_fields):
    res = {
        "status": "SUCCESS",
        "msg": "",
        "data": None,
    }
    sess = get_session()
    sql = """
    select
        db_name,
        cluster_name,
        db_vendor,
        db_version,
        db_type,
        db_patch,
        application_type,
        appln_support_code,
        db_home,
        service_name,
        listener_port,
        monitored,
        appln_contact,
        contents,
        wbx_cluster,
        web_domain,
        to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime,
        trim_host
    where 1=1
    """
    sql += process_filter_integer_sql(["listener_port"], **filter_fields)

    try:
        conn = sess.execute(sql)
        rows = conn.fetchall()
    except Exception as e:
        res["status"] = "FAILED"
        res["msg"] = str(e)
        logger.error(f"fetch all results for wbxora.database_info error: {str(e)}")
    else:
        data = []
        for row in rows:
            data.append(dict(
                db_name=row[0],
                cluster_name=row[1],
                db_vendor=row[2],
                db_version=row[3],
                db_type=row[4],
                db_patch=row[5],
                application_type=row[6],
                appln_support_code=row[7],
                db_home=row[8],
                service_name=row[9],
                listener_port=row[10],
                monitored=row[11],
                appln_contact=row[12],
                contents=row[13],
                wbx_cluster=row[14],
                web_domain=row[15],
                createtime=row[16],
                lastmodifiedtime=row[17],
                trim_host=row[18],
            ))
        sess.close()
        res["data"] = data
    return res

def get_database_info(**filter_fields):
    sess = get_session()
    sql = f"""
    select 
        db_name,
        cluster_name,
        db_vendor,
        db_version,
        db_type,
        db_patch,
        application_type,
        appln_support_code,
        db_home,
        service_name,
        listener_port,
        monitored,
        appln_contact,
        contents,
        wbx_cluster,
        web_domain,
        to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime,
        trim_host
    from wbxora.database_info
    where 1=1
    """
    sql += process_filter_integer_sql(["listener_port"], **filter_fields)

    try:
        data = sess.execute(sql).fetchone()
    except Exception as e:
        logger.error(f"fetch {filter_fields} data error: {str(e)}")
    else:
        sess.close()
        if data:
            return dict(
                db_name=data[0],
                cluster_name=data[1],
                db_vendor=data[2],
                db_version=data[3],
                db_type=data[4],
                db_patch=data[5],
                application_type=data[6],
                appln_support_code=data[7],
                db_home=data[8],
                service_name=data[9],
                listener_port=data[10],
                monitored=data[11],
                appln_contact=data[12],
                contents=data[13],
                wbx_cluster=data[14],
                web_domain=data[15],
                createtime=data[16],
                lastmodifiedtime=data[17],
                trim_host=data[18],
            )


def update_database_info(**update_fields):
    res = {
        "status": "SUCCESS",
        "msg": "",
        "data": None,
    }
    sess = get_session()
    sql = """
    update wbxora.database_info set
    {key_values}
    """
    key_val_string = [f"{key}='{val}'" for key,val in update_fields.items()]
    key_val_string = ",".join(key_val_string)
    sql.format(key_values=key_val_string)
    try:
        sess.execute(sql)
    except Exception as e:
        res["status"] = "FAILED"
        res["msg"] = str(e)
        logger.error(f"update wbxora.database_info error: {str(e)}")
        sess.rollback()
    else:
        sess.commit()
        sess.close()
        res["msg"] = "Update SUCCEED"

    return res

def list_depotdb_database_info(xor="and", **filter_fields):
    sess = get_session()
    sql = """
    select
        db_name,
        cluster_name,
        db_vendor,
        db_version,
        db_type,
        db_patch,
        application_type,
        appln_support_code,
        db_home,
        service_name,
        listener_port,
        monitored,
        appln_contact,
        contents,
        wbx_cluster,
        web_domain,
        to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime,
        trim_host
    from wbxora.database_info
    where 1=1
    """
    if "db_name" in filter_fields:
        sql += f"and db_name='{filter_fields['db_name']}'"
    
    if "host_name" in filter_fields:
        sql += f"and '{filter_fields['host_name']}' like '%' || trim_host || '%'"

    try:
        conn = sess.execute(sql)
        rows = conn.fetchall()
    except Exception as e:
        sess.rollback()
        logger.error(f"fetch all results for wbxora.database_info error: {str(e)}")
    else:
        data = []
        for row in rows:
            data.append(dict(
                db_name=row[0],
                cluster_name=row[1],
                db_vendor=row[2],
                db_version=row[3],
                db_type=row[4],
                db_patch=row[5],
                application_type=row[6],
                appln_support_code=row[7],
                db_home=row[8],
                service_name=row[9],
                listener_port=row[10],
                monitored=row[11],
                appln_contact=row[12],
                contents=row[13],
                wbx_cluster=row[14],
                web_domain=row[15],
                createtime=row[16],
                lastmodifiedtime=row[17],
                trim_host=row[18],
            ))
        sess.close()
        return data