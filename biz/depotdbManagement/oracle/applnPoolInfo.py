from copy import deepcopy
from biz.depotdbManagement import get_session, process_filter_integer_sql, logger


def create_appln_pool_info(data):
    update_data=deepcopy(data)
    update_data.pop("db_name", None)
    update_data.pop("schemaname", None)
    key_val_string = [f"{key}=excluded.{key}" for key,_ in update_data.items()]
    key_val_string = ",".join(key_val_string)
    res = {
        "status": "SUCCESS",
        "msg": "",
        "data": None,
    }
    sess = get_session()
    data["db_name"] = data["db_name"].upper()
    data["schemaname"] = data["schemaname"].lower()
    fields_string = ",".join(list(data.keys()))
    values_temp = "','".join([str(i) for i in list(data.values())])
    values_string = f"'{values_temp}'"
    sql = f"""
    insert into wbxora.appln_pool_info({fields_string})
    values
    ({values_string})
    on conflict(db_name, schemaname) do update
    set
    {key_val_string},
    lastmodifiedtime=now();
    """
    try:
        sess.execute(sql)
    except Exception as e:
        res["status"] = "FAILED"
        res["msg"] = str(e)
        logger.error(f"create appln_pool_info error: {str(e)}")
        sess.rollback()
    else:
        sess.commit()
        sess.close()
        res["data"] = get_appln_pool_info(
            db_name=data["db_name"].upper(),
            schemaname=data["schemaname"].lower()
        )

    return res


def list_appln_pool_info(**filter_fields):
    res = {
        "status": "SUCCESS",
        "msg": "",
        "data": None,
    }
    sess = get_session()
    sql = """
    select db_name,appln_support_code,schemaname,password,created_by,modified_by,schematype,
    password_vault_path||'/'||case when lower(schemaname) = 'wbxdba' then 'wbxdba' else schematype end as password_vault_path,
    to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
    to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from wbxora.appln_pool_info
    where 1=1
    """
    sql += process_filter_integer_sql([], **filter_fields)


    try:
        conn = sess.execute(sql)
        rows = conn.fetchall()
    except Exception as e:
        res["status"] = "FAILED"
        res["msg"] = str(e)
        logger.error(f"fetch all results for wbxora.appln_pool_info error: {str(e)}")
    else:
        data = []
        for row in rows:
            data.append(dict(
                db_name=row[0],
                appln_support_code=row[1],
                schemaname=row[2],
                password=row[3],
                created_by=row[4],
                modified_by=row[5],
                schematype=row[6],
                password_vault_path=row[7],
                createtime=row[8],
                lastmodifiedtime=row[9]
            ))
        sess.close()
        res["data"] = data
    return res

def get_appln_pool_info(**filter_fields):
    sess = get_session()
    sql = f"""
    select db_name,appln_support_code,schemaname,password,created_by,modified_by,schematype,
    to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
    to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from wbxora.appln_pool_info
    where 1=1
    """
    sql += process_filter_integer_sql([], **filter_fields)

    try:
        data = sess.execute(sql).fetchone()
    except Exception as e:
        logger.error(f"fetch {filter_fields} data error: {str(e)}")
    else:
        sess.close()
        if data:
            return dict(
                db_name=data[0],
                appln_support_code=data[1],
                schemaname=data[2],
                password=data[3],
                created_by=data[4],
                modified_by=data[5],
                schematype=data[6],
                createtime=data[7],
                lastmodifiedtime=data[8]
            )


def list_depotdb_appln_pool_infos(**filters):
    sess = get_session()
    sql = f"""
    select 
        wapi.trim_host,
        wapi.db_name,
        wapi.appln_support_code,
        wapi.schemaname,
        wapi.password,
        wapi.password_vault_path,
        wapi.created_by,
        wapi.modified_by,
        wapi.schematype,
        to_char(wapi.createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(wapi.lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from wbxora.appln_pool_info wapi, wbxora.host_info dhi, wbxora.database_info ddi
    where wapi.db_name=ddi.db_name and ddi.trim_host=dhi.trim_host
    """
    if "db_name" in filters:
        sql += f"and wapi.db_name='{filters['db_name'].upper()}'"
    
    if "host_name" in filters:
        sql += f"and dhi.host_name='{filters['host_name']}'"

    sql += "order by schemaname;"

    try:
        rows = sess.execute(sql).fetchall()
    except Exception as e:
        sess.rollback()
        logger.error(f"fetch {filters} data error: {str(e)}")
    else:
        data = []
        for row in rows:
            data.append(dict(
                trim_host=row[0],
                db_name=row[1],
                appln_support_code=row[2],
                schemaname=row[3],
                password=row[4],
                password_vault_path=row[5],
                created_by=row[6],
                modified_by=row[7],
                schematype=row[8],
                createtime=row[9],
                lastmodifiedtime=row[10],
            ))
        sess.close()
        return data