from copy import deepcopy
from biz.depotdbManagement import get_session, process_filter_integer_sql, logger


def create_instance_info(data):
    update_data=deepcopy(data)
    update_data.pop("host_name", None)
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
    insert into wbxora.instance_info({fields_string})
    values
    ({values_string})
    on conflict(host_name, db_name) do update
    set
    {key_val_string};
    """
    try:
        sess.execute(sql)
    except Exception as e:
        res["status"] = "FAILED"
        res["msg"] = str(e)
        logger.error(f"create instance_info error: {str(e)}")
        sess.rollback()
    else:
        sess.commit()
        sess.close()
        res["data"] = get_instance_info(
            host_name=data["host_name"],
            db_name = data["db_name"]
        )
    return res

def list_instance_info(**filter_fields):
    res = {
        "status": "SUCCESS",
        "msg": "",
        "data": None,
    }
    sess = get_session()
    sql = """
    select
        trim_host,
        host_name,
        db_name,
        instance_name,
        created_by,
        modified_by,
        to_char(date_added, 'yyyy-MM-dd HH24:mi:ss') date_added,
        to_char(lastmodifieddate, 'yyyy-MM-dd HH24:mi:ss') lastmodifieddate
    from wbxora.instance_info
    where 1=1
    """
    sql += process_filter_integer_sql([], **filter_fields)

    try:
        conn = sess.execute(sql)
        rows = conn.fetchall()
    except Exception as e:
        res["status"] = "FAILED"
        res["msg"] = str(e)
        logger.error(f"fetch all results for wbxora.instance_info error: {str(e)}")
    else:
        data = []
        for row in rows:
            data.append(dict(
                trim_host=row[0],
                host_name=row[1],
                db_name=row[2],
                instance_name=row[3],
                created_by=row[4],
                modified_by=row[5],
                date_added=row[6],
                lastmodifieddate=row[7],
            ))
        sess.close()
        res["data"] = data
    return res

def get_instance_info(**filter_fields):
    sess = get_session()
    sql = f"""
    select
        trim_host,
        host_name,
        db_name,
        instance_name,
        created_by,
        modified_by,
        to_char(date_added, 'yyyy-MM-dd HH24:mi:ss') date_added,
        to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from wbxora.instance_info
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
                trim_host=data[0],
                host_name=data[1],
                db_name=data[2],
                instance_name=data[3],
                created_by=data[4],
                modified_by=data[5],
                date_added=data[6],
                lastmodifieddate=data[7],
            )

def list_depotdb_instance_info(**filter_fields):
    sess=get_session()
    sql = """
    select
        trim_host,
        host_name,
        db_name,
        instance_name,
        created_by,
        modified_by,
        to_char(date_added, 'yyyy-MM-dd HH24:mi:ss') date_added,
        to_char(lastmodifieddate, 'yyyy-MM-dd HH24:mi:ss') lastmodifieddate
    from wbxora.instance_info
    where 1=1
    """

    if "db_name" in filter_fields:
        sql += f"and db_name='{filter_fields['db_name']}'"
    
    if "host_name" in filter_fields:
        sql += f"and host_name='{filter_fields['host_name']}'"

    try:
        conn = sess.execute(sql)
        rows = conn.fetchall()
    except Exception as e:
        sess.rollback()
        logger.error(f"fetch all results for wbxora.instance_info error: {str(e)}")
    else:
        data = []
        for row in rows:
            data.append(dict(
                trim_host=row[0],
                host_name=row[1],
                db_name=row[2],
                instance_name=row[3],
                created_by=row[4],
                modified_by=row[5],
                date_added=row[6],
                lastmodifieddate=row[7],
            ))
        sess.close()
        return data