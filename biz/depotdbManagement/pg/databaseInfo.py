from biz.depotdbManagement import get_session, process_filter_integer_sql, logger


def list_database_info(xor="and", **filter_fields):
    res = {
        "status": "SUCCESS",
        "msg": "",
        "data": None,
    }
    sess = get_session()
    sql = """
    select
        db_name,
        host_name,
        cluster_name,
        db_vendor,
        db_version,
        db_type,
        application_type,
        appln_support_code,
        db_home,
        listener_port,
        monitored,
        wbx_cluster,
        web_domain,
        to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from depot.database_info
    where 1=1
    """
    sql += process_filter_integer_sql(["listener_port"], xor=xor, **filter_fields)

    try:
        conn = sess.execute(sql)
        rows = conn.fetchall()
    except Exception as e:
        res["status"] = "FAILED"
        res["msg"] = str(e)
        logger.error(f"fetch all results for depot.database_info error: {str(e)}")
    else:
        data = []
        for row in rows:
            data.append(dict(
                db_name=row[0],
                host_name=row[1],
                cluster_name=row[2],
                db_vendor=row[3],
                db_version=row[4],
                db_type=row[5],
                application_type=row[6],
                appln_support_code=row[7],
                db_home=row[8],
                listener_port=row[9],
                monitored=row[10],
                wbx_cluster=row[11],
                web_domain=row[12],
                createtime=row[13],
                lastmodifiedtime=row[14],
            ))
        sess.close()
        res["data"] = data
    return res

def get_database_info(xor="and", **filter_fields):
    sess = get_session()
    sql = f"""
    select
        db_name,
        host_name,
        cluster_name,
        db_vendor,
        db_version,
        db_type,
        application_type,
        appln_support_code,
        db_home,
        listener_port,
        monitored,
        wbx_cluster,
        web_domain,
        to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from depot.database_info ddi, depot.host_info dhi
    where 1=1
    """
    sql += process_filter_integer_sql(["listener_port"], xor=xor, **filter_fields)

    try:
        data = sess.execute(sql).fetchone()
    except Exception as e:
        logger.error(f"fetch {filter_fields} data error: {str(e)}")
    else:
        sess.close()
        if data:
            return dict(
                db_name=data[0],
                host_name=data[1],
                cluster_name=data[2],
                db_vendor=data[3],
                db_version=data[4],
                db_type=data[5],
                application_type=data[6],
                appln_support_code=data[7],
                db_home=data[8],
                listener_port=data[9],
                monitored=data[10],
                wbx_cluster=data[11],
                web_domain=data[12],
                createtime=data[13],
                lastmodifiedtime=data[14],
            )


def list_depotdb_database_info(xor="and", **filter_fields):
    sess = get_session()
    sql = """
    select
        db_name,
        host_name,
        cluster_name,
        db_vendor,
        db_version,
        db_type,
        application_type,
        appln_support_code,
        db_home,
        listener_port,
        monitored,
        wbx_cluster,
        web_domain,
        to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from depot.database_info
    where 1=1
    """
    sql += process_filter_integer_sql(["listener_port"], xor=xor, **filter_fields)

    try:
        conn = sess.execute(sql)
        rows = conn.fetchall()
    except Exception as e:
        logger.error(f"fetch all results for depot.database_info error: {str(e)}")
    else:
        data = []
        for row in rows:
            data.append(dict(
                db_name=row[0],
                host_name=row[1],
                cluster_name=row[2],
                db_vendor=row[3],
                db_version=row[4],
                db_type=row[5],
                application_type=row[6],
                appln_support_code=row[7],
                db_home=row[8],
                listener_port=row[9],
                monitored=row[10],
                wbx_cluster=row[11],
                web_domain=row[12],
                createtime=row[13],
                lastmodifiedtime=row[14],
            ))
        sess.close()
    return data