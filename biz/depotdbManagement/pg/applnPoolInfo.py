from biz.depotdbManagement import get_session, process_filter_integer_sql, logger


def list_appln_pool_info(xor="and", **filter_fields):
    res = {
        "status": "SUCCESS",
        "msg": "",
        "data": None,
    }
    sess = get_session()
    sql = """
    select 
        db_name,
        appln_support_code,
        schemaname,
        password,
        password_vault_path,
        created_by,
        modified_by,
        schematype,
        to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from depot.appln_pool_info
    where 1=1
    """
    sql += process_filter_integer_sql([], xor=xor , **filter_fields)


    try:
        conn = sess.execute(sql)
        rows = conn.fetchall()
    except Exception as e:
        res["status"] = "FAILED"
        res["msg"] = str(e)
        logger.error(f"fetch all results for depot.appln_pool_info error: {str(e)}")
    else:
        data = []
        for row in rows:
            data.append(dict(
                db_name=row[0],
                appln_support_code=row[1],
                schemaname=row[2],
                password=row[3],
                password_vault_path=row[4],
                created_by=row[5],
                modified_by=row[6],
                schematype=row[7],
                createtime=row[8],
                lastmodifiedtime=row[9],
            ))
        sess.close()
        res["data"] = data
    return res

def get_appln_pool_info(xor="and", **filter_fields):
    sess = get_session()
    sql = f"""
    select 
        db_name,
        appln_support_code,
        schemaname,
        password,
        password_vault_path,
        created_by,
        modified_by,
        schematype,
        to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from depot.appln_pool_info
    where 1=1
    """
    sql += process_filter_integer_sql([], xor=xor, **filter_fields)

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
                password_vault_path=data[4],
                created_by=data[5],
                modified_by=data[6],
                schematype=data[7],
                createtime=data[8],
                lastmodifiedtime=data[9]
            )


def list_depotdb_appln_pool_info(**filters):
    sess = get_session()
    sql = f"""
    select 
        dapi.db_name,
        dapi.appln_support_code,
        dapi.schemaname,
        dapi.password,
        dapi.password_vault_path,
        dapi.created_by,
        dapi.modified_by,
        dapi.schematype,
        to_char(dapi.createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(dapi.lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from depot.appln_pool_info dapi, depot.host_info dhi, depot.database_info ddi
    where dapi.db_name=ddi.db_name and ddi.host_name=dhi.host_name
    """
    if "db_name" in filters:
        sql += f"and dapi.db_name='{filters['db_name']}'"
    
    if "host_name" in filters:
        sql += f"and dhi.host_name='{filters['host_name']}'"

    sql += "order by schemaname;"

    try:
        rows = sess.execute(sql).fetchall()
    except Exception as e:
        logger.error(f"fetch {filters} data error: {str(e)}")
    else:
        sess.close()
        data = []
        for row in rows:
            data.append(dict(
                db_name=row[0],
                appln_support_code=row[1],
                schemaname=row[2],
                password=row[3],
                password_vault_path=row[4],
                created_by=row[5],
                modified_by=row[6],
                schematype=row[7],
                createtime=row[8],
                lastmodifiedtime=row[9],
            ))
        return data
