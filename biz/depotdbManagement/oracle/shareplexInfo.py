from biz.depotdbManagement import get_session, process_filter_integer_sql, logger


def create_shareplex_info(data):
    res = {
        "status": "SUCCESS",
        "msg": "",
        "data": None,
    }
    temp = get_shareplex_info(
        src_host=data["src_host"],
        src_db=data["src_db"],
        port=data["port"],
        tgt_host=data["tgt_host"],
        tgt_db=data["tgt_db"]
    )
    if temp:
        res["msg"] = "data exists"
        res["data"] = temp
        return res

    sess = get_session()
    fields_string = ",".join(list(data.keys()))
    values_temp = "','".join([str(i) for i in list(data.values())])
    values_string = f"'{values_temp}'"
    sql = f"""
    insert into wbxora.shareplex_info({fields_string})
    values
    ({values_string})
    """
    try:
        sess.execute(sql)
    except Exception as e:
        res["status"] = "FAILED"
        res["msg"] = str(e)
        logger.error(f"create shareplex_info error: {str(e)}")
        sess.rollback()
    else:
        sess.commit()
        sess.close()
        res["data"] = get_shareplex_info(
            src_host=data["src_host"],
            src_db=data["src_db"],
            port=data["port"],
            tgt_host=data["tgt_host"],
            tgt_db=data["tgt_db"]
        )
    return res

def list_shareplex_info(**filter_fields):
    res = {
        "status": "SUCCESS",
        "msg": "",
        "data": None,
    }
    sess = get_session()
    sql = """
    select
        src_host,
        src_db,
        port,
        replication_to,
        tgt_host,
        tgt_db,
        qname,
        src_splex_sid,
        tgt_splex_sid,
        src_schema,
        tgt_schema,
        created_by,
        modified_by,
        to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from wbxora.shareplex_info
    where 1=1
    """
    sql += process_filter_integer_sql([], **filter_fields)

    try:
        conn = sess.execute(sql)
        rows = conn.fetchall()
    except Exception as e:
        res["status"] = "FAILED"
        res["msg"] = str(e)
        logger.error(f"fetch all results for wbxora.shareplex_info error: {str(e)}")
    else:
        data = []
        for row in rows:
            data.append(dict(
                src_host=row[0],
                src_db=row[1],
                port=row[2],
                replication_to=row[3],
                tgt_host=row[4],
                tgt_db=row[5],
                qname=row[6],
                src_splex_sid=row[7],
                tgt_splex_sid=row[8],
                src_schema=row[9],
                tgt_schema=row[10],
                created_by=row[11],
                modified_by=row[12],
                createtime=row[13],
                lastmodifiedtime=row[14],
            ))
        sess.close()
        res["data"] = data
    return res

def get_shareplex_info(**filter_fields):
    sess = get_session()
    sql = f"""
    select
        src_host,
        src_db,
        port,
        replication_to,
        tgt_host,
        tgt_db,
        qname,
        src_splex_sid,
        tgt_splex_sid,
        src_schema,
        tgt_schema,
        created_by,
        modified_by,
        to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from wbxora.shareplex_info
    where 1=1
    """
    sql += process_filter_integer_sql(["port"], **filter_fields)

    try:
        data = sess.execute(sql).fetchone()
    except Exception as e:
        logger.error(f"fetch {filter_fields} data error: {str(e)}")
    else:
        sess.close()
        if data:
            return dict(
                src_host=data[0],
                src_db=data[1],
                port=data[2],
                replication_to=data[3],
                tgt_host=data[4],
                tgt_db=data[5],
                qname=data[6],
                src_splex_sid=data[7],
                tgt_splex_sid=data[8],
                src_schema=data[9],
                tgt_schema=data[10],
                created_by=data[11],
                modified_by=data[12],
                createtime=data[13],
                lastmodifiedtime=data[14],
            )

def list_depotdb_shareplex_info(**filter_fields):
    sess=get_session()
    sql="""
    select
        src_host,
        src_db,
        port,
        replication_to,
        tgt_host,
        tgt_db,
        qname,
        src_splex_sid,
        tgt_splex_sid,
        src_schema,
        tgt_schema,
        created_by,
        modified_by,
        to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from wbxora.shareplex_info
    where 1=1
    """
    if "db_name" in filter_fields:
        sql += f"and (src_db='{filter_fields['db_name']}' or tgt_db='{filter_fields['db_name']}')"
    
    if "host_name" in filter_fields:
        sql += f"and (src_host='{filter_fields['host_name']}' or tgt_host='{filter_fields['host_name']}')"

    try:
        rows = sess.execute(sql).fetchall()
    except Exception as e:
        sess.rollback()
        logger.error(f"fetch {filter_fields} data error: {str(e)}")
    else:
        data = []
        for row in rows:
            data.append(dict(
                src_host=row[0],
                src_db=row[1],
                port=row[2],
                replication_to=row[3],
                tgt_host=row[4],
                tgt_db=row[5],
                qname=row[6],
                src_splex_sid=row[7],
                tgt_splex_sid=row[8],
                src_schema=row[9],
                tgt_schema=row[10],
                created_by=row[11],
                modified_by=row[12],
                createtime=row[13],
                lastmodifiedtime=row[14],
            ))
        sess.close()
        return data