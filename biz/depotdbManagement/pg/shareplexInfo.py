from biz.depotdbManagement import get_session, process_filter_integer_sql, logger


def list_shareplex_info(xor="and", **filter_fields):
    res = {
        "status": "SUCCESS",
        "msg": "",
        "data": None,
    }
    sess = get_session()
    sql = """
    select src_host,src_db,port,tgt_host,tgt_db,qname,src_splex_sid,tgt_splex_sid,
    src_schema,tgt_schema,created_by,modified_by,
    to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
    to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from depot.shareplex_info
    where 1=1
    """
    sql += process_filter_integer_sql([],xor=xor , **filter_fields)

    try:
        conn = sess.execute(sql)
        rows = conn.fetchall()
    except Exception as e:
        res["status"] = "FAILED"
        res["msg"] = str(e)
        logger.error(f"fetch all results for depot.shareplex_info error: {str(e)}")
    else:
        data = []
        for row in rows:
            data.append(dict(
                src_host=row[0],
                src_db=row[1],
                port=row[2],
                tgt_host=row[3],
                tgt_db=row[4],
                qname=row[5],
                src_splex_sid=row[6],
                tgt_splex_sid=row[7],
                src_schema=row[8],
                tgt_schema=row[9],
                created_by=row[10],
                modified_by=row[11],
                createtime=row[12],
                lastmodifiedtime=row[13],
            ))
        sess.close()
        res["data"] = data
    return res

def get_shareplex_info(xor="and", **filter_fields):
    sess = get_session()
    sql = f"""
    select src_host,src_db,port,tgt_host,tgt_db,qname,src_splex_sid,tgt_splex_sid,
    src_schema,tgt_schema,created_by,modified_by,
    to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
    to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from depot.shareplex_info
    where 1=1
    """
    sql += process_filter_integer_sql(["port"],xor=xor , **filter_fields)

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
                tgt_host=data[3],
                tgt_db=data[4],
                qname=data[5],
                src_splex_sid=data[6],
                tgt_splex_sid=data[7],
                src_schema=data[8],
                tgt_schema=data[9],
                created_by=data[10],
                modified_by=data[11],
                createtime=data[12],
                lastmodifiedtime=data[13],
            )

def list_depotdb_shareplex_info(**filters):
    sess = get_session()
    sql = f"""
    select 
        src_host,
        src_db,
        port,
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
    from depot.shareplex_info
    where 1=1
    """

    if "db_name" in filters:
        sql += f"and (src_db='{filters['db_name']}' or tgt_db='{filters['db_name']}')"
    
    if "host_name" in filters:
        sql += f"and (src_host='{filters['host_name']}' or tgt_host='{filters['host_name']}')"

    try:
        rows = sess.execute(sql).fetchall()
    except Exception as e:
        logger.error(f"fetch {filters} data error: {str(e)}")
    else:
        data = []
        for row in rows:
            data.append(dict(
                src_host=row[0],
                src_db=row[1],
                port=row[2],
                tgt_host=row[3],
                tgt_db=row[4],
                qname=row[5],
                src_splex_sid=row[6],
                tgt_splex_sid=row[7],
                src_schema=row[8],
                tgt_schema=row[9],
                created_by=row[10],
                modified_by=row[11],
                createtime=row[12],
                lastmodifiedtime=row[13],
            ))
        sess.close()
        return data