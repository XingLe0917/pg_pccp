from biz.depotdbManagement import get_session, process_filter_integer_sql, logger


def list_host_info(xor="and", **filter_fields):
    res = {
        "status": "SUCCESS",
        "msg": "",
        "data": None,
    }
    sess = get_session()
    sql = """
    select host_name,cname,domain,site_code,region_name,public_ip,private_ip,
    os_type_code,processor,kernel_release,hardware_platform,physical_cpu,
    cores,cpu_model,flag_node_virtual,comments,ssh_port,
    to_char(install_date, 'yyyy-MM-dd HH24:mi:ss') install_date,
    to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
    to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from depot.host_info
    where 1=1
    """
    sql += process_filter_integer_sql(["physical_cpu", "cores"],xor=xor , **filter_fields)

    try:
        conn = sess.execute(sql)
        rows = conn.fetchall()
    except Exception as e:
        res["status"] = "FAILED"
        res["msg"] = str(e)
        logger.error(f"fetch all results for depot.host_info error: {str(e)}")
    else:
        data = []
        for row in rows:
            data.append(dict(
                host_name=row[0],
                cname=row[1],
                domain=row[2],
                site_code=row[3],
                region_name=row[4],
                public_ip=row[5],
                private_ip=row[6],
                os_type_code=row[7],
                processor=row[8],
                kernel_release=row[9],
                hardware_platform=row[10],
                physical_cpu=row[11],
                cores=row[12],
                cpu_model=row[13],
                flag_node_virtual=row[14],
                comments=row[15],
                ssh_port=row[16],
                install_date=row[17],
                createtime=row[18],
                lastmodifiedtime=row[19],
            ))
        sess.close()
        res["data"] = data
    return res

def get_host_info(xor="and", **filter_fields):
    sess = get_session()
    sql = f"""
    select host_name,cname,domain,site_code,region_name,public_ip,private_ip,
    os_type_code,processor,kernel_release,hardware_platform,physical_cpu,
    cores,cpu_model,flag_node_virtual,comments,ssh_port,
    to_char(install_date, 'yyyy-MM-dd HH24:mi:ss') install_date,
    to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
    to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from depot.host_info
    where 1=1
    """
    sql += process_filter_integer_sql(["physical_cpu", "cores"],xor=xor , **filter_fields)

    try:
        data = sess.execute(sql).fetchone()
    except Exception as e:
        logger.error(f"fetch {filter_fields} data error: {str(e)}")
    else:
        sess.close()
        if data:
            return dict(
                host_name=data[0],
                cname=data[1],
                domain=data[2],
                site_code=data[3],
                region_name=data[4],
                public_ip=data[5],
                private_ip=data[6],
                os_type_code=data[7],
                processor=data[8],
                kernel_release=data[9],
                hardware_platform=data[10],
                physical_cpu=data[11],
                cores=data[12],
                cpu_model=data[13],
                flag_node_virtual=data[14],
                comments=data[15],
                ssh_port=data[16],
                install_date=data[17],
                createtime=data[18],
                lastmodifiedtime=data[19],
            )

def list_depotdb_host_info(**filters):
    sess = get_session()
    sql = f"""
    select 
        dhi.host_name,
        dhi.cname,
        dhi.domain,
        dhi.site_code,
        dhi.region_name,
        dhi.public_ip,
        dhi.private_ip,
        dhi.os_type_code,
        dhi.processor,
        dhi.kernel_release,
        dhi.hardware_platform,
        dhi.physical_cpu,
        dhi.cores,
        dhi.cpu_model,
        dhi.flag_node_virtual,
        dhi.comments,
        dhi.ssh_port,
        to_char(dhi.install_date, 'yyyy-MM-dd HH24:mi:ss') install_date,
        to_char(dhi.createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(dhi.lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from depot.host_info dhi, depot.database_info ddi
    where dhi.host_name=ddi.host_name
    """
    if "db_name" in filters:
        sql += f"and ddi.db_name='{filters['db_name']}'"
    
    if "host_name" in filters:
        sql += f"and dhi.host_name='{filters['host_name']}'"
    
    try:
        rows = sess.execute(sql).fetchall()
    except Exception as e:
        logger.error(f"fetch {filters} data error: {str(e)}")
    else:
        data = []
        for row in rows:
            data.append(dict(
                host_name=row[0],
                cname=row[1],
                domain=row[2],
                site_code=row[3],
                region_name=row[4],
                public_ip=row[5],
                private_ip=row[6],
                os_type_code=row[7],
                processor=row[8],
                kernel_release=row[9],
                hardware_platform=row[10],
                physical_cpu=row[11],
                cores=row[12],
                cpu_model=row[13],
                flag_node_virtual=row[14],
                comments=row[15],
                ssh_port=row[16],
                install_date=row[17],
                createtime=row[18],
                lastmodifiedtime=row[19],
            ))
        sess.close()
        return data