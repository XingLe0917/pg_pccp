from copy import deepcopy
from biz.depotdbManagement import get_session, process_filter_integer_sql, logger


def create_host_info(data):
    update_data=deepcopy(data)
    update_data.pop("host_name", None)
    update_data.pop("createtime", None)
    res = {
        "status": "SUCCESS",
        "msg": "",
        "data": None,
    }
    key_val_string = [f"{key}=excluded.{key}" for key,_ in update_data.items()]
    key_val_string = ",".join(key_val_string)

    sess = get_session()
    fields_string = ",".join(list(data.keys()))
    values_temp = "','".join([str(i) for i in list(data.values())])
    values_string = f"'{values_temp}'"
    sql = f"""
    insert into wbxora.host_info({fields_string})
    values
    ({values_string})
    on conflict(host_name) do update
    set
    {key_val_string};
    """
    try:
        sess.execute(sql)
    except Exception as e:
        res["status"] = "FAILED"
        res["msg"] = str(e)
        logger.error(f"create host_info error: {str(e)}")
        sess.rollback()
    else:
        sess.commit()
        sess.close()
        res["data"] = get_host_info(host_name=data["host_name"])
    return res

def list_host_info(**filter_fields):
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
        cname,
        domain,
        site_code,
        host_ip,
        vip_name,
        vip_ip,
        priv_name,
        priv_ip,
        scan_name,
        scan_ip1,
        scan_ip2,
        scan_ip3,
        region_name,
        os_type_code,
        processor,
        kernel_release,
        hardware_platform,
        physical_cpu,
        cores,
        cpu_model,
        flag_node_virtual,
        to_char(install_date, 'yyyy-MM-dd HH24:mi:ss') install_date,
        comments,
        ssh_port,
        to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from wbxora.host_info
    where 1=1
    """
    sql += process_filter_integer_sql(["physical_cpu", "cores"], **filter_fields)

    try:
        conn = sess.execute(sql)
        rows = conn.fetchall()
    except Exception as e:
        res["status"] = "FAILED"
        res["msg"] = str(e)
        logger.error(f"fetch all results for wbxora.host_info error: {str(e)}")
    else:
        data = []
        for row in rows:
            data.append(dict(
                trim_host=row[0],
                host_name=row[1],
                cname=row[2],
                domain=row[3],
                site_code=row[4],
                host_ip=row[5],
                vip_name=row[6],
                vip_ip=row[7],
                priv_name=row[8],
                priv_ip=row[9],
                scan_name=row[10],
                scan_ip1=row[11],
                scan_ip2=row[12],
                scan_ip3=row[13],
                region_name=row[14],
                os_type_code=row[15],
                processor=row[16],
                kernel_release=row[17],
                hardware_platform=row[18],
                physical_cpu=row[19],
                cores=row[20],
                cpu_model=row[21],
                flag_node_virtual=row[22],
                install_date=row[23],
                comments=row[24],
                ssh_port=row[25],
                createtime=row[26],
                lastmodifiedtime=row[27],
            ))
        sess.close()
        res["data"] = data
    return res

def get_host_info(**filter_fields):
    sess = get_session()
    sql = f"""
    select
        trim_host,
        host_name,
        cname,
        domain,
        site_code,
        host_ip,
        vip_name,
        vip_ip,
        priv_name,
        priv_ip,
        scan_name,
        scan_ip1,
        scan_ip2,
        scan_ip3,
        region_name,
        os_type_code,
        processor,
        kernel_release,
        hardware_platform,
        physical_cpu,
        cores,
        cpu_model,
        flag_node_virtual,
        to_char(install_date, 'yyyy-MM-dd HH24:mi:ss') install_date,
        comments,
        ssh_port,
        to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from wbxora.host_info
    where 1=1
    """
    sql += process_filter_integer_sql(["physical_cpu", "cores"], **filter_fields)

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
                cname=data[2],
                domain=data[3],
                site_code=data[4],
                host_ip=data[5],
                vip_name=data[6],
                vip_ip=data[7],
                priv_name=data[8],
                priv_ip=data[9],
                scan_name=data[10],
                scan_ip1=data[11],
                scan_ip2=data[12],
                scan_ip3=data[13],
                region_name=data[14],
                os_type_code=data[15],
                processor=data[16],
                kernel_release=data[17],
                hardware_platform=data[18],
                physical_cpu=data[19],
                cores=data[20],
                cpu_model=data[21],
                flag_node_virtual=data[22],
                install_date=data[23],
                comments=data[24],
                ssh_port=data[25],
                createtime=data[26],
                lastmodifiedtime=data[27],
            )

def list_depotdb_host_info(**filter_fields):
    sess = get_session()
    sql = """
    select
        whi.trim_host,
        whi.host_name,
        whi.cname,
        whi.domain,
        whi.site_code,
        whi.host_ip,
        whi.vip_name,
        whi.vip_ip,
        whi.priv_name,
        whi.priv_ip,
        whi.scan_name,
        whi.scan_ip1,
        whi.scan_ip2,
        whi.scan_ip3,
        whi.region_name,
        whi.os_type_code,
        whi.processor,
        whi.kernel_release,
        whi.hardware_platform,
        whi.physical_cpu,
        whi.cores,
        whi.cpu_model,
        whi.flag_node_virtual,
        to_char(whi.install_date, 'yyyy-MM-dd HH24:mi:ss') install_date,
        whi.comments,
        whi.ssh_port,
        to_char(whi.createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(whi.lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from wbxora.host_info whi, wbxora.database_info wdi
    where whi.trim_host=wdi.trim_host
    """
    if "db_name" in filter_fields:
        sql += f"and wdi.db_name='{filter_fields['db_name']}'"
    
    if "host_name" in filter_fields:
        sql += f"and whi.host_name='{filter_fields['host_name']}'"

    try:
        conn = sess.execute(sql)
        rows = conn.fetchall()
    except Exception as e:
        sess.rollback()
        logger.error(f"fetch all results for wbxora.host_info error: {str(e)}")
    else:
        data = []
        for row in rows:
            data.append(dict(
                trim_host=row[0],
                host_name=row[1],
                cname=row[2],
                domain=row[3],
                site_code=row[4],
                host_ip=row[5],
                vip_name=row[6],
                vip_ip=row[7],
                priv_name=row[8],
                priv_ip=row[9],
                scan_name=row[10],
                scan_ip1=row[11],
                scan_ip2=row[12],
                scan_ip3=row[13],
                region_name=row[14],
                os_type_code=row[15],
                processor=row[16],
                kernel_release=row[17],
                hardware_platform=row[18],
                physical_cpu=row[19],
                cores=row[20],
                cpu_model=row[21],
                flag_node_virtual=row[22],
                install_date=row[23],
                comments=row[24],
                ssh_port=row[25],
                createtime=row[26],
                lastmodifiedtime=row[27],
            ))
        sess.close()
        return data