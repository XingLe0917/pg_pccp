
from common.wbxinfluxdb import wbxinfluxdb

def get_pg_database_summary_tablelist(db_name,start_time,end_time):
    result = {"status": "", "errormsg": "", "data": None}
    influx_db_obj = wbxinfluxdb()
    tablelist = influx_db_obj.get_pg_database_summary_data(db_name,start_time,end_time)
    try:
        if not tablelist:
            result['status'] = 'FAILED'
            result['data'] = []
            result['arrormsg'] = 'no data'
        else:
            result['status'] = 'SUCCEED'
            result['data'] = tablelist
    except Exception as e:
        result["status"] = "FAILED"
        result["errormsg"] = str(e)
    return result

def get_pg_statement_summary_tablelist(db_name,start_time,end_time):
    result1 = {"status": "", "errormsg": "", "data": None}
    influx_db_obj1 = wbxinfluxdb()
    tablelist1 = influx_db_obj1.get_pg_statement_summary_data(db_name,start_time,end_time)
    try:
        if not tablelist1:
            result1['status'] = 'FAILED'
            result1['data'] = []
            result1['arrormsg'] = 'no data'
        else:
            result1['status'] = 'SUCCEED'
            result1['data'] = tablelist1
    except Exception as e:
        result1["status"] = "FAILED"
        result1["errormsg"] = str(e)
    return result1

def get_pg_checkpoint_performance_tablelist(db_name,start_time,end_time):
    result = {"status": "", "errormsg": "", "data": None}
    influx_db_obj = wbxinfluxdb()
    tabledata = influx_db_obj.get_pg_checkpoint_performance_data(db_name,start_time,end_time)
    tablecount = influx_db_obj.get_pg_checkpoint_performance_count(db_name,start_time,end_time)
    tablesum = influx_db_obj.get_pg_checkpoint_performance_sum(db_name,start_time,end_time)
    try:
        if not tabledata:
            result['status'] = 'FAILED'
            result['data'] = []
            result['arrormsg'] = 'no data'
        else:
            result['status'] = 'SUCCEED'
            result['data'] = tabledata
            result['sum'] = tablesum
            result['count'] = tablecount
    except Exception as e:
        result["status"] = "FAILED"
        result["errormsg"] = str(e)
    return result

def get_wbxdba_stat_all_tables_tablelist(db_name,start_time,end_time):
    result = {"status": "", "errormsg": "", "data": None}
    influx_db_obj = wbxinfluxdb()
    tablelist = influx_db_obj.get_wbxdba_stat_all_tables_data(db_name,start_time,end_time)
    try:
        if not tablelist:
            result['status'] = 'FAILED'
            result['data'] = []
            result['arrormsg'] = 'no data'
        else:
            result['status'] = 'SUCCEED'
            result['data'] = tablelist
    except Exception as e:
        result["status"] = "FAILED"
        result["errormsg"] = str(e)
    return result

def get_pg_stat_statements_tablelist(db_name,start_time,end_time):
    result = {"status": "", "errormsg": "", "data": None}
    influx_db_obj = wbxinfluxdb()
    tablelist = influx_db_obj.get_pg_stat_statements_data(db_name,start_time,end_time)
    try:
        if not tablelist:
            result['status'] = 'FAILED'
            result['data'] = []
            result['arrormsg'] = 'no data'
        else:
            result['status'] = 'SUCCEED'
            result['data'] = tablelist
    except Exception as e:
        result["status"] = "FAILED"
        result["errormsg"] = str(e)
    return result

def get_pg_vacuum_stat_tablelist(db_name,start_time,end_time):
    result = {"status": "", "errormsg": "", "data": None}
    influx_db_obj = wbxinfluxdb()
    tablelist = influx_db_obj.get_pg_vacuum_stat_data(db_name,start_time,end_time)
    try:
        if not tablelist:
            result['status'] = 'FAILED'
            result['data'] = []
            result['arrormsg'] = 'no data'
        else:
            result['status'] = 'SUCCEED'
            result['data'] = tablelist
    except Exception as e:
        result["status"] = "FAILED"
        result["errormsg"] = str(e)
    return result

def get_pg_stat_bgwriter_tablelist(db_name,start_time,end_time):
    result = {"status": "", "errormsg": "", "data": None}
    influx_db_obj = wbxinfluxdb()
    tablelist = influx_db_obj.get_pg_stat_bgwriter_data(db_name,start_time,end_time)
    try:
        if not tablelist:
            result['status'] = 'FAILED'
            result['data'] = []
            result['arrormsg'] = 'no data'
        else:
            result['status'] = 'SUCCEED'
            result['data'] = tablelist
    except Exception as e:
        result["status"] = "FAILED"
        result["errormsg"] = str(e)
    return result