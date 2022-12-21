from common.wbxinfluxdb import wbxinfluxdb

def testshowinflux(client):
    # sql = "show tag values from WBXDBMONITOR_LISTENER_LOG with key=host"
    # sql = "delete from wbxdb_monitor_odm where db_inst_name='bdprdasj1(sjdbormt060.webex.com)' or db_inst_name='bdprdasj2(sjdbormt061.webex.com)' or db_inst_name='idprdsj1(sjdbormt060.webex.com)' or db_inst_name='idprdsj2(sjdbormt061.webex.com)'" # wbxdb_monitor_odm, wbxdb_monitor_owi
    # sql = "show tag values from wbxdb_monitor_odm with key=db_inst_name where host='sjdbormt060.webex.com'"
    # sql = "delete from wbxdb_monitor_odm where host='sjdbormt060.webex.com' or host='sjdbormt061.webex.com'"
    # sql = "show tag values from osconfig with key=host where datastore='ORACLE' and time > '2020-10-21'"
    # sql = "DROP SERIES from wbxdb_monitor_odm where db_inst_name='ttacomb61(tadbth391.webex.com)'"
    # sql = "show series from system"
    # sql = "select * from shareplex_process where time > now() - 1d  and process_type = 'export' and " \
    #        "src_host = 'sgdbormt014-vip' and tgt_host = 'jpdbormt015-vip' and port='19096' order by time desc limit 1"
    # sql = "show measurements"
    # sql = "show tag keys from WBXDBMONITOR_LISTENER_LOG"
    # sql = "select * from WBXDBMONITOR_LISTENER_LOG where time > now()-5m"
    # sql = "select * from wbxdb_monitor_odm where db_inst_name in ('racbgweb1(frdbormt011.webex.com)','racbgweb2(frdbormt012.webex.com)' ,'racbgweb3(frdbormt015.webex.com)' ,'racbgweb4(frdbormt016.webex.com)') and time > now()-5m"
    # sql = "delete from wbxdb_monitor_odm where db_inst_name =~ /'racbgweb1(frdbormt011.webex.com)'|'racbgweb2(frdbormt012.webex.com)'|'racbgweb3(frdbormt015.webex.com)'|'racbgweb4(frdbormt016.webex.com)/"
    #sql = "select db_name, datname,xact_commit,xact_rollback,deadlocks,blks_hit,blks_read,tup_returned,tup_fetched,tup_inserted,tup_updated,tup_deleted,temp_bytes,temp_files from pg_database_summary order by time limit 10"
    #sql = "select db_name,user_calls,total_exec_time,temp_blks_read,temp_blks_written from pg_statement_summary order by time desc limit 10"
    # sql = "select schemaname,relname,n_live_tup,n_tup_del,n_tup_ins,n_tup_upd,ration from pg_vacuum_stat"
    sql = "select * from wbxdba_stat_all_tables limit 10"
    # sql = "select buffers_backend, buffers_backend_fsync,buffers_alloc,buffers_checkpoint,buffers_clean,checkpoint_sync_time,checkpoint_write_time,checkpoints_req,checkpoints_timed,maxwritten_clean from pg_checkpoint_stat"
    # sql = "select * from wbxdba_stat_all_tables limit 10"
    #sql = "select * from pg_stat_statements order by time desc limit 10"
    # sql = "DROP SERIES from wbxdb_monitor_odm where db_inst_name='racbgweb2(frdbormt012.webex.com)'"
    result = client.query(sql)
    return result

if __name__ == '__main__':
    influx_db_obj = wbxinfluxdb()
    data = testshowinflux(influx_db_obj._client)
    points = data.get_points()
    for item in points:
        print(item)


