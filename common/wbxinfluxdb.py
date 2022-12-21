from influxdb import InfluxDBClient
import logging
from datetime import datetime
import copy
from common.singleton import Singleton
from common.wbxutil import wbxutil
from common.Config import Config

logger = logging.getLogger("DBAMONITOR")
import datetime


@Singleton
class wbxinfluxdb:
    def __init__(self):

        self._client = Config().getInfluxDBclient()

    def utctime(self, time):
        timeformat = "%Y-%m-%d %H:%M:%S"
        aaa_strptime = datetime.datetime.strptime(time, timeformat)
        dateformat = "%Y-%m-%dT%H:%M:%SZ"
        return datetime.datetime.strftime(aaa_strptime, dateformat)

    def get_pg_database_summary_data(self, db_name, start_time, end_time):
        con = ""
        sql = "select db_name,datname,xact_commit,xact_rollback,deadlocks,blks_hit,blks_read,tup_returned,tup_fetched," \
              "tup_inserted,tup_updated,tup_deleted,temp_bytes,temp_files from pg_database_summary " + con + "order " \
                                                                                                             "by time " \
                                                                                                             "desc " \
                                                                                                             "limit 10 "
        if db_name:
            con = "where db_name= '%s'" % db_name
        if start_time and end_time:
            con += "and time >= '%s' and time < '%s'" % (self.utctime(start_time), self.utctime(end_time))
        print(sql)
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        pg_db_data = []
        for item in points:
            rst_dict = {"db_name": item["db_name"], "datname": item["datname"], "xact_commit": item["xact_commit"],
                        "xact_rollback": item["xact_rollback"], "deadlocks": item["deadlocks"],
                        "blks_hit": item["blks_hit"], "blks_read": item["blks_read"],
                        "tup_returned": item["tup_returned"], "tup_fetched": item["tup_fetched"],
                        "tup_inserted": item["tup_inserted"], "tup_updated": item["tup_updated"],
                        "tup_deleted": item["tup_deleted"], "temp_bytes": item["temp_bytes"],
                        "temp_files": item["temp_files"]}
            pg_db_data.append(rst_dict)
        return pg_db_data

    def get_pg_statement_summary_data(self, db_name, start_time, end_time):
        con = ""
        sql = "select db_name,user_calls,total_exec_time,temp_blks_read,temp_blks_written from pg_statement_summary " \
              + con + "order by time desc limit 10 "
        if db_name:
            con = "where db_name= '%s'" % db_name
        if start_time and end_time:
            con += "and time >= '%s' and time < '%s'" % (self.utctime(start_time), self.utctime(end_time))
        print(sql)
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        pg_db_data = []
        for item in points:
            rst_dict = {"db_name": item["db_name"], "user_calls": item["user_calls"],
                        "total_exec_time": item["total_exec_time"], "temp_blks_read": item["temp_blks_read"],
                        "temp_blks_written": item["temp_blks_written"]}
            pg_db_data.append(rst_dict)
        return pg_db_data

    def get_pg_checkpoint_performance_data(self, db_name, start_time, end_time):
        con = ""
        sql = "select total_time,sync_time,write_time,write_buffers from pg_checkpoint_performance " \
              + con + "order by time desc limit 1000"
        if db_name:
            con = "where db_name= '%s'" % db_name
        if start_time and end_time:
            con += "and time >= '%s' and time < '%s'" % (self.utctime(start_time), self.utctime(end_time))
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        pg_db_data = []
        for item in points:
            rst_dict = {"total_time": item["total_time"], "sync_time": item["sync_time"],
                        "write_time": item["write_time"], "write_buffers": item["write_buffers"]}
            pg_db_data.append(rst_dict)
        return pg_db_data

    def get_pg_checkpoint_performance_count(self, db_name, start_time, end_time):
        con = ""
        sql = "select COUNT(total_time) from pg_checkpoint_performance " \
              + con + "order by time desc"
        if db_name:
            con = "where db_name= '%s'" % db_name
        if start_time and end_time:
            con += "and time >= '%s' and time < '%s'" % (self.utctime(start_time), self.utctime(end_time))
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        pg_db_data = []
        for item in points:
            rst_dict = {"count": item["count"]}
            pg_db_data.append(rst_dict)
        return pg_db_data

    def get_pg_checkpoint_performance_sum(self, db_name, start_time, end_time):
        con = ""
        sql = "select SUM(total_time), SUM(sync_time),SUM(write_time),SUM(write_buffers) from pg_checkpoint_performance" \
              + con
        if db_name:
            con = "where db_name= '%s'" % db_name
        if start_time and end_time:
            con += "and time >= '%s' and time < '%s'" % (self.utctime(start_time), self.utctime(end_time))
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        pg_db_data = []
        for item in points:
            rst_dict = {"total_time": round(item["sum"],1), "sync_time": round(item["sum_1"],1),
                        "write_time": round(item["sum_2"],1), "write_buffers": round(item["sum_3"],1)}
            pg_db_data.append(rst_dict)
        return pg_db_data

    def get_wbxdba_stat_all_tables_data(self, db_name, start_time, end_time):
        con = ""
        sql = "select * from wbxdba_stat_all_tables " \
              + con + "order by time desc limit 10 "
        if db_name:
            con = "where db_name= '%s'" % db_name
        if start_time and end_time:
            con += "and time >= '%s' and time < '%s'" % (self.utctime(start_time), self.utctime(end_time))
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        pg_db_data = []
        for item in points:
            rst_dict = {"db_name": item["db_name"]}
            pg_db_data.append(rst_dict)
        return pg_db_data

    def get_pg_stat_statements_data(self, db_name, start_time, end_time):
        con = ""
        sql = "select queryid,db_name,total_exec_time,blk_read_time,blk_write_time,rows,calls from pg_stat_statements " \
              + con + "order by time desc limit 10 "
        if db_name:
            con = "where db_name= '%s'" % db_name
        if start_time and end_time:
            con += "and time >= '%s' and time < '%s'" % (self.utctime(start_time), self.utctime(end_time))
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        pg_db_data = []
        for item in points:
            rst_dict = {"queryid": item["queryid"], "db_name": item["db_name"],
                        "total_exec_time": item["total_exec_time"], "blk_read_time": item["blk_read_time"],
                        "blk_write_time": item["blk_write_time"], "rows": item["rows"], "calls": item["calls"]}
            pg_db_data.append(rst_dict)
        return pg_db_data

    def get_pg_vacuum_stat_data(self, db_name, start_time, end_time):
        con = ""
        sql = "select schemaname,relname,n_live_tup,n_tup_del,n_tup_ins,n_tup_upd,ration from pg_vacuum_stat " \
              + con + "order by time desc limit 10 "
        if db_name:
            con = "where db_name= '%s'" % db_name
        if start_time and end_time:
            con += "and time >= '%s' and time < '%s'" % (self.utctime(start_time), self.utctime(end_time))
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        pg_db_data = []
        for item in points:
            rst_dict = {"schemaname": item["schemaname"], "relname": item["relname"],
                        "n_live_tup": item["n_live_tup"], "n_tup_del": item["n_tup_del"],
                        "n_tup_ins": item["n_tup_ins"], "n_tup_upd": item["n_tup_upd"], "ration": item["ration"]}
            pg_db_data.append(rst_dict)
        return pg_db_data

    def get_pg_stat_bgwriter_data(self, db_name, start_time, end_time):
        con = ""
        sql = "select buffers_backend, buffers_backend_fsync,buffers_alloc,buffers_checkpoint,buffers_clean," \
              "checkpoint_sync_time,checkpoint_write_time,checkpoints_req,checkpoints_timed,maxwritten_clean from " \
              "pg_checkpoint_stat " \
              + con + "order by time desc limit 10 "
        if db_name:
            con = "where db_name= '%s'" % db_name
        if start_time and end_time:
            con += "and time >= '%s' and time < '%s'" % (self.utctime(start_time), self.utctime(end_time))
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        pg_db_data = []
        for item in points:
            rst_dict = {"buffers_backend": item["buffers_backend"], "buffers_backend_fsync": item["buffers_backend_fsync"],
                        "buffers_alloc": item["buffers_alloc"], "buffers_checkpoint": item["buffers_checkpoint"],
                        "buffers_clean": item["buffers_clean"], "checkpoint_sync_time": item["checkpoint_sync_time"],
                        "checkpoint_write_time": item["checkpoint_write_time"], "checkpoints_req": item["checkpoints_req"],
                        "checkpoints_timed": item["checkpoints_timed"], "maxwritten_clean": item["maxwritten_clean"]}
            pg_db_data.append(rst_dict)
        return pg_db_data

    def get_active_session_count(self):
        sql = """
                SELECT db_inst_name, db_name, last("active_session_count") as "active_session_count" FROM (SELECT max("db_session_stat_value") as "active_session_count" FROM "wbxdb_monitor_session" WHERE ("db_session_status" = 'ACTIVE' AND "db_service_name" != 'Backend') AND time > now() - 6h GROUP BY time(1m), "db_name", "db_inst_name" ) GROUP BY  "db_name", "db_inst_name"
                """
        # sql = "select * from wbxdb_monitor_session order by time desc limit 1"
        points = self._client.query(sql)
        result = []
        for item in points:
            result.append(item[0])
        return sorted(result, key=lambda dbitem: dbitem["active_session_count"], reverse=True)
