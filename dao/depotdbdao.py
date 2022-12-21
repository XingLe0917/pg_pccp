import logging
from typing import Optional
from datetime import datetime
from dao.wbxdaomanager import wbxdao
from dao.vo.depotdbvo import wbxdatabase, wbxshareplexchannel
from common.wbxexception import wbxexception
from sqlalchemy import  or_, and_

logger = logging.getLogger("DBAMONITOR")

class DepotDBDao(wbxdao):

    def getDatabaseInfo(self):
        session = self.getLocalSession()
        dbList = session.query(wbxdatabase).filter(and_(wbxdatabase.db_type == "PROD")). \
            filter(wbxdatabase.db_vendor == "POSTGRESQL").all()
        # dbList = session.query(wbxdatabase).filter(wbxdatabase.trim_host.like('%sjdbth39%')).all()
        return dbList

    def addDatabaseInfo(self, db):
        session = self.getLocalSession()
        session.add(db)

    def getDatabaseInfoByDBName(self, db_name):
        session = self.getLocalSession()
        dbvo = session.query(wbxdatabase).filter(wbxdatabase.db_name == db_name).first()
        return dbvo

    def getShareplexChannel(self):
        session = self.getLocalSession()
        channelList = session.query(wbxshareplexchannel).all()
        return channelList

    def getShareplexChannelByTargetDBAndPort(self, tgt_db, port):
        session = self.getLocalSession()
        channelList = session.query(wbxshareplexchannel).filter(
            and_(wbxshareplexchannel.tgt_db == tgt_db, wbxshareplexchannel.port == port)).all()
        return channelList

    def getShareplexChannelByDBName(self, db_name, hostnameList):
        session = self.getLocalSession()
        spch = session.query(wbxshareplexchannel).filter(
            or_(and_(wbxshareplexchannel.src_db == db_name, wbxshareplexchannel.src_host.in_(hostnameList)),
                and_(wbxshareplexchannel.tgt_db == db_name, wbxshareplexchannel.tgt_host.in_(hostnameList)))).all()
        return spch

    def getShareplexChannelByChannelid(self, channelid):
        session = self.getLocalSession()
        spch = session.query(wbxshareplexchannel).filter(wbxshareplexchannel.channelid == channelid).first()
        return spch

    def addShareplexChannel(self, spchannel):
        session = self.getLocalSession()
        session.add(spchannel)

    def deleteShareplexChannel(self, spchannel):
        session = self.getLocalSession()
        session.delete(spchannel)

    def getCheckCronStatus(self):
        session = self.getLocalSession()
        SQL = '''
           SELECT host_name,status,db_agent_exist,to_char(monitor_time,'YYYY-MM-DD hh24:mi:ss') monitor_time
            FROM wbxcronjobstatus
            ORDER BY host_name
              '''
        return session.execute(SQL).fetchall()

    def get_role_list(self):
        session = self.getLocalSession()
        sql = "select distinct role_name from ccp_role_page_info"
        rows = session.execute(sql).fetchall()
        result = []
        if not rows:
            raise wbxexception("Failed to get_role_list")
        for row in rows:
            result.append(row[0])
        return result

    def get_user_list_by_rolename(self, role_name):
        session = self.getLocalSession()
        sql = "select distinct username from ccp_user_role_info where role_name='" + role_name + "'"
        rows = session.execute(sql).fetchall()
        result = []
        for row in rows:
            result.append(row[0])
        return result

    def assign_role_to_user(self, username, role_name):
        session = self.getLocalSession()
        sql = "insert into ccp_user_role_info (username, role_name) values ('" + username + "','" + role_name + "')"
        logger.info(sql)
        session.execute(sql)

    def delete_user_from_role(self, username, role_name):
        session = self.getLocalSession()
        sql = "delete from ccp_user_role_info where username = '" + username + "' and role_name = '" + role_name + "'"
        logger.info(sql)
        session.execute(sql)

    def get_existed_url_list(self):
        session = self.getLocalSession()
        sql = "select distinct parent_page_dir, page_dir, page_name from ccp_role_page_info"
        rows = session.execute(sql).fetchall()
        rst_dict = {}
        for item in rows:
            rst_dict.update({item[2]: "".join([item[0], item[1]])})
        return rst_dict

    def add_page_to_depot(self, page_dir, parent_page_dir, page_name):
        session = self.getLocalSession()
        sql = "insert into ccp_role_page_info (page_dir, parent_page_dir, page_name, role_name, permission) values ('%s','%s', '%s', 'visitor', '1')" % (
            page_dir, parent_page_dir, page_name)
        logger.info(sql)
        session.execute(sql)
        sql = "insert into ccp_page_info (page_dir, parent_page_dir, page_name) values ('%s','%s', '%s')" % (
            page_dir, parent_page_dir, page_name)
        logger.info(sql)
        session.execute(sql)

    def delete_page_from_depot(self, page_dir, parent_page_dir, page_name):
        session = self.getLocalSession()
        sql = "delete from ccp_role_page_info where page_dir = '%s' and parent_page_dir = '%s' and page_name='%s'" % (
            page_dir, parent_page_dir, page_name)
        logger.info(sql)
        session.execute(sql)
        sql = "delete from ccp_page_info where page_dir = '%s' and parent_page_dir = '%s' and page_name='%s'" % (
            page_dir, parent_page_dir, page_name)
        logger.info(sql)
        session.execute(sql)

    def get_page_role_list(self):
        session = self.getLocalSession()
        sql = "select distinct parent_page_dir, page_dir, role_name, permission from ccp_role_page_info"
        rows = session.execute(sql).fetchall()
        return rows

    def update_role_permission(self, page_dir, parent_page_dir, role_name, permission):
        session = self.getLocalSession()
        sql = "select count(1) from ccp_role_page_info where page_dir='%s' and parent_page_dir='%s' and role_name='%s'" % (
            page_dir, parent_page_dir, role_name)
        rows = session.execute(sql).fetchall()
        if int(rows[0][0]) == 0:
            self.add_role_to_page(page_dir, parent_page_dir, role_name, permission)
        else:
            sql = "update ccp_role_page_info set permission=" + permission + " where page_dir='" + page_dir + \
                  "' and parent_page_dir='" + parent_page_dir + "' and role_name='" + role_name + "'"
            logger.info(sql)
            session.execute(sql)

    def add_role_to_page(self, page_dir, parent_page_dir, role_name, permission):
        session = self.getLocalSession()
        sql = "insert into ccp_role_page_info (page_dir, parent_page_dir, role_name, permission) values ('%s','%s','%s','%s')" % (page_dir, parent_page_dir, role_name, permission)
        logger.info(sql)
        session.execute(sql)

    def get_role_list_by_name(self, username):
        session = self.getLocalSession()
        SQL = "select count(1) from ccp_user_role_info"
        rows = session.execute(SQL).fetchall()
        if int(rows[0][0]) == 0:
            sql = "insert into ccp_user_role_info (username, role_name) values ('" + username + "','admin')"
            logger.info(sql)
            session.execute(sql)
        sql = "select distinct role_name from ccp_user_role_info where username='" + username + "'"
        rows = session.execute(sql).fetchall()
        rst_list = []
        for item in rows:
            rst_list.append(item[0])
        return rst_list

    def check_login_user(self, username):
        session = self.getLocalSession()
        sql = "select distinct username from ccp_user_role_info where username='%s'" % username
        rows = session.execute(sql).fetchall()
        return rows

    def get_access_dir(self, username):
        session = self.getLocalSession()
        sql = "select distinct p.parent_page_dir, p.page_dir from ccp_role_page_info p, ccp_user_role_info r where r.username='%s' and r.role_name=p.role_name and p.permission='1'" % username
        rows = session.execute(sql).fetchall()
        if not rows:
            raise wbxexception("find no page info by username %s" % username)
        return rows

    def get_access_favourite_dir(self, username):
        session = self.getLocalSession()
        sql = """
                            select distinct p.parent_page_dir, p.page_dir
                    from ccp_role_page_info p, ccp_user_role_info r, ccp_user_page_info u
                    where r.username='%s'
                    and r.username=u.username
                    and p.parent_page_dir=u.parent_page_dir
                    and p.page_dir=u.page_dir
                    and r.role_name=p.role_name
                    and p.permission='1'""" % username
        rows = session.execute(sql).fetchall()
        if not rows:
            raise wbxexception("find no page info by username %s" % username)
        return rows

    def add_user_favourite_page(self, username, page_name, page_dir, parent_page_dir):
        session = self.getLocalSession()
        sql = "insert into ccp_user_page_info (username, page_name, page_dir, parent_page_dir) values ('" + username + "','" + page_name + "','" + page_dir + "', '" + parent_page_dir + "')"
        logger.info(sql)
        session.execute(sql)

    def getCCPUserRole(self,cec):
        session = self.getLocalSession()
        SQL = "select count(1) from ccp_user_role_info"
        rows = session.execute(SQL).fetchall()
        if int(rows[0][0]) == 0:
            sql = "insert into ccp_user_role_info (username, role_name) values ('" + cec + "','admin')"
            logger.info(sql)
            session.execute(sql)
        SQL = '''select username,role_name,
        to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') create_time,
        to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') update_time
         from ccp_user_role_info where username =:cec'''
        rows = session.execute(SQL, {"cec": cec}).fetchall()
        return rows

    def delete_user_favourite_page(self, username, page_name, page_dir, parent_page_dir):
        session = self.getLocalSession()
        sql = "delete from ccp_user_page_info where username='%s' and page_name='%s' and page_dir='%s' and parent_page_dir='%s'" % (
            username, page_name, page_dir, parent_page_dir)
        logger.info(sql)
        session.execute(sql)

    def getCCPUserList(self):
        session = self.getLocalSession()
        SQL = '''select username,role_name,
        to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') create_time,
        to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') update_time
        from ccp_user_role_info '''
        rows = session.execute(SQL).fetchall()
        return rows

    def getCronStatusList(self,host_name):
        session = self.getLocalSession()
        if host_name:
            SQL = '''
            select host_name, to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastupdatetime,
            to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
            CASE WHEN NOW() - INTERVAL '10 min' < monitor_time THEN 'running'
                ELSE  'stopped'
            END status
            from wbxcronjobstatus where host_name like '%%%s%%'
            ''' %(host_name)
        else:
            SQL = '''
            select host_name, to_char(monitor_time, 'yyyy-MM-dd HH24:mi:ss') lastupdatetime,
            to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
            CASE WHEN NOW() - INTERVAL '10 min' < monitor_time THEN 'running'
                ELSE  'stopped'
            END status
            from wbxcronjobstatus  '''
        rows = session.execute(SQL).fetchall()
        return rows

    def insertCronStatus(self,host_name):
        session = self.getLocalSession()
        SQL = '''
        insert into wbxcronjobstatus(host_name,status,db_agent_exist,monitor_time) values ('%s','running','1',NOW());
        '''%(host_name)
        session.execute(SQL)

    def updateCronStatus(self,host_name):
        session = self.getLocalSession()
        SQL =''' update wbxcronjobstatus set monitor_time=current_timestamp where host_name = '%s' ''' %(host_name)
        session.execute(SQL)

    def get_cronStatusForAlertList(self):
        session = self.getLocalSession()
        SQL = '''
        select host_name, to_char(monitor_time, 'yyyy-MM-dd HH24:mi:ss') lastupdatetime
        from wbxcronjobstatus where NOW() - INTERVAL '10 min' >= monitor_time
        '''
        rows = session.execute(SQL).fetchall()
        return rows

    def getBackupStatusList(self,host_name):
        session = self.getLocalSession()
        if host_name:
            SQL = '''
            select host_name, full_backup_status, incr_backup_status,
            to_char(last_full_backup_time, 'yyyy-MM-dd HH24:mi:ss') lastet_full_backup,
            to_char(last_incr_backup_time, 'yyyy-MM-dd HH24:mi:ss') lastet_incr_backup
            from wbxbackupjobstatus where host_name like '%%%s%%'
            ''' %(host_name)
        else:
            SQL = '''
            select host_name, full_backup_status, incr_backup_status,
            to_char(last_full_backup_time, 'yyyy-MM-dd HH24:mi:ss') lastet_full_backup,
            to_char(last_incr_backup_time, 'yyyy-MM-dd HH24:mi:ss') lastet_incr_backup
            from wbxbackupjobstatus '''
        rows = session.execute(SQL).fetchall()
        return rows

    def insertBackupStatus(self,host_name,db_name,full_backup_status,incr_backup_status,):
        session = self.getLocalSession()
        SQL = ""
        if full_backup_status:
            SQL = ''' insert into wbxbackupjobstatus(host_name,db_name,full_backup_status,last_full_backup_time)
                    values ('%s','%s','%s',current_timestamp) ''' %(host_name,db_name,full_backup_status)
        elif incr_backup_status:
            SQL = ''' insert into wbxbackupjobstatus(host_name,db_name,incr_backup_status,last_incr_backup_time)
                                values ('%s','%s','%s',current_timestamp) ''' % (host_name,db_name,incr_backup_status)
        logger.info(SQL)
        session.execute(SQL)

    def updateBackupStatus(self,host_name,db_name,full_backup_status,incr_backup_status):
        session = self.getLocalSession()
        SQL = ""
        if full_backup_status:
            SQL = ''' update wbxbackupjobstatus set full_backup_status='%s', last_full_backup_time=current_timestamp where host_name = '%s' and db_name='%s' ''' % (
            full_backup_status, host_name,db_name)
        elif incr_backup_status:
            SQL = ''' update wbxbackupjobstatus set incr_backup_status='%s', last_incr_backup_time=current_timestamp where host_name = '%s' and db_name='%s' ''' % (
            incr_backup_status, host_name,db_name)
        logger.info(SQL)
        session.execute(SQL)

    def get_database_info_by_dbname(self,db_name):
        session = self.getLocalSession()
        sql = """ select db_name from database_info where db_name = '%s' """ % db_name
        rows = session.execute(sql).fetchall()
        return rows

    def insert_database_info(self,databaseVo):
        session = self.getLocalSession()
        db_name = databaseVo.db_name
        host_name = databaseVo.host_name
        cluster_name = databaseVo.cluster_name
        db_vendor = databaseVo.db_vendor
        db_version = databaseVo.db_version
        db_type = databaseVo.db_type
        application_type = databaseVo.application_type
        appln_support_code = databaseVo.appln_support_code
        db_home = databaseVo.db_home
        listener_port = databaseVo.listener_port
        monitored = databaseVo.monitored
        wbx_cluster = databaseVo.wbx_cluster
        web_domain = databaseVo.web_domain
        sql = '''
                INSERT INTO database_info(db_name,host_name,cluster_name,db_vendor,db_version,db_type,application_type,appln_support_code,
                db_home,listener_port,monitored,wbx_cluster,web_domain,createtime,lastmodifiedtime)
                VALUES('%s','%s','%s','%s','%s','%s','%s','%s',
                '%s','%s','%s','%s','%s',now(),now())
                ''' % (db_name, host_name, cluster_name, db_vendor, db_version, db_type,application_type, appln_support_code,
                       db_home, listener_port, monitored, wbx_cluster, web_domain)
        logger.info(sql)
        session.execute(sql)

    def update_database_info(self,databaseVo):
        session = self.getLocalSession()
        db_name = databaseVo.db_name
        host_name = databaseVo.host_name
        cluster_name = databaseVo.cluster_name
        db_vendor = databaseVo.db_vendor
        db_version = databaseVo.db_version
        db_type = databaseVo.db_type
        application_type = databaseVo.application_type
        appln_support_code = databaseVo.appln_support_code
        db_home = databaseVo.db_home
        listener_port = databaseVo.listener_port
        monitored = databaseVo.monitored
        wbx_cluster = databaseVo.wbx_cluster
        web_domain = databaseVo.web_domain
        sql = '''
        update database_info set host_name='%s',cluster_name='%s',db_vendor='%s',db_version='%s',db_type='%s',application_type='%s',appln_support_code='%s',
                                db_home='%s',listener_port='%s',monitored='%s',wbx_cluster='%s',web_domain='%s'
                                where db_name = '%s'
            ''' % (host_name, cluster_name, db_vendor, db_version, db_type, application_type, appln_support_code,
            db_home, listener_port, monitored, wbx_cluster, web_domain, db_name)
        logger.info(sql)
        session.execute(sql)

    def get_host_info_by_cname(self,cname):
        session = self.getLocalSession()
        sql = '''
        select cname from host_info where cname = '%s'
        '''  % (cname)
        rows = session.execute(sql).fetchall()
        return rows

    def insert_host_info(self,hostInfoVo):
        session = self.getLocalSession()
        cname = hostInfoVo.cname
        host_name = hostInfoVo.host_name
        domain = hostInfoVo.domain
        site_code = hostInfoVo.site_code
        region_name = hostInfoVo.region_name
        public_ip = hostInfoVo.public_ip
        private_ip = hostInfoVo.private_ip
        os_type_code = hostInfoVo.os_type_code
        processor = hostInfoVo.processor
        kernel_release = hostInfoVo.kernel_release
        hardware_platform = hostInfoVo.hardware_platform
        physical_cpu = hostInfoVo.physical_cpu
        cores = hostInfoVo.cores
        cpu_model = hostInfoVo.cpu_model
        flag_node_virtual = hostInfoVo.flag_node_virtual
        comments = hostInfoVo.comments
        ssh_port = hostInfoVo.ssh_port
        install_date = hostInfoVo.install_date
        sql = '''INSERT INTO host_info(cname, host_name, domain, site_code, region_name,public_ip, private_ip, os_type_code, processor, kernel_release, hardware_platform,
                        physical_cpu, cores, cpu_model, flag_node_virtual, install_date, comments, ssh_port,createtime,lastmodifiedtime)
                        VALUES('%s','%s','%s','%s','%s',
                        '%s','%s','%s','%s','%s','%s',
                        '%s','%s','%s','%s',TIMESTAMP '%s','%s',%s,now(),now())
                        ''' % (cname, host_name, domain, site_code, region_name,public_ip, private_ip, os_type_code, processor, kernel_release, hardware_platform,
                               physical_cpu, cores, cpu_model, flag_node_virtual, install_date, comments, ssh_port)
        logger.info(sql)
        session.execute(sql)

    def update_host_info(self,hostInfoVo):
        session = self.getLocalSession()
        cname = hostInfoVo.cname
        host_name = hostInfoVo.host_name
        domain = hostInfoVo.domain
        site_code = hostInfoVo.site_code
        region_name = hostInfoVo.region_name
        public_ip = hostInfoVo.public_ip
        private_ip = hostInfoVo.private_ip
        os_type_code = hostInfoVo.os_type_code
        processor = hostInfoVo.processor
        kernel_release = hostInfoVo.kernel_release
        hardware_platform = hostInfoVo.hardware_platform
        physical_cpu = hostInfoVo.physical_cpu
        cores = hostInfoVo.cores
        cpu_model = hostInfoVo.cpu_model
        flag_node_virtual = hostInfoVo.flag_node_virtual
        comments = hostInfoVo.comments
        ssh_port = hostInfoVo.ssh_port
        install_date = hostInfoVo.install_date
        sql = '''
                update host_info set host_name='%s',domain='%s',site_code='%s',region_name='%s',public_ip='%s',private_ip='%s',os_type_code='%s',processor='%s',
                kernel_release='%s',hardware_platform='%s',physical_cpu='%s',cores='%s',cpu_model='%s',flag_node_virtual='%s',comments='%s',
                ssh_port='%s',install_date=TIMESTAMP '%s',lastmodifiedtime=now()
                where cname = '%s' ''' % (host_name, domain, site_code, region_name,public_ip, private_ip, os_type_code, processor, kernel_release, hardware_platform,
                       physical_cpu, cores, cpu_model, flag_node_virtual, comments, ssh_port,install_date, cname)
        logger.info(sql)
        session.execute(sql)

    def get_depot_db_list(self,cname,db_name):
        session = self.getLocalSession()
        SQL = '''
        select di.DB_NAME,di.CNAME,di.CLUSTER_NAME,di.DB_VENDOR,DB_VERSION,
        di.DB_TYPE,di.APPLICATION_TYPE,di.APPLN_SUPPORT_CODE,di.DB_HOME,di.LISTENER_PORT,di.MONITORED,di.WBX_CLUSTER,di.WEB_DOMAIN
        from database_info di, host_info hi
        where di.cname = hi.cname
        '''
        if db_name:
            SQL += " and di.db_name like '%%%s%%' " % (db_name)
        if cname:
            SQL += " and di.cname like '%%%s%%' " % (cname)
        logger.info(SQL)
        rows = session.execute(SQL).fetchall()
        return rows


    def get_depot_host_list(self,cname,host_name):
        session = self.getLocalSession()
        SQL = '''
        select hi.CNAME,hi.HOST_NAME,hi.DOMAIN,hi.SITE_CODE,hi.REGION_NAME,hi.PUBLIC_IP,
        hi.PRIVATE_IP,hi.OS_TYPE_CODE,hi.PROCESSOR,hi.KERNEL_RELEASE,hi.HARDWARE_PLATFORM,hi.PHYSICAL_CPU,hi.CORES,hi.CPU_MODEL,
        hi.FLAG_NODE_VIRTUAL,hi.INSTALL_DATE,hi.COMMENTS,hi.SSH_PORT
        from database_info di, host_info hi
        where di.cname = hi.cname
        '''
        if host_name:
            SQL += " and hi.host_name like '%%%s%%' " % (host_name)
        if cname :
            SQL += " and hi.cname like '%%%s%%' " % (cname)
        logger.info(SQL)
        rows = session.execute(SQL).fetchall()
        return rows

    def get_depot_db(
        self,
        *,
        db_name: Optional[str] = None,
        db_type: Optional[str] = None
    ):
        session = self.getLocalSession()
        SQL = '''
        select
            db_name, host_name, listener_port
        from database_info
        where 1=1
        '''
        if db_name:
            SQL += f" and database_info.db_name = '{db_name}' "
        if db_type:
            SQL += f" and database_info.db_type = '{db_type}' "
        logger.info(SQL)
        row = session.execute(SQL).fetchone()
        return row

    def get_depot_db_info_list(self):
        session = self.getLocalSession()
        SQL = '''
        select
            di.db_name,di.host_name,hi.cname,di.db_type,di.application_type,
            di.appln_support_code,di.wbx_cluster,di.web_domain
        from database_info di, host_info hi
        where di.host_name = hi.host_name
        order by di.createtime;
        '''
        rows = session.execute(SQL).fetchall()
        return rows

    def get_depot_manage_list(self,host_name,db_name,cname):
        session = self.getLocalSession()
        SQL = '''
        select di.DB_NAME,di.host_name,hi.cname,di.CLUSTER_NAME,di.DB_VENDOR,di.DB_VERSION,
        di.DB_TYPE,di.APPLICATION_TYPE,di.APPLN_SUPPORT_CODE,di.DB_HOME,di.LISTENER_PORT,di.MONITORED,di.WBX_CLUSTER,di.WEB_DOMAIN,
    hi.HOST_NAME,hi.DOMAIN,hi.SITE_CODE,hi.REGION_NAME,hi.PUBLIC_IP,
        hi.PRIVATE_IP,hi.OS_TYPE_CODE,hi.PROCESSOR,hi.KERNEL_RELEASE,hi.HARDWARE_PLATFORM,hi.PHYSICAL_CPU,hi.CORES,hi.CPU_MODEL,
        hi.FLAG_NODE_VIRTUAL,to_char(hi.INSTALL_DATE, 'yyyy-MM-dd HH24:mi:ss') INSTALL_DATE,hi.COMMENTS,hi.SSH_PORT
        from database_info di, host_info hi
        where di.host_name = hi.host_name
                '''
        if db_name:
            SQL += " and di.db_name like '%%%s%%'  " % (db_name)
        if host_name:
            SQL += " and hi.host_name like '%%%s%%' " % (host_name)
        if cname:
            SQL += " and hi.cname like '%%%s%%' " % (cname)
        logger.info(SQL)
        rows = session.execute(SQL).fetchall()
        return rows

    def get_partition_job_host(self, *, host_name, db_name):
        sess = self.getLocalSession()
        sql = """
            select host_name, db_name, add_log, drop_log from wbxpartitionjobstatus
            where host_name='{host_name}' and db_name='{db_name}';
        """.format(host_name=host_name, db_name=db_name)
        data = sess.execute(sql).fetchone()
        return data

    def insert_partition_job_status(self, *, params):
        sess = self.getLocalSession()
        sql = """
            insert into wbxpartitionjobstatus (host_name,db_name,{status_field},{time_field},{log_field})
            values ('{host_name}','{db_name}','{op_status}','{op_time}','{log_content}');
        """.format(**params)
        sess.execute(sql)

    def update_partition_job_status(self, *, params):
        sess = self.getLocalSession()
        sql1 = """
            update wbxpartitionjobstatus
            set
            {status_field}='{op_status}',
            {time_field}='{op_time}',
            lastmodifiedtime='{lastmodifiedtime}',
        """
        if params["jobtype"] == "BEGIN":
            sql2 = """
                {log_field}=''
            """
        else:
            sql2 = """
                {log_field}=concat({log_field},'{log_content}')
            """
        sql3 = """
            where host_name='{host_name}' and db_name='{db_name}'
        """
        sql = (sql1 + sql2 + sql3).format(**params, lastmodifiedtime=datetime.now())
        sess.execute(sql)


    def list_partition_job_status(self, *, params):
        results = []
        sess = self.getLocalSession()
        sql = """
            select host_name, db_name,
                   add_status,
                   to_char(add_begin_time, 'yyyy-MM-dd HH24:mi:ss') add_begin_time,
                   to_char(add_end_time, 'yyyy-MM-dd HH24:mi:ss') add_end_time,
                   drop_status,
                   to_char(drop_begin_time, 'yyyy-MM-dd HH24:mi:ss') drop_begin_time,
                   to_char(drop_end_time, 'yyyy-MM-dd HH24:mi:ss') drop_end_time,
                   add_log, drop_log,
                   to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
                   to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
            from wbxpartitionjobstatus
        """
        sql_condition = """
            where 1=1
        """
        for key, val in params.items():
            sql_condition += f""" and {key} = '{val}' """

        sql_end = f"""
            order by createtime desc;
        """

        sql += (sql_condition + sql_end)

        data = sess.execute(sql).fetchall()
        for row in data:
            data_dict = dict(
                host_name=row[0],
                db_name=row[1],
                add_status=row[2],
                add_begin_time=row[3],
                add_end_time=row[4],
                drop_status=row[5],
                drop_begin_time=row[6],
                drop_end_time=row[7],
                add_log=row[8],
                drop_log=row[9],
                createtime=row[10],
                lastmodifiedtime=row[11],
            )
            results.append(data_dict)
        return results

    def get_shareplexInfoList(self):
        session = self.getLocalSession()
        sql = '''
         select ss.src_site_code, ss.tgt_site_code,ss.queue_name,ss.src_db,ss.src_db_type,ss.src_application_type,ss.src_appln_support_code,ss.src_web_domain,
    ss.tgt_db,ss.tgt_db_type,ss.tgt_application_type,ss.tgt_appln_support_code,ss.port,ss.src_schema,ss.tgt_schema,ss.tgt_web_domain,
    sai.schematype src_schematype,tai.schematype tgt_schematype,
        sai.password_vault_path src_vault_path,tai.password_vault_path tgt_vault_path
        from(
        select distinct shi.site_code src_site_code,thi.site_code tgt_site_code,si.qname queue_name,
                si.src_db, sdi.db_type src_db_type,sdi.application_type src_application_type,sdi.appln_support_code src_appln_support_code,sdi.web_domain src_web_domain,
                si.tgt_db, tdi.db_type tgt_db_type,tdi.application_type tgt_application_type,tdi.appln_support_code tgt_appln_support_code,tdi.web_domain tgt_web_domain,si.port,
                si.src_schema,
                si.tgt_schema
                from shareplex_info si,database_info sdi,database_info tdi,host_info shi,host_info thi
                where si.src_db= sdi.db_name
                and si.tgt_db= tdi.db_name
                and si.src_host=shi.host_name
                and si.tgt_host=thi.host_name
                and tdi.db_type in ('PROD','BTS_PROD')
                and sdi.db_type in ('PROD','BTS_PROD')
        )ss , appln_pool_info sai, appln_pool_info tai
        where ss.src_db= sai.db_name
        and ss.src_schema = sai.schemaname
        and ss.tgt_db = tai.db_name
        and ss.tgt_schema = tai.schemaname
        '''
        logger.info(sql)
        rows = session.execute(sql).fetchall()
        return rows

    def add_shareplexInfoList(self,src_host, src_db, port, tgt_host, tgt_db, qname,
                             src_splex_sid, tgt_splex_sid, src_schema, tgt_schema):
        session = self.getLocalSession()
        sql = '''
                        insert into shareplex_info(src_host,src_db,port,tgt_host,tgt_db,qname,
                        created_by,modified_by,src_splex_sid,tgt_splex_sid,src_schema,tgt_schema) VALUES
                        (:src_host,:src_db,:port,:tgt_host,:tgt_db,:qname,'auto','auto',:src_splex_sid,:tgt_splex_sid,:src_schema,:tgt_schema)
                        '''

        session.execute(sql, {"src_host": src_host, "src_db": src_db,
                              "port": port, "tgt_host": tgt_host, "tgt_db": tgt_db, "qname": qname,
                              "src_splex_sid": src_splex_sid, "tgt_splex_sid": tgt_splex_sid, "src_schema": src_schema,
                              "tgt_schema": tgt_schema})

    def get_ccp_user_role_info_count(self):
        session = self.getLocalSession()
        sql = '''
        select * from ccp_user_role_info where role_name= 'admin'
        '''
        rows = session.execute(sql).fetchall()
        return rows

    def insert_admin(self,cec):
        session = self.getLocalSession()
        sql = '''
        insert into ccp_user_role_info(username,role_name) values ('%s','admin')
        ''' %(cec)
        logger.info(sql)
        session.execute(sql)

    def getDBInfo(self,db_name):
        session = self.getLocalSession()
        sql = '''
        select db_name,host_name,appln_support_code from database_info where lower(db_name) = '%s'
        ''' %(db_name)
        rows = session.execute(sql).fetchall()
        return rows

    def getSchemaNameByDB(self,db_name,schematype):
        session = self.getLocalSession()
        sql = '''
                select distinct schemaname from appln_pool_info where lower(db_name) = '%s' and schematype = '%s'  and lower(schemaname) NOT LIKE '%%_clone' 
                ''' % (db_name,schematype)
        rows = session.execute(sql).fetchone()
        return rows

    def getVaultPath(self,db_name,schema):
        session = self.getLocalSession()
        sql = '''
        select schemaname,password,password_vault_path,schematype from appln_pool_info where lower(db_name) = '%s' and schemaname= '%s'
                        ''' % (db_name, schema)
        rows = session.execute(sql).fetchone()
        return rows

    def getApplnPoolInfo(self,db_name,schemaname):
        session = self.getLocalSession()
        sql = '''
        select db_name,appln_support_code,schemaname,password,password_vault_path from appln_pool_info where db_name = '%s' and schemaname='%s'
        '''%(db_name.lower(),schemaname.lower())
        rows = session.execute(sql).fetchall()
        return rows

    def insert_appln_pool_info(self,db_name,appln_support_code,schemaname,password,password_vault_path,created_by,modified_by,schematype):
        """db_name,schemaname lower"""
        session = self.getLocalSession()
        sql = '''
                insert into appln_pool_info(db_name,appln_support_code,schemaname,password,password_vault_path,
                            created_by,modified_by,schematype)
           values ('%s','%s','%s','%s','%s','%s','%s','%s')
                ''' % (db_name.lower(),appln_support_code,schemaname.lower(),password,password_vault_path,created_by,modified_by,schematype)
        logger.info(sql)
        session.execute(sql)

    def update_appln_pool_info(self,db_name,appln_support_code,schemaname,password,password_vault_path,modified_by,schematype):
        session = self.getLocalSession()
        sql = '''
        update appln_pool_info set appln_support_code='%s',password='%s',password_vault_path='%s',modified_by='%s',schematype='%s',lastmodifiedtime=NOW()
        where db_name='%s' and schemaname='%s'
        ''' %(appln_support_code,password,password_vault_path,modified_by,schematype,db_name.lower(),schemaname.lower())
        logger.info(sql)
        session.execute(sql)

    def getReplicationChannel(self,src_db,tgt_db,port):
        session = self.getLocalSession()
        sql = '''
        select src_host,src_db,port,tgt_host,tgt_db,qname from shareplex_info where src_db='%s' and tgt_db='%s' and port = %s;
        ''' %(src_db,tgt_db,port)
        rows = session.execute(sql).fetchall()
        return rows

    def add_alertdetail(self, wbxmonitoralertdetailVo):
        session = self.getLocalSession()
        sql = '''
           insert into wbxmonitoralertdetail(alertdetailid,alerttitle,priority,host_name,db_name,splex_port,alert_type,job_name,parameter)
                                         values('%s','%s','%s','%s','%s','%s','%s','%s','%s')
           ''' % (
        wbxmonitoralertdetailVo.alertdetailid,
        wbxmonitoralertdetailVo.alerttitle, wbxmonitoralertdetailVo.priority,
        wbxmonitoralertdetailVo.host_name, wbxmonitoralertdetailVo.db_name,
        wbxmonitoralertdetailVo.splex_port,
        wbxmonitoralertdetailVo.alert_type,wbxmonitoralertdetailVo.job_name,
        wbxmonitoralertdetailVo.parameter)
        logger.info(sql)
        session.execute(sql)

    def getdbcommonlist(self,**kargs):
        session = self.getLocalSession()
        db_type=kargs["db_type"]
        appln_support_code=kargs["appln_support_code"]
        application_type=kargs["application_type"]
        schematype=kargs["schematype"]
        web_domain=kargs["webdomain"]
        pool=kargs["pool"]
        sql = '''
        select schemaname,schematype,hi.private_ip,'meetpaas/mcceng' password_vault_namespace,
         password_vault_path||'/'||case when lower(schemaname) like 'test%%' then schematype else regexp_replace(schemaname,'_clone$','') end as password_vault_path
          from database_info di
          join appln_pool_info ai on di.db_name=ai.db_name
		  join host_info hi on di.host_name=hi.cname
		  left join appln_mapping_info am on di.db_name = am.db_name
        where schemaname not like '%%_clone'
          and lower(di.db_type)=lower('%s')
          and lower(di.appln_support_code)=lower('%s')
          and (lower(ai.schematype)=lower('%s') or 'all'=lower('%s'))         
        ''' %(db_type,appln_support_code,schematype,schematype)
        if web_domain:
           sql=sql+ '''  and lower(di.web_domain) = lower('%s')''' %(web_domain)
        if application_type:
            sql = sql + '''  and lower(di.application_type) = lower('%s')''' % (application_type)
        if pool:
            sql = sql + '''  and lower(am.mapping_name) = lower('%s')''' % (pool)
        rows = session.execute(sql).fetchall()
        logger.info(sql)
        return rows

    def get_wbxmonitoralert_type(self):
        session = self.getLocalSession()
        sql = '''select distinct alert_type from wbxmonitoralert '''
        rows = session.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def getWbxmonitoralert(self, db_name, status, host_name, alert_type, start_date, end_date):
        session = self.getLocalSession()
        sql = '''
        select t1.alertid,t1.alerttitle,t1.status,t1.autotaskid,t1.host_name,t1.db_name,t1.splex_port,
        t1.alert_type,t1.job_name,t1.parameter,
        to_char(t1.first_alert_time,'yyyy-MM-dd HH24:mi:ss') first_alert_time, 
        to_char(t1.last_alert_time,'yyyy-MM-dd HH24:mi:ss') last_alert_time, 
        t1.alert_count,
        t1.attemptcount,
        to_char(t1.fixtime,'yyyy-MM-dd HH24:mi:ss') fixtime, 
        to_char(t1.createtime,'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(t1.lastmodifiedtime,'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
        from wbxmonitoralert t1 
        where t1.first_alert_time>=to_date('%s', 'YYYY-MM-DD') 
        and t1.first_alert_time<to_date('%s', 'yyyy-MM-DD')
        ''' % (start_date, end_date)
        if db_name:
            sql += " and t1.db_name like '%%%s%%' " % (db_name)
        if host_name:
            sql += " and t1.host_name like '%%%s%%' " % (host_name)
        if status:
            sql += " and t1.status = '%s' " % (status)
        if alert_type:
            sql += " and t1.alert_type = '%s' " % (alert_type)
        sql += " order by t1.last_alert_time desc"
        logger.info(sql)
        rows = session.execute(sql).fetchall()
        return rows

    def getWbxmonitoralertdetail(self, alertid):
        session = self.getLocalSession()
        sql = '''
                select alertdetailid,alerttitle,host_name,db_name,splex_port,alert_type,job_name,parameter,
                to_char(alerttime,'yyyy-MM-dd HH24:mi:ss') alerttime,
                to_char(createtime,'yyyy-MM-dd HH24:mi:ss') createtime,
                to_char(lastmodifiedtime,'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
                from wbxmonitoralertdetail 
                where alertid = '%s'
                order by alerttime desc
               ''' % (alertid)
        rows = session.execute(sql).fetchall()
        return rows

    def get_channel_info(self,src_host, src_db, port, tgt_host, tgt_db, qname, src_schema, tgt_schema):
        session = self.getLocalSession()
        sql = '''
        select src_host,src_db,port,tgt_host,tgt_db,qname,src_schema,tgt_schema from shareplex_info 
        where src_db='%s' and tgt_db='%s' and port = %s and src_host='%s'
         and tgt_host='%s' and qname= '%s' and src_schema='%s' and tgt_schema='%s'
        ''' %(src_db,tgt_db,port,src_host,tgt_host,qname,src_schema,tgt_schema)
        logger.info(sql)
        rows = session.execute(sql).fetchall()
        return rows

    def get_homepage_server_info(self):
        session = self.getLocalSession()
        sql = """
        select 'pg_' || db_version || '_db',count(db_name) from database_info group by db_version 
        union 
        select 'ora_' || decode(db_version,'19.0.0.0.0','19c') || '_db',count(db_name) from wbxora.database_info group by db_version
        """
        rows = session.execute(sql).fetchall()
        if not rows:
            return {"total": 0}
        total_db_count = sum([int(row[1]) for row in rows])
        server_info_dict = {"total": total_db_count}
        server_info_dict.update({"data": [{"value": row[1], "name": row[0]} for row in rows]})
        return server_info_dict

    def get_db_count_info(self):
        session = self.getLocalSession()
        sql = """
        select 'postgres_db',count(db_name) from database_info 
        union 
        select 'oracle_db',count(db_name) from wbxora.database_info
        """
        rows = session.execute(sql).fetchall()
        if not rows:
            return {"total": 0}
        total_db_count = sum([int(row[1]) for row in rows])
        server_info_dict = {"total": total_db_count}
        server_info_dict.update({"data": [{"value": row[1], "name": row[0]} for row in rows]})
        return server_info_dict

    def get_db_type_count_info(self):
        session = self.getLocalSession()
        pg_sql = """
        select appln_support_code, count(db_name) from database_info group by appln_support_code
        """
        pg_rows = session.execute(pg_sql).fetchall()
        pg_data_dict = dict(zip([row[0].upper() for row in pg_rows], [int(row[1]) for row in pg_rows]))
        ora_sql = """
        select appln_support_code, count(db_name) from wbxora.database_info group by appln_support_code
        """
        ora_rows = session.execute(ora_sql).fetchall()
        ora_data_dict = dict(zip([row[0] for row in ora_rows], [int(row[1]) for row in ora_rows]))
        data_dict = pg_data_dict
        for _k, _v in ora_data_dict.items():
            if _k in data_dict.keys():
                data_dict[_k] += _v
            else:
                data_dict.update({
                    _k: _v
                })
        if not data_dict:
            return {"total": 0}
        data_list = []
        for _k, _v in data_dict.items():
            data_list.append({
                "name": _k,
                "value": _v
            })
        total_db_count = sum(list(data_dict.values()))
        server_info_dict = {"total": total_db_count}
        server_info_dict.update({"data": data_list})
        return server_info_dict

    def get_rencent_alert(self):
        session = self.getLocalSession()
        sql = """
select * from (select alertid,alerttitle,host_name,db_name,splex_port,alert_type,job_name,to_char(alerttime,'YYYY-MM-DD hh24:mi:ss') alert_time,to_char(createtime,'YYYY-MM-DD hh24:mi:ss') create_time from wbxmonitoralertdetail where createtime> now()- INTERVAL '7 day' order by createtime desc) as a limit 6
        """
        rows = session.execute(sql).fetchall()
        data = []
        for row in rows:
            data.append({
                "alertid": row[0],
                "alerttitle": row[1],
                "host_name": row[2],
                "db_name": row[3],
                "splex_port": row[4],
                "alert_type": row[5],
                "job_name": row[6],
                "alert_time": row[7],
                "create_time": row[8],
            })
        return data
