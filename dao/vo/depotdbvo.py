from datetime import datetime

from sqlalchemy import Column,Integer,String, DateTime, TIMESTAMP, func, text, select
from sqlalchemy.orm import column_property
from dao.vo.wbxvo import Base
from common.wbxutil import wbxutil

class wbxoraexception(object):
    ORA_INVALID_USERNAME_PASSWORD="ORA-01017"
    ORA_CONNECTION_TIMEOUT = "ORA-12170"

class wbxdatabasemanager(object):
    APPLN_SUPPORT_CODE_WEBDB= "WEB"
    APPLN_SUPPORT_CODE_TAHOEDB= "TEL"
    APPLN_SUPPORT_CODE_CONFIGDB= "CONFIG"
    APPLN_SUPPORT_CODE_BILLINGDB= "OPDB"
    APPLN_SUPPORT_CODE_TEODB = "TEO"
    APPLN_SUPPORT_CODE_GLOOKUPDB = "LOOKUP"
    APPLN_SUPPORT_CODE_DIAGNSDB = "DIAGNS"
    APPLN_SUPPORT_CODE_MMP = "MMP"
    APPLN_SUPPORT_CODE_MEDIATE = "MEDIATE"

    DC_SJC="SJC02"
    DC_AMS="AMS01"
    DC_NRT="NRT02"

    DB_TYPE_PROD="PROD"
    DB_TYPE_BTS="BTS_PROD"

    APPLICATION_TYPE_PRI="PRI"
    APPLICATION_TYPE_GSB="GSB"

    def __init__(self):
        self.dbtypedict={}
        self.dbdict={}

class wbxloginuser(Base):
    __tablename__ = "host_user_info"
    host_name = Column(String(30), primary_key=True)
    trim_host = Column(String(30))
    username = Column(String(30))
    pwd = Column(String(64))
    createtime = Column(DateTime, default=func.now())
    lastmodifieddate = Column(DateTime, default=func.now(), onupdate=func.now())

class wbxserver(Base):
    __tablename__ = "host_info"
    cname = Column(String, primary_key=True)
    host_name = Column(String)
    domain = Column(String)
    site_code = Column(String)
    region_name = Column(String)
    public_ip = Column(String)
    private_ip = Column(String)
    os_type_code = Column(String(30))
    processor = Column(String(15))
    kernel_release = Column(String(30))
    hardware_platform = Column(String(30))
    physical_cpu = Column(String(22))
    cores = Column(String(22))
    cpu_model = Column(String(50))
    flag_node_virtual = Column(String(1))
    install_date = Column(TIMESTAMP, default=func.now())
    comments = Column(String(100))
    ssh_port = Column(String(15))
    createtime = Column(TIMESTAMP, default=func.now())
    lastmodifiedtime = Column(TIMESTAMP, default=func.now(), onupdate=func.now())


class wbxdatabase(Base):
    __tablename__ = "database_info"
    db_name = Column(String, primary_key=True)
    host_name = Column(String)
    cluster_name = Column(String)
    db_vendor = Column(String(20), default="POSTGRESQL")
    db_version = Column(String(10))
    db_type = Column(String(10))
    application_type = Column(String(10))
    appln_support_code = Column(String(15))
    db_home = Column(String(100))
    listener_port = Column(String(22))
    monitored = Column(String(1), default='Y')
    wbx_cluster = Column(String(25))
    web_domain = Column(String(13))
    createtime = Column(TIMESTAMP, default=func.now())
    lastmodifiedtime = Column(TIMESTAMP, default=func.now())


class wbxadbmon(Base):
    __tablename__ = "wbxadbmon"
    src_host = Column(String(50), primary_key=True)
    src_db=Column(String(30), primary_key=True)
    port= Column(String(10), primary_key=True)
    replication_to=Column(String(25), primary_key=True)
    tgt_host = Column(String(50), primary_key=True)
    tgt_db = Column(String(30), primary_key=True)
    lastreptime=Column(DateTime)
    montime = Column(DateTime)

    def getSourceDBID(self):
        return "%s_%s" % (self.src_host[0:-1], self.src_db)

    def getTargetDBID(self):
        return "%s_%s" % (self.tgt_host[0:-1], self.tgt_db)

    def isKafkaChannel(self):
        return self.tgt_db.find('kafka') >= 0

    def __str__(self):
        return "wbxadbmon: src_host=%s, src_db=%s, port=%s, replication_to=%s, tgt_host=%s,tgt_db=%s, lastreptime=%s" % (
        self.src_host, self.src_db, self.port, self.replication_to, self.tgt_host, self.tgt_db, wbxutil.convertDatetimeToString(self.lastreptime))


class wbxshareplexchannel(Base):
    __tablename__ = "shareplex_info"
    src_host = Column(String(50))
    src_db = Column(String(30), primary_key=True)
    port = Column(String(22), primary_key=True)
    qname = Column(String(64), primary_key=True)
    tgt_host = Column(String(50))
    tgt_db = Column(String(30), primary_key=True)
    replication_to = Column(String(30))
    src_splex_sid = Column(String(30))
    tgt_splex_sid = Column(String(50))
    src_schema = Column(String(22))
    tgt_schema = Column(String(30))
    created_by = Column(String(100), default="DEPOT")
    modified_by = Column(String(100), default="DEPORT")
    date_added = Column(DateTime, default=func.now())
    lastmodifieddate = Column(DateTime, default=func.now(), onupdate=func.now())


    def getSourceDBID(self):
        return "%s_%s" % (self.src_host[0:-1], self.src_db)

    def getTargetDBID(self):
        return "%s_%s" % (self.tgt_host[0:-1], self.tgt_db)

    def getMonitorTableName(self):
        if wbxutil.isNoneString(self.qname):
            table_name = "SPLEX_MONITOR_ADB"
        else:
            table_name = "SPLEX_MONITOR_ADB_%s" % self.qname.upper()
        return table_name

    def getSchemaname(self):
        return "SPLEX%s" % self.port

    def getDirection(self):
        if self.isKafkaChannel():
            repto = self.replication_to.split("_")[:2]
            direction = "%s_%s" % ("_".join(repto), self.qname) if not wbxutil.isNoneString(self.qname) else "_".join(repto)
        else:
            direction = "%s_%s" % (self.replication_to, self.qname) if not wbxutil.isNoneString(self.qname) else self.replication_to
        return direction

    def isKafkaChannel(self):
        if self.tgt_splex_sid == "KAFKA":
            return True
        else:
            return False

    def __str__(self):
        return "str: from %s.%s to %s.%s under port %s with monitor_table=%s" % (self.src_host, self.src_db,  self.tgt_host, self.tgt_db, self.port, self.getMonitorTableName())

    def __repr__(self):
        return "repr: %s.%s on port %s to %s.%s" % (self.src_host, self.src_db, self.port, self.tgt_host, self.tgt_db)

class wbxschema(Base):
    __tablename__ = "appln_pool_info"
    trim_host = Column(String(30), primary_key=True)
    db_name = Column(String(25), primary_key=True)
    appln_support_code = Column(String(25))
    schema = Column(String(35), primary_key=True)
    password = Column(String(512))
    date_added = Column(DateTime, default=func.now())
    lastmodifieddate = Column(DateTime, default=func.now(), onupdate=func.now())
    created_by = Column(String(100), default="DEPOT")
    modified_by = Column(String(100), default="DEPOT")
    km_version = Column(String(10), default=-1)
    schematype = Column(String(16))
    new_password = Column(String(512))
    track_id = Column(String(512))
    change_status = Column(Integer, default=0)

    def getSchema(self):
        return self.schema

    def getPassword(self):
        return self.password

    def getdbid(self):
        return "%s_%s" % (self.trim_host, self.db_name)

    def __str__(self):
        return "trim_host=%s, db_name=%s, schemaname=%s, password=%s, schematype=%s" % (self.trim_host, self.db_name, self.schema, self.password, self.schematype)

class wbxmappinginfo(Base):
    __tablename__ = "appln_mapping_info"
    trim_host = Column(String(30), primary_key=True)
    db_name = Column(String(25), primary_key=True)
    appln_support_code = Column(String(25))
    mapping_name = Column(String(30), primary_key=True)
    schema = Column(String(30))

    def getdbid(self):
        return "%s_%s" % (self.trim_host, self.db_name)

class ShareplexCRDeploymentVO(Base):
    __tablename__ = "wbxshareplexcrdeployment"
    trim_host = Column(String(30), primary_key=True)
    db_name = Column(String(30), primary_key=True)
    port = Column(String(22), primary_key=True)
    release_number = Column(Integer)
    major_number = Column(Integer)
    monitor_time = Column(DateTime)
    recentcrdate = Column(DateTime)

