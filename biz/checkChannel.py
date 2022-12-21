import logging
from datetime import datetime
from dao.wbxdaomanager import wbxdaomanagerfactory
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


logger = logging.getLogger("DBAMONITOR")


class CallDB:
    def __init__(
        self,
        username: str,
        password: str,
        db_name: str,
        db_domain: str,
        db_port: int=5432,
        dsn: str = "postgresql://{username}:{password}@{db_domain}:{db_port}/{db_name}"
    ):
        self.username = username
        self.password = password
        self.db_name = db_name
        self.db_port = db_port
        self.db_domain = db_domain
        self.dsn = dsn.format(
            username=self.username, password=self.processPassword(self.password),
            db_domain=self.db_domain, db_port=self.db_port,
            db_name=self.db_name)
        self.engine = None

    def processPassword(self, password):
        result = ""
        char_map = {
            "@": "%40",
            "!": "%21",
            "*": "%2A",
            "'": "%27",
            "(": "%28",
            ")": "%29",
            ";": "%3B",
            ":": "%3A",
            "&": "%26",
            "=": "%3D",
            "+": "%2B",
            "$": "%24",
            ",": "%2C",
            "/": "%2F",
            "?": "%3F",
            "#": "%23",
            "[": "%5B",
            "]": "%5D",
            " ": "%20",
            "<": "%3C",
            ">": "%3E",
            "%": "%25"
        }
        for char in password:
            if char_map.get(char, None):
                result+=char_map.get(char)
            else:
                result+=char

        return result              

    def __enter__(self):
        self.engine = create_engine(self.dsn, connect_args={"connect_timeout": 5})
        self.session = sessionmaker(bind=self.engine)()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """
        also can process exception here
        """
        if exc_value is not None:
            self.session.rollback()
            raise Exception(f"{str(exc_type)}:{str(exc_value)}")
        self.session.commit()
        self.engine.dispose()


def get_wbxadbmon_single_info(*, src_host: str, src_db: str, port: str, tgt_host: str, tgt_db: str):
    sql = f"""
    select 
        src_host,
        src_db,
        port,
        tgt_host,
        tgt_db,
        to_char(lastreptime, 'yyyy-MM-dd HH24:mi:ss') lastreptime,
        to_char(montime, 'yyyy-MM-dd HH24:mi:ss') montime 
    from wbxadbmon
    where src_host='{src_host}' and src_db='{src_db}' and port='{port}' and tgt_host='{tgt_host}' and tgt_db='{tgt_db}';
    """
    daoManagerFactor = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactor.getDefaultDaoManager()
    session = daoManager.startTransaction()
    conn = session.execute(sql)
    data = conn.fetchone()
    session.close()
    return dict(
        src_host=data[0],
        src_db=data[1],
        port=data[2],
        tgt_host=data[3],
        tgt_db=data[4],
        last_replication_time=data[5],
        monitor_time=data[6]
    )


def get_channel_info(*, src_host: str, src_db: str, port: str, tgt_host: str, tgt_db: str):
    sql = f"""
    select
        si.src_host, 
        si.src_db, 
        si.src_schema,
        src_api.password as src_password,
        src_api.appln_support_code as src_appln_support_code,
        src_api.schematype as src_schematype, 
        si.port, 
        si.tgt_host, 
        si.tgt_db, 
        si.tgt_schema,
        tgt_api.password as tgt_password,
        tgt_api.appln_support_code as tgt_appln_support_code,
        tgt_api.schematype as tgt_schematype,
        si.qname 
    from shareplex_info si
    join appln_pool_info src_api
    on si.src_db=src_api.db_name and si.src_schema=src_api.schemaname
    join appln_pool_info tgt_api
    on si.tgt_db=tgt_api.db_name and si.tgt_schema=tgt_api.schemaname
    where 
        si.src_host='{src_host}' and 
        si.src_db='{src_db}' and
        si.port='{port}' and
        si.tgt_host='{tgt_host}' and
        si.tgt_db='{tgt_db}' and
        si.src_schema='wbxdba';
    """
    daoManagerFactor = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactor.getDefaultDaoManager()
    session = daoManager.startTransaction()
    conn = session.execute(sql)
    data = conn.fetchone()
    session.close()
    if data:
        return dict(
            src_host=data[0],
            src_db=data[1],
            src_schema=data[2],
            src_password=data[3],
            src_appln_support_code=data[4],
            src_schematype=data[5],
            port=data[6],
            tgt_host=data[7],
            tgt_db=data[8],
            tgt_schema=data[9],
            tgt_password=data[10],
            tgt_appln_support_code=data[11],
            tgt_schematype=data[12],
            qname=data[13],
        )

def check_single_channel(*, src_host: str, src_db: str, port: str, tgt_host: str, tgt_db: str):
    result = {"status": "SUCCESS", "errormsg": "", "data": None}
    channel_info = get_channel_info(
        src_host=src_host,
        src_db=src_db,
        port=port,
        tgt_host=tgt_host,
        tgt_db=tgt_db,
    )

    if not channel_info:
        result["status"] = "FAILED"
        result["errormsg"]= f"channel not exists: {src_host}:{port}/{src_db} - {tgt_host}:{port}/{tgt_db}"
        return result

    try:
        with CallDB(
            username=channel_info["src_schema"],
            password=channel_info["src_password"],
            db_name=channel_info["src_db"],
            db_domain=channel_info["src_host"]) as db:
            update_src_db_sql = f"""
            insert into rep_monitor_adb_{channel_info["port"]}_{channel_info["qname"]} (src_db,tgt_db,port,queuename,logtime)
            values ('{channel_info["src_db"]}','{channel_info["tgt_db"]}',{int(channel_info["port"])},'{channel_info["qname"]}',now())
            on conflict (src_db,tgt_db,port,queuename) do update 
            set logtime=now();
            """
            db.session.execute(update_src_db_sql)
    except Exception as e:
        result["status"] = "FAILED"
        result["errormsg"] = f"update source db logtime error: {str(e)}"
        logger.error(f"update source db logtime error: {str(e)}")

    query_logtime = None
    try:
        with CallDB(
            username=channel_info["tgt_schema"],
            password=channel_info["tgt_password"],
            db_name=channel_info["tgt_db"],
            db_domain=channel_info["tgt_host"],
        ) as db:
            query_tgt_db_sql = f"""
            select logtime from rep_monitor_adb_{channel_info["port"]}_{channel_info["qname"]}
            where 
                src_db='{channel_info["src_db"]}' and 
                tgt_db='{channel_info["tgt_db"]}' and 
                port={int(channel_info["port"])}; 
            """
            conn = db.session.execute(query_tgt_db_sql)
            query_logtime = conn.fetchone()
    except Exception as e:
        result["status"] = "FAILED"
        result["errormsg"] += f"update source db logtime error: {str(e)}"
        logger.error(f"query target db logtime error: {str(e)}")

    query_logtime = query_logtime[0] if query_logtime else datetime.strptime("1970-1-1", "%Y-%m-%d")

    update_depot_db_sql = f"""
    insert into wbxadbmon (src_host,src_db,port,tgt_host,tgt_db,lastreptime,montime)
    values
    ('{channel_info["src_host"]}','{channel_info["src_db"]}','{channel_info["port"]}','{channel_info["tgt_host"]}','{channel_info["tgt_db"]}','{query_logtime}','{datetime.now()}')
    on conflict(src_host,src_db,port,tgt_host,tgt_db) do update
    set 
    lastreptime=excluded.lastreptime, 
    montime=excluded.montime;
    """
    daoManagerFactor = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactor.getDefaultDaoManager()
    session = daoManager.startTransaction()
    try:
        session.execute(update_depot_db_sql)
    except Exception as e:
        result["status"] = "FAILED"
        result["errormsg"] = f"update depot wbxadbmon error: {str(e)}"
        logger.error(f"update depot wbxadbmon error: {str(e)}")
        session.rollback()
    else:
        session.commit()
    
    result['data'] = get_wbxadbmon_single_info(
        src_host=channel_info["src_host"],
        src_db=channel_info["src_db"],
        port=str(channel_info["port"]),
        tgt_host=channel_info["tgt_host"],
        tgt_db=channel_info["tgt_db"]
    )
    return result
    