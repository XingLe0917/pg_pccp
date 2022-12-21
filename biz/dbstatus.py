import logging
import re
from typing import Tuple
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from schemas.dbstatus import DBStatusQueryParams
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys


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
        self.engine = create_engine(self.dsn)
        self.session = sessionmaker(bind=self.engine)()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """
        also can process exception here
        """
        self.engine.dispose()



def get_daoManager():
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    return daoManager


def get_db_info_list():
    result = {"status": "", "errormsg": "", "data": None}
    dao_manager = get_daoManager()
    dao = dao_manager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    try:
        dao_manager.startTransaction()
        db_info_data_raw = dao.get_depot_db_info_list()
        db_info_data = []
        for row in db_info_data_raw:
            db_info_data.append(dict(
                db_name=row[0],
                host_name=row[1],
                cname=row[2],
                db_type=row[3],
                application_type=row[4],
                appln_support_code=row[5],
                wbx_cluster=row[6],
                web_domain=row[7],
            ))
        dao_manager.close()
        result["status"] = "SUCCESS"
        result["data"] = db_info_data
    except Exception as e:
        result["status"] = "FAILED"
        result["errormsg"] = str(e)
    return result


def get_db_status(params: DBStatusQueryParams):
    result = {"status": "", "errormsg": ""}
    if params.DB_NAME is None and params.DB_TYPE is None:
        result["status"] = "FAILED"
        result["errormsg"] = "please enter at least one parameter: db_type or db_name"
        return result
    result["status"] = None
    result["errormsg"] = ""
    dao_manager = get_daoManager()
    dao = dao_manager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    dao_manager.startTransaction()
    # result = dao.getDatabaseInfo()
    #1. get db info from depotdb
    db_obj = dao.get_depot_db(
        db_name=params.DB_NAME, db_type=params.DB_TYPE
    )
    if db_obj is None:
        result["status"] = "FAILED"
        result["errormsg"] = "db not exist!"
        return result
    
    (db_name, host_name, listener_port) = db_obj

    #2. request db login credential, TODO: need to be complete
    username, password = get_db_credentials(db_name=db_name)
    #3. exec sql to check db status
    with CallDB(
            username=username, password=password, db_name=db_name,
            db_domain=host_name, db_port=listener_port) as db_obj:
        try:
            conn = db_obj.session.execute("select now();")
            res = conn.fetchone()
            conn.close()
            result["status"] = "SUCCESS"
            result["errormsg"] = ""
        except Exception as e:
            result["status"] = "FAILED"
            if "authentication failed" in str(e):
                result["errormsg"] = "password incorrect"
            elif "Connection refused" in str(e):
                result["errormsg"] = "can not connect db"
            elif re.findall(r"database \".*?\" does not exist", str(e)) != []:
                result["errormsg"] = f"{db_name} does not exist"
            elif "address: Unknown host" in str(e):
                result["errormsg"] = "Unknown host"
            else:
                result["errormsg"] = str(e)
    
    dao_manager.close()
    #4. return result
    return result


def get_db_credentials(db_name, schemaname="wbxdba") -> Tuple[str, str]:
    dao_manager = get_daoManager()
    sess = dao_manager.startTransaction()
    sql = f"""
    select password from appln_pool_info
    where db_name='{db_name}' and schemaname='{schemaname}';
    """
    password = sess.execute(sql).fetchone()
    if password:
        password = password[0]
    else:
        password = "wbxdba"
    sess.close()
    return schemaname, password


