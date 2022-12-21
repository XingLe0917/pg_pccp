from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool
from common.wbxexception import wbxexception
import threading
import json
import sys
import os
import logging
import traceback
from common.Config import Config

threadlocal = threading.local()
threadlocal.current_session = {}

logger = logging.getLogger("DBAMONITOR")
# logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
# logging.getLogger("sqlalchemy.pool").setLevel(logging.DEBUG)
# logger.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

class wbxdaomanagerfactory(object):
    daomanagerfactory = None
    daoManagerCache = {}
    dbcache = {}
    DEFAULT_DBID="DEFAULT"
    DEFAULT_LOGINSCHEMA="depot"


    _lock = threading.Lock()

    def __init__(self):
        raise wbxexception("Can not instance a wbxdaomanagerfactory, please call getDaoManagerFactory()")

    @staticmethod
    def getDaoManagerFactory():
        if wbxdaomanagerfactory.daomanagerfactory is None:
            wbxdaomanagerfactory.daomanagerfactory = object.__new__(wbxdaomanagerfactory)
        return wbxdaomanagerfactory.daomanagerfactory

    def getDefaultDaoManager(self):
        daoManager = self.getDaoManager(self.DEFAULT_DBID)
        return daoManager

    # There is potential risk at here, because there is dbmanagercache which initialize enginer at first time, so the poolengine parameter is fixed at first initialization;
    def getDaoManager(self, db_name, expire_on_commit= False):
        # Each daomanager is shared, but the connection is not shared, each Daomanager has a separate connection
        if db_name in self.daoManagerCache:
            daomanager = self.daoManagerCache[db_name]
            daomanager.setLocalSession()
            return daomanager
        with self._lock:
            cfg = Config()
            connectionurl = cfg.getDBConnectionurl(db_name)
            if connectionurl is None:
                raise wbxexception("Not find the database with db_name=%s" % db_name)
            sslcert_file = cfg.getSSLCert()
            sslkey_file = cfg.getSSLKey()
            sslrootcert_file = cfg.getSSLRootCert()
            engine = create_engine('postgresql+psycopg2://%s' % connectionurl,
                                   connect_args={"application_name": "pg_pccp",
                                                 "connect_timeout": 10,
                                                 "sslmode": "disable",
                                                 "sslkey": sslkey_file,
                                                 "sslrootcert": sslrootcert_file,
                                                 "sslcert": sslcert_file},
                                   pool_recycle=600, pool_size=10, max_overflow=10, pool_timeout=10,
                                   echo_pool=True,
                                   echo=True)
            daomanager = wbxdaomanager(engine, expire_on_commit)
            self.daoManagerCache[db_name] = daomanager
            return daomanager

'''
each session is a connection, and the engine have a pool attributes. so wbxdaomanager have a engine property, and each time it will return a new session for each DaoManager
'''
class wbxdaomanager():
    def __init__(self, engine, expire_on_commit=False):
        self.daoMap={}
        self.loadDaoMap()
        self.engine = engine
        self.expire_on_commit = expire_on_commit
        self.setLocalSession()

    def loadDaoMap(self):
        currentfile = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
        daoconfigfile = os.path.join(os.path.join(currentfile, "conf"),"dao_mapper.json")

        # daoconfigfile = "conf/dao_mapper.json"
        f = open(daoconfigfile)
        jsonstr = f.read()
        f.close()
        daoDict = json.loads(jsonstr)
        for daoName, daoClz in daoDict.items():
            idxsize = daoClz.rfind(".")
            modulepath=daoClz[0:idxsize]
            clzname=daoClz[idxsize + 1:]
            amodule = sys.modules[modulepath]
            daocls = getattr(amodule, clzname)
            wbxdao = daocls()
            self.daoMap[daoName] = wbxdao

        # logger.info(self.daoMap.keys())

    def getLocalSession(self):
        session = threadlocal.current_session[self.engine]
        return session

    # Session is not thread-safe. so each thread we create a new Session
    # http://docs.sqlalchemy.org/en/latest/orm/session_basics.html
    # Session is not appropriate to cache object; All cached data in Session should be removed after commit;
    # But because we do not introduce Cache Module in this project, so Session is also used as a cache, it is workaround method
    def setLocalSession(self):
        if not hasattr(threadlocal, "current_session"):
            threadlocal.current_session={}
        if self.engine not in threadlocal.current_session:
            sessionclz = sessionmaker(bind=self.engine, expire_on_commit=self.expire_on_commit)
            session = sessionclz()
            threadlocal.current_session[self.engine] = session

    def startTransaction(self):
        session = self.getLocalSession()
        return session

    def commit(self):
        try:
            session = self.getLocalSession()
            if session is not None:
                session.commit()
        except Exception as e:
            logger.error("Error occurred at session.commit with %s" % e)
            logger.error(traceback.format_exc())


    def rollback(self):
        try:
            session = self.getLocalSession()
            if session is not None:
                session.rollback()
        except Exception as e:
            pass

    def getDao(self, daoKey):
        wbxdao = self.daoMap[daoKey]
        wbxdao.setDaoManager(self)
        return wbxdao

    def flush(self):
        session = self.getLocalSession()
        session.dirty()

    def close(self):
        session = self.getLocalSession()
        session.close()

class wbxdao(object):
    wbxdaomanager = None

    def setDaoManager(self, daoManager):
        self.wbxdaomanager = daoManager

    def getLocalSession(self):
        return self.wbxdaomanager.getLocalSession()

class DaoKeys():
    DAO_DEPOTDBDAO = "DAO_DEPOTDBDAO"
    DAO_DBAUDITDAO = "DAO_DBAUDITDAO"







    


