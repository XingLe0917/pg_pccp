
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common.Config import Config


class wbxdbconnection:
    def __init__(self,username,password,db_name,db_ip,db_port):
        self._expire_on_commit = True
        self._username=username
        self._password = password
        self._db_name = db_name
        self._db_ip = db_ip
        self._db_port = db_port
        self._db_conn_str = "%s:%s@%s:%s/%s" %(self._username,self._password,self._db_ip,self._db_port,self._db_name)

    def connect(self):
        url = "postgresql+psycopg2://%s" %(self._db_conn_str)
        cfg = Config()
        sslcert_file = cfg.getSSLCert()
        sslkey_file = cfg.getSSLKey()
        sslrootcert_file = cfg.getSSLRootCert()

        self._engine = create_engine(url,
            connect_args={
                "application_name": "pg_pccp",
                "connect_timeout": 10,
                "sslmode": "verify-ca",
                "sslkey": sslcert_file,
                "sslrootcert": sslkey_file,
                "sslcert": sslrootcert_file,
            },
            echo=True,
        ).connect()

        sessionclz = sessionmaker(bind=self._engine, expire_on_commit=self._expire_on_commit)
        self._session = sessionclz()

    def startTransaction(self):
        pass;

    def commit(self):
        if self._session is not None:
            try:
                self._session.commit()
            except Exception as e:
                pass

    def rollback(self):
        if self._session is not None:
            try:
                self._session.rollback()
            except Exception as e:
                pass

    def close(self):
        if self._session is not None:
            try:
                self._session.close()
            except Exception as e:
                pass

    def execute(self,SQL):
        rows = self._session.execute(SQL).fetchall()
        return rows

if __name__ == "__main__":
    username = "depot"
    password = "depot"
    db_name = "auditdb"
    db_ip = "192.168.190.37"
    db_port = "5432"
    dbconnect = wbxdbconnection(username, password, db_name, db_ip, db_port)
    dbconnect.connect()
    dbconnect.startTransaction()
    rows = dbconnect.execute("select now()")
    print(rows)
    dbconnect.commit()
    dbconnect.close()