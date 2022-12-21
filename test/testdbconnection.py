from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, TIMESTAMP, func, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import psycopg2
from sqlalchemy.types import Text, SmallInteger, Integer
import ssl


Base = declarative_base()

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
    cname = Column(String)
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
    lastmodifieddate = Column(TIMESTAMP, default=func.now())


class wbxdbconnection:
    def __init__(self):
        self._expire_on_commit = True

    def connect(self):
        # self._engine = create_engine('postgresql+psycopg2://depot:depot@10.244.12.77:5432/auditdb',
        #                    connect_args={"application_name":"pg_pccp",
        #                                  "connect_timeout":10,
        #                                  "sslmode":"verify-ca",
        #                                  "sslkey":"/home/ocp/project/ssh2/client.key",
        #                                  "sslrootcert":"/home/ocp/project/ssh2/root.crt",
        #                                  "sslcert":"/home/ocp/project/ssh2/client.crt"},
        #                    echo=True).connect()
        # self._engine = create_engine('postgresql+psycopg2://depot:depot@10.244.12.77:5432/auditdb',
        #                              connect_args={"application_name": "pg_pccp",
        #                                            "connect_timeout": 10,
        #                                            "sslmode": "verify-ca",
        #                                            "sslkey": "/Users/lexing/Documents/ciscoGithub/pg_pccp/conf/ssl/client.key",
        #                                            "sslrootcert": "/Users/lexing/Documents/ciscoGithub/pg_pccp/conf/ssl/root.crt",
        #                                            "sslcert": "/Users/lexing/Documents/ciscoGithub/pg_pccp/conf/ssl/client.crt"},
        #
        #                              echo=True).connect()



        # self._engine = create_engine('postgresql+psycopg2://wbxdba:wbxdba@10.244.12.123:5432/pgbweb',
        #                              echo=True).connect()


        # self._engine = create_engine('postgresql+psycopg2://wbxdba:wbxdba@10.240.212.156:5432/pghtweb',
        #                              echo=True).connect()

        self._engine = create_engine(
            "postgresql+psycopg2://depot:depot@192.168.190.37:5432/auditdb",
            connect_args={
                "application_name": "pg_pccp",
                "connect_timeout": 10,
                "sslmode": "disable",
                "sslkey": "/Users/lexing/Documents/ciscoGithub/pg_pccp/conf/ssl/client.key",
                "sslrootcert": "/Users/lexing/Documents/ciscoGithub/pg_pccp/conf/ssl/root.crt",
                "sslcert": "/Users/lexing/Documents/ciscoGithub/pg_pccp/conf/ssl/client.crt",
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
    dbconnect = wbxdbconnection()
    dbconnect.connect()
    dbconnect.startTransaction()
    rows = dbconnect.execute("select now()")
    print(rows)
    dbconnect.commit()
    dbconnect.close()