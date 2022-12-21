import base64
import os
import json
import sys

from common.singleton import Singleton
from common.wbxexception import wbxexception
# from influxdb import InfluxDBClient


@Singleton
class Config:

    def __init__(self):
        self.CONFIGFILE_DIR = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), "conf")
        self.loadDBAMonitorConfigFile()
        self.DEPOT_IP = "depotdb_ip"
        self.DEPOTDB_PORT = "depotdb_port"
        self.DEPOTDB_DBNAME = "depotdb_dbname"
        self.DEPOT_USERNAME = "depotdb_username"
        self.DEPORT_PASSWORD = "depotdb_password"
        self.SSLROOTCERT = "sslrootcert_file"
        self.SSLCERT = "sslcert_file"
        self.SSLKEY = "sslkey_file"
        self.DEPLOY_IP = "deploy_ip"
        self.DEPLOY_PORT = "deploy_port"
        self.CLIENT_ID = "client_id"
        self.SECRET = "secret"
        self.PCCP_SERVICE_AUTH = "pccp_service_authorization"
        self.InfluxDB_ip = "influxDB_ip",
        self.InfluxDB_port = "influxDB_port",
        self.InfluxDB_user = "influxDB_user",
        self.InfluxDB_pwd = "influxDB_pwd"

    def getDBAMonitorConfigFile(self):
        dbamonitor_config_file = os.path.join(self.CONFIGFILE_DIR, "pccp.json")
        # dbamonitor_config_file = os.path.join(self.CONFIGFILE_DIR, "bjdbamonitortool_config.json")
        if not os.path.isfile(dbamonitor_config_file):
            raise wbxexception("%s does not exist" % dbamonitor_config_file)
        return dbamonitor_config_file

    def getLoggerConfigFile(self):
        logger_config_file = os.path.join(self.CONFIGFILE_DIR, "logger.conf")
        if not os.path.isfile(logger_config_file):
            raise wbxexception("%s does not exist" % logger_config_file)
        return logger_config_file

    def loadDBAMonitorConfigFile(self):
        dbamonitor_config_file = self.getDBAMonitorConfigFile()
        f = open(dbamonitor_config_file, "r")
        self.configDict = json.load(f)
        f.close()

    def getDBConnectionurl(self, db_name):
        if db_name == "DEFAULT":
            return "%s:%s@%s:%s/auditdb" % (self.configDict[self.DEPOT_USERNAME], self.configDict[self.DEPORT_PASSWORD],
                                            self.configDict[self.DEPOT_IP], self.configDict[self.DEPOTDB_PORT])
        else:
            return None

    def getSSLCert(self):
        return self.configDict[self.SSLCERT]

    def getSSLKey(self):
        return self.configDict[self.SSLKEY]

    def getSSLRootCert(self):
        return self.configDict[self.SSLROOTCERT]

    def getClentID(self):
        return self.configDict[self.CLIENT_ID]

    def getDeployIP(self):
        return self.configDict[self.DEPLOY_IP]

    def getDeployPort(self):
        return self.configDict[self.DEPLOY_PORT]

    def getSecret(self):
        return self.configDict[self.SECRET]

    def getPCCPServiceAuth(self):
        return self.configDict[self.PCCP_SERVICE_AUTH]

    # def getInfluxDBclient(self):
    #     database = "auditdb"
    #     if self.configDict['influxDB_test']:
    #         return InfluxDBClient(self.configDict['influxDB_test_ip'], int(self.configDict['influxDB_port']),
    #                               self.configDict['influxDB_user'],
    #                               base64.b64decode(self.configDict['influxDB_test_pwd']).decode("utf-8"), database)
    #     else:
    #         return InfluxDBClient(self.configDict['influxDB_ip'], int(self.configDict['influxDB_port']),
    #                               self.configDict['influxDB_user'],
    #                               base64.b64decode(self.configDict['influxDB_pwd']).decode("utf-8"), database)
