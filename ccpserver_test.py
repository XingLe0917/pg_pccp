import sys

import logging.config
from common.Config import Config
from dao.depotdbdao import DepotDBDao
from dao.dbauditdao import DBAuditDao

from view2 import loadFlask

logger = None

def init():
    global logger
    config = Config()
    logconfigfile = config.getLoggerConfigFile()
    logging.config.fileConfig(logconfigfile)
    logger = logging.getLogger("DBAMONITOR")

def testDao():
    from biz.cronjobmonitor import getDBCount,getCheckCronStatus
    getCheckCronStatus()

if __name__=='__main__':
    init()
    testDao()

    # loadFlask()