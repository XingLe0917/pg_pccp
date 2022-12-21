import sys

import logging.config
from common.Config import Config
from dao.depotdbdao import DepotDBDao
from dao.dbauditdao import DBAuditDao

from view import loadFlask

logger = None

def init():
    global logger
    config = Config()
    logconfigfile = config.getLoggerConfigFile()
    logging.config.fileConfig(logconfigfile)
    logger = logging.getLogger("DBAMONITOR")

if __name__=='__main__':
    init()
    loadFlask()