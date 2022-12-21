import logging

from dao.wbxdaomanager import wbxdao
from datetime import datetime, timedelta
from common.wbxutil import wbxutil
from sqlalchemy import literal_column

from sqlalchemy import func, and_, text
logger = logging.getLogger("DBAMONITOR")

class DBAuditDao(wbxdao):
    pass