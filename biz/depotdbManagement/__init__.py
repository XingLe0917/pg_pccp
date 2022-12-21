import logging
from dao.wbxdaomanager import wbxdaomanagerfactory

logger = logging.getLogger("DBAMONITOR")


def get_session():
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    daoManager.setLocalSession()
    return daoManager.getLocalSession()


def process_filter_integer_sql(int_list, xor="and", **filter_fields):
    sql = ""
    for filter_filed, value in filter_fields.items():
        if value:
            if filter_filed in int_list:
                sql += f" {xor} {filter_filed}={value}"
                continue
            sql += f" {xor} {filter_filed}='{value}'"
    
    return sql