import logging
from datetime import datetime
from schemas.wbxpartitionjobstatus import PartitionJobSchema
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys


logger = logging.getLogger("DBAMONITOR")


def get_daoManager():
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    return daoManager


def list_partition_job_status(*, params):
    daomanager = get_daoManager()
    daomanager.startTransaction()
    dao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
    result = dao.list_partition_job_status(params=params)
    daomanager.close()
    return result

def persist_partition_job_status(*, data: PartitionJobSchema):
    """
        save partition job log data
        data:
            - host_name
            - db_name
            - status
            - optype
            - jobtype
            - log
    """
    operation_params_map = {
        "ADD": {
            "field":"add_status",
            "operation_time_map": {
                "BEGIN": "add_begin_time",
                "END": "add_end_time",
            },
            "log_field": "add_log",
        },
        "DROP": {
            "field":"drop_status",
            "operation_time_map": {
                "BEGIN": "drop_begin_time",
                "END": "drop_end_time"
            },
            "log_field": "drop_log",
        }
    }
    operation_type = operation_params_map.get(data.optype)
    op_status_field = operation_type.get("field")
    op_time_field = operation_type.get("operation_time_map").get(data.jobtype)
    log_field = operation_type.get("log_field")
    try:
        daomanager = get_daoManager()
        daomanager.startTransaction()
        # dao -> DepotDBDao
        dao = daomanager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        # if no such hostname, db_name record
        odata = dao.get_partition_job_host(host_name=data.host_name, db_name=data.db_name)
        params = dict(
            status_field=op_status_field,
            time_field=op_time_field,
            host_name=data.host_name,
            db_name=data.db_name,
            op_status=data.status,
            op_time=datetime.now(),
            log_field=log_field,
            log_content=data.log,
            jobtype = data.jobtype,
        )
        if odata is not None:
            dao.update_partition_job_status(params=params)
        else:
            dao.insert_partition_job_status(params=params)
        daomanager.commit()
    except Exception as e:
        daomanager.rollback()
        logging.error(str(e))
        raise e
    finally:
        if daomanager:
            daomanager.close()