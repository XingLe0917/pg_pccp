import logging
from copy import deepcopy
from dao.wbxdaomanager import wbxdaomanagerfactory

logger = logging.getLogger("DBAMONITOR")


def get_session():
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    daoManager.setLocalSession()
    return daoManager.getLocalSession()


def get_appln_mapping_info(**filter_fields):
    sess = get_session()
    sql = f"""
    select 
        db_name,
        mapping_name,
        appln_support_code,
        schema_name,
        service_name,
        to_char(createtime, 'yyyy-MM-dd HH24:mi:ss') createtime,
        to_char(lastmodifiedtime, 'yyyy-MM-dd HH24:mi:ss') lastmodifiedtime
    from depot.appln_mapping_info
    where 1=1
    """
    for filter_filed, value in filter_fields.items():
        if value:
            sql += f" and {filter_filed}='{value}'"

    try:
        data = sess.execute(sql).fetchone()
    except Exception as e:
        logger.error(f"fetch {filter_fields} data error: {str(e)}")
    else:
        sess.close()
        if data:
            return dict(
                db_name=data[0],
                mapping_name=data[1],
                appln_support_code=data[2],
                schema_name=data[3],
                service_name=data[4],
                createtime=data[5],
                lastmodifiedtime=data[6]
            )


def create_appln_mapping_info(data):
    update_data=deepcopy(data)
    update_data.pop("db_name", None)
    update_data.pop("mapping_name", None)
    key_val_string = [f"{key}=excluded.{key}" for key,_ in update_data.items()]
    key_val_string = ",".join(key_val_string)
    res = {
        "status": "SUCCESS",
        "msg": "",
        "data": None,
    }
    sess = get_session()
    fields_string = ",".join(list(data.keys()))
    values_temp = "','".join([str(i) for i in list(data.values())])
    values_string = f"'{values_temp}'"
    sql = f"""
    insert into depot.appln_mapping_info({fields_string})
    values
    ({values_string})
    on conflict(db_name, mapping_name) do update
    set
    {key_val_string},
    lastmodifiedtime=now();
    """
    try:
        sess.execute(sql)
    except Exception as e:
        res["status"] = "FAILED"
        res["msg"] = str(e)
        logger.error(f"create appln_mapping_info error: {str(e)}")
        sess.rollback()
    else:
        sess.commit()
        sess.close()
        res["data"] = get_appln_mapping_info(
            db_name=data["db_name"],
            mapping_name=data["mapping_name"]
        )

    return res