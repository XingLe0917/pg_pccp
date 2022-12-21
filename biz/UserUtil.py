import base64
import logging
from base64 import b64encode

import requests

from common.Config import Config
# from common.envConfig import mapping
from dao.wbxdaomanager import wbxdaomanagerfactory, DaoKeys


logger = logging.getLogger("DBAMONITOR")

def getTokenByCode(code):
    logger.info("getTokenByCode, code=%s"%(code))
    cfg = Config()
    authorization = 'Basic ' + b64encode((cfg.getClentID() + ':' + cfg.getSecret()).encode('utf-8')).decode('utf-8')
    deploy_ip = cfg.getDeployIP()
    deploy_port = cfg.getDeployPort()
    # redirect_uri = "http%3A%2F%2F10.252.52.189%3A9000%2FloginRedirection"
    redirect_uri = "http%3A%2F%2F" + deploy_ip + "%3A" + deploy_port + "%2FloginRedirection"
    headers = {'content-type': 'application/x-www-form-urlencoded',
               "Authorization": authorization}
    url = "https://idbroker.webex.com/idb/oauth2/v1/access_token?grant_type=authorization_code&redirect_uri="+redirect_uri+"&code=" + code
    logger.info("authorization=%s" % (authorization))
    logger.info("url=%s" % (url))
    response = requests.post(url=url,headers=headers)
    return response.json()

def getUserByToken(token):
    url = "https://identity.webex.com/identity/scim/v1/Users/me"
    headers = {'content-type': 'application/json; charset=UTF-8','Authorization':"Bearer "+token}
    r = requests.get(url=url, headers=headers)
    return r

def checkUserInPCCP(cec):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        daoManager.startTransaction()
        ls = depotdbDao.get_ccp_user_role_info_count()
        if len(ls) == 0:
            depotdbDao.insert_admin(cec)
            daoManager.commit()
        rows = depotdbDao.getCCPUserRole(cec)
        daoManager.commit()
        if len(rows)>0:
            return True
        else:
            return False
    except Exception as e:
        daoManager.rollback()
        raise e
    finally:
        daoManager.close()

def getTokeninfo(access_token):
    logger.info("getTokeninfo")
    cfg = Config()
    token_info_authorization = cfg.getPCCPServiceAuth()
    headers = {'content-type': 'application/x-www-form-urlencoded',
               "Authorization": token_info_authorization}
    url = "https://idbroker.webex.com/idb/oauth2/v1/tokeninfo"
    body = "access_token=" + str(access_token)
    response = requests.post(url=url, headers=headers,data=body)
    return response.json()

def getOtherUser(username):
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        daoManager.startTransaction()
        user = depotdbDao.get_OtherUser(username)
        daoManager.commit()
        if len(user)>0:
            return dict(user[0])
        else:
            return None
    except Exception as e:
        daoManager.rollback()
        raise e
    finally:
        daoManager.close()

def getCCPUser():
    daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
    daoManager = daoManagerFactory.getDefaultDaoManager()
    try:
        depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
        daoManager.startTransaction()
        users = depotdbDao.getCCPUserList()
        daoManager.commit()
        if len(users) > 0:
            return [dict(vo) for vo in users]
        else:
            return None
    except Exception as e:
        daoManager.rollback()
        raise e
    finally:
        daoManager.close()

def getOAuthTokenForMA():
    logger.info("getOAuthTokenForMA")
    orgid = "6078fba4-49d9-4291-9f7b-80116aab6974"
    url = "https://idbroker.webex.com/idb/token/%s/v2/actions/GetBearerToken/invoke" %(orgid)
    headers = {'content-type': 'application/json; charset=UTF-8'}
    data = {
        "name":"pg_pccp_ua",
        "password":"EVKO.bfik.49.SJZL.fykp.13.BUYO.chmn.1267"
    }

    response = requests.post(url=url,headers=headers,json=data)
    return response.json()

def getAccessTokenForMA():
    logger.info("getAccessTokenForMA")
    # config = mapping["prod"]()
    cfg = Config()
    authorization = 'Basic ' + b64encode((cfg.getClentID() + ':' + cfg.getSecret()).encode('utf-8')).decode('utf-8')
    res = getOAuthTokenForMA()
    print(res)
    if "BearerToken" in res:
        bearerToken = res['BearerToken']
        headers = {'content-type': 'application/x-www-form-urlencoded',
                   "Authorization": authorization}
        url = "https://idbroker.webex.com/idb/oauth2/v1/access_token?grant_type=urn:ietf:params:oauth:grant-type:saml2-bearer&assertion=" + bearerToken + "&scope=identity:myprofile_read"
        response = requests.post(url=url,headers=headers)
        return response.json()['access_token']
    else:
        logger.error("Error get BearerToken")

def getAuthorizationForMA():
    res = {"status": "SUCCESS", "errormsg": "", "authorization": None}
    logger.info("getAuthorizationForMA")
    username = "pg_pccp_ua"
    access_token = getAccessTokenForMA()
    authorization = "Basic " + b64encode((username + ':' + access_token).encode('utf-8')).decode('utf-8')
    res['authorization'] = authorization
    logger.info("authorization")
    logger.info(authorization)
    return res

if __name__ == "__main__":
    access_token = ""
    env="prod"
    # a = getTokeninfo(access_token)
    aa = getAuthorizationForMA()
    print(aa)
