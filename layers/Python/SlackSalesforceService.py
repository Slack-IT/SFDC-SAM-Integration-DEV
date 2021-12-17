import requests
import json
from xml.etree import ElementTree as ET
from collections import OrderedDict
import logging
import urllib.parse
logging.basicConfig(level=logging.INFO,format='%(filename)s:%(lineno)s - %(funcName)20s() | %(message)s')
logger = logging.getLogger(__name__)




class SlackSalesforceService:
  '''Needed to create a custom API Wrapper for the version 4 Bulk Import.'''
  ENVs = {
    "UAT": { "auth_url": "https://slack--uat.my.salesforce.com/services/oauth2/token", "instance_url": "https://slack--uat.my.salesforce.com"},
    "PROD": {"auth_url": "https://login.salesforce.com/services/oauth2/token", "instance_url": ""}
  }

  def __init__(self,password=None,client_id=None,client_secret=None,username=None,
               secret_token=None, access_token=None, business_unit_id=None,
               env="UAT", job_id = None):
    self.password = password
    self.client_id = client_id
    self.client_secret = client_secret
    if username.find('+') >= 0:
      self.username = urllib.parse.quote_plus(username)
    else:
      self.username = username
    self.access_token = access_token
    self.secret_token = secret_token
    self.business_unit_id=business_unit_id
    self.auth_url = self.__class__.ENVs[env]["auth_url"]
    self.instance_url = self.__class__.ENVs[env]["instance_url"]
    self.job_id = job_id


  def _api_call(self,method,endpoint,headers=None,data=None):
    logger.info(f"{method} {endpoint}")
    try:
      response = requests.request(method, endpoint, headers=headers,data=data)
    except requests.exceptions.HTTPError as err:
      logger.error(err)
      raise SystemExit(err)
    return response

  def _authenticate(self):
    payload = f'grant_type=password&client_id={self.client_id}&client_secret={self.client_secret}&username={self.username}&password={self.password}'
    headers = {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Accept': 'application/json'
    }
    response = self._api_call("POST",self.auth_url,headers=headers,data=payload)

    self.access_token = json.loads(response.text)['access_token']


  def import_init_job(self,object=None,operation=None):
    assert(object is not None)
    assert(operation is not None)

    self._authenticate()
    url = f"{self.instance_url}/services/data/v50.0/jobs/ingest"
    payload = json.dumps({
      "externalIdFieldName": "Id",
      "object": object,
      "operation": operation
    })
    headers = {
      'Authorization': f'Bearer {self.access_token}',
      'Content-Type': 'application/json'
    }

    response = self._api_call("POST",url,headers=headers,data=payload)
    self.job_id = json.loads(response.text)['id']
    logger.info(f"job_id={self.job_id}")
    return self.job_id


  def import_add_batch(self,job_id=None,file_path=None):
    assert(file_path is not None)
    self._authenticate()

    if not job_id:
      job_id = self.job_id
    url = f"{self.instance_url}/services/data/v50.0/jobs/ingest/{job_id}/batches"
    with open(file_path,'rb') as f:
      payload = f.read()
    headers = {
      'Authorization': f'Bearer {self.access_token}',
      'Content-Type': 'text/csv'
    }

    self._api_call("PUT",url,headers=headers,data=payload)

    return True

  def import_exe_job(self,job_id=None):
    self._authenticate()

    if not job_id:
      job_id = self.job_id
    url = f"{self.instance_url}/services/data/v50.0/jobs/ingest/{job_id}"
    payload = json.dumps({
      "state": "UploadComplete"
    })
    headers = {
      'Authorization': f'Bearer {self.access_token}',
      'Content-Type': 'application/json'
    }

    response = self._api_call("PATCH",url,headers=headers,data=payload)

    return True

  def import_poll_job(self,job_id):
    self._authenticate()

    if not job_id:
      job_id = self.job_id
    url = f"{self.instance_url}/services/data/v50.0/jobs/ingest/{job_id}"
    payload = {}
    headers = {
      'Authorization': f'Bearer {self.access_token}'
    }
    response = self._api_call("GET",url,headers=headers,data=payload)

    responseJson = json.loads(response.text)
    job_status = OrderedDict()
    if responseJson['state'] == 'JobComplete':
      job_status['isProcessed'] = True
      job_status["recordsUpdated"] = responseJson['numberRecordsProcessed']
      job_status["recordsFailed"] = responseJson['numberRecordsFailed']
      job_status['totalProcessingTime'] = responseJson['totalProcessingTime']
      job_status['createdDate'] = responseJson['createdDate']
    else:
      job_status['isProcessed'] = False
    return job_status