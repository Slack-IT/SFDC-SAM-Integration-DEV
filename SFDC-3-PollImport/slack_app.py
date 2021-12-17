import requests
import json
from collections import OrderedDict
from datetime import datetime
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PARDOT_SLACK_URL = "https://hooks.slack.com/services/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"
SLACK_TEST_URL = "https://hooks.slack.com/services/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"

def _compute_batch_processing_time(job_updated_at,job_created_at):
  createdAt_Datetime = datetime.strptime(job_created_at, '%Y-%m-%d %H:%M:%S')
  updatedAt_Datetime = datetime.strptime(job_updated_at, '%Y-%m-%d %H:%M:%S')
  result = ((updatedAt_Datetime - createdAt_Datetime).seconds / 60 / 60)
  hours = int(result)
  minutes = int(((result * 60) % 60))
  seconds = int(((result * 3600) % 60))
  #06 hours 53 min 09 sec
  return  f"{hours} hours {minutes} minutes {seconds} seconds"


def send_alert_message(job_details):

  job_id = job_details['Job ID']
  updatedCount = job_details['updatedCount']
  errorCount = job_details['errorCount']
  createdCount = job_details['createdCount']
  job_created_at = job_details['createdAt']
  job_updated_at = job_details['updatedAt']
  job_state = job_details["state"]
  job_total_records = str(int(updatedCount) + int(createdCount) + int(errorCount))
  total_time = _compute_batch_processing_time(job_updated_at,job_created_at)

  #job_total_time_process =
  #:stopwatch: 04 hours 01 min 03 sec

  payload = json.dumps({
    "blocks": [
      {
        "type": "header",
        "text": {
          "type": "plain_text",
          "text": "Pardot - Consent MGMT"
        }
      },
      {
        "type": "divider"
      },
      {
        "type": "section",
        "text": {
          "type": "mrkdwn",
          "text": f"The Pardot job id *{job_id}* completed with status: {job_state} :white-check-mark:"
        }
      },
      {
        "type": "section",
        "text": {
          "type": "mrkdwn",
          "text": "*Batch Processing Time*:"
        }
      },
      {
        "type": "section",
        "text": {
          "type": "mrkdwn",
          "text": f":stopwatch: {total_time} \n Created at: {job_created_at} \n Updated at: {job_updated_at}"
        }
      },
      {
        "type": "section",
        "text": {
          "type": "mrkdwn",
          "text": "*Job details(Number of records)*:"
        }
      },
      {
        "type": "section",
        "fields": [
          {
            "type": "mrkdwn",
            "text": f"*Total:*\n {job_total_records}"
          },
          {
            "type": "mrkdwn",
            "text": f"*Created:*\n {createdCount}"
          },
          {
            "type": "mrkdwn",
            "text": f"*Updated:*\n {updatedCount}"
          },
          {
            "type": "mrkdwn",
            "text": f"*Errors:*\n {errorCount} "
          }
        ]
      },
      {
        "type": "section",
        "text": {
          "type": "mrkdwn",
          "text": f"For more error details see:\n*<https://pi.pardot.com/import/read/id/{job_id}>*"
        }
      }
    ]
  })
  headers = {
    'Content-Type': 'application/json'
  }
  try:
    response = requests.request("POST", PARDOT_SLACK_URL, headers=headers, data=payload)
  except requests.exceptions.RequestException as e:
    logger.error(e)
    return False
  return response.ok