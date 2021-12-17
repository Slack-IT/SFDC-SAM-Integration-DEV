import json
import boto3
import common
import logging
import slack_app
from boto3.dynamodb.conditions import Key
from SlackSalesforceService import SlackSalesforceService

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger()


def query_db_jobs():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Consent-JobStatus')
    scan_kwargs = {
        'FilterExpression': Key('isProcessed').eq(False)
    }
    response = table.scan(**scan_kwargs)
    return response.get('Items', [])


def check_status(batch_jobs, api_auth_details):
    # for each batch id, run a pardot api state check
    pc = SlackPardotService(password=api_auth_details['password'],
                            client_id=api_auth_details['client_id'],
                            client_secret=api_auth_details['client_secret'],
                            username=api_auth_details['username'],
                            secret_token=api_auth_details['secret_token'],
                            business_unit_id=api_auth_details['business_unit_id'],
                            env="Prod")
    responses = []
    for job in batch_jobs:
        # poll job["Job ID"]
        logger.info(f'Job ID to be polled: {job["Job ID"]}')
        response = pc.batch_queue_poll_status(batch_id=job["Job ID"])
        responses.append(response)
    return responses


def _send_slack_alert(response):
    return slack_app.send_alert_message(response)



def _update_db_job(response):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Consent-JobStatus')
    response = table.update_item(
    Key={
        'Job ID': response["Job ID"]
    },
    UpdateExpression="set isProcessed=:r",
    ExpressionAttributeValues={
        ':r': True
    },
    ReturnValues="UPDATED_NEW"
    )
    return response



def evaluate_polling_results(responses):
    for response in responses:
        if response['state'] == 'Complete':
            if (_send_slack_alert(response)):
                _update_db_job(response)
    return


def main():
    batch_jobs = query_db_jobs()
    if not batch_jobs:
        logger.info("No Jobs to process. Stopping")
        return
    api_auth_details = json.loads(common.get_secret())
    responses = check_status(batch_jobs, api_auth_details)
    evaluate_polling_results(responses)


def lambda_handler(event, context):
    # TODO implement
    print("lambda handler called")
    main()
    '''
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    '''
