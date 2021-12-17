import json
import boto3
import urllib.parse
import base64
import os
import logging
import common
from collections import OrderedDict
from botocore.exceptions import ClientError
import botocore
from csv import reader
from SlackSalesforceService import SlackSalesforceService

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger()


def _list_tmp_files():
    disk = os.listdir('/tmp')
    for file_name in disk:
        print(file_name)


def fetch_S3_file(event=None):
    logger.info('S3Client Start Processing')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    s3 = boto3.resource('s3')
    destination_file = key.split('/')[-1]
    try:
        s3.Bucket(bucket).download_file(key, f"/tmp/{destination_file}")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
    _list_tmp_files()
    return f'/tmp/{destination_file}'


def _debug_dynamoDB():
    dynamodb_client = boto3.client('dynamodb')
    existing_tables = dynamodb_client.list_tables()['TableNames']
    print(existing_tables)


def put_DynamoDB_batch_id(table=None, batch_id=None):
    client = boto3.client('dynamodb')
    client.put_item(
        TableName=table,
        Item={
            'Job ID': {
                'S': str(batch_id),
            },
            'isProcessed': {
                'BOOL': False
            }
        })
    logger.info(f'Added batch_id:{str(batch_id)} to DynamoDB table')
    # return response


def pardot_load(api_auth_details=None, file_name=None):
    # loading first then at the end place job id into dynamodb
    pc = SlackPardotService(password=api_auth_details['password'],
                            client_id=api_auth_details['client_id'],
                            client_secret=api_auth_details['client_secret'],
                            username=api_auth_details['username'],
                            secret_token=api_auth_details['secret_token'],
                            business_unit_id=api_auth_details['business_unit_id'],
                            env="Prod")
    batch_id = pc.batch_queue_import_init()
    pc.batch_queue_import_add_job(batch_id=batch_id,file_path=file_name)
    pc.batch_queue_import_start()
    logger.info(f'Added Job and kicked off Batch: {batch_id}')
    return batch_id


def main(event):
    logger.info('Main Method')
    # Fetch API Auth details for Pardot
    api_auth_details = json.loads(common.get_secret())
    # Fetch S3 file based on Key/Bucket provided from event
    file_name = fetch_S3_file(event=event)
    # load file into pardot, return a int batch id
    batch_id = pardot_load(api_auth_details,file_name)
    put_DynamoDB_batch_id(table="Consent-JobStatus",batch_id=batch_id)


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))
    logger.info('Recieved Event')
    # Start Data Load of S3 file to Pardot
    main(event)
