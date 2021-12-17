import json
import boto3
import urllib.parse
from csv import reader
import base64
from botocore.exceptions import ClientError
import logging
import uuid
from smart_open import open
import datetime

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger("Main")

def _put_DynamoDB_warning(table=None, warning=None):
    client = boto3.client('dynamodb')
    job_id = uuid.uuid4()
    client.put_item(
        TableName=table,
        Item={
            'Job ID': {
                'S': job_id,
            },
            'isProcessed': {
                'BOOL': False
            },
            'warning' : {
                'S': warning
            }
        })
    logger.info(f'Added warning for job:{str(job_id)} to DynamoDB table')


def _cleanse_cell_value(cell_value):
    return cell_value.replace('"','')

def _transform_row(row_index,row,data_map):
    fout_row = []
    # Process file row besides headers
    if row_index > 0:
        for (col_index, consent_value) in enumerate(row):
            clean_consent_value = _cleanse_cell_value(consent_value)
            if col_index in data_map["ignoreColumns"]:
                continue
            elif col_index == 2:
                if clean_consent_value not in data_map["restrictValues"]["CONSENT_STATUS"]:
                    fout_row.append("")
                else:
                    fout_row.append(clean_consent_value)
            else:
                fout_row.append(clean_consent_value)
        fout_row.append(data_map["campaignMap"]["productionTest"])
    # Process file headers
    elif (row_index == 0):
        for (col_index, column_header) in enumerate(row):
            clean_column_header = _cleanse_cell_value(column_header)
            if (clean_column_header in data_map["fieldMap"].keys()):
                fout_row.append(data_map["fieldMap"][clean_column_header])
                continue
            if (clean_column_header in data_map["allowedFields"].keys()):
                data_map["allowedFields"][clean_column_header] = col_index
                fout_row.append(clean_column_header)
                continue
            else:
                data_map["ignoreColumns"].append(col_index)
        fout_row.append(data_map["Campaign ID"])
    else:
        pass
    api_ready_row = ",".join(str(val) for val in fout_row) + "\n"
    return api_ready_row
    
    
def _init_consent_map():
    return {
        "Integration ID": "Pardot-Outbound",
        "Campaign ID": "campaign_id",
        "fieldMap": {
            "EMAIL_ADDRESS": "Email"
        },
        "allowedFields": {
            "PROSPECT_ID": -1,
            "CONSENT_STATUS": -1,
            "SLACK_USER_TIPS_TRICKS_ALLOWED": -1,
            "SLACK_USER_PROMOTIONAL_OFFERS_ALLOWED": -1,
            "SLACK_USER_RESEARCH_EMAILS_ALLOWED": -1
            },
        "campaignMap": {
            "sandbox": 28655,
            "productionTest": 78561,
            "production": 3843
        },
        "ignoreColumns": [],
        "restrictValues": {
            "CONSENT_STATUS": ["Opt-in","Explicit Opt-out","Auto Opt-in","Auto Opt-out","No Preference"]
        }
    }


def init_s3_file_stream(bucket,key):
    logger.info(f'file stream init, source:{bucket}{key}')
    # any transformations, from column mapping to data type conversions should occur here.
    # should return new file or just stream to S3 'apiready' for performance reasons
    # at the bare minimum I need to append a campaign_id column
    date_operator = datetime.datetime.today()
    # may not need, as file may already have date.
    # curr_date =  f'_{date_operator.year}_{date_operator.month}_{date_operator.day}'
    date_subdir = key.split('/')[-2]
    destination_file = key.split('/')[-1]
    data_map = _init_consent_map()
    print("test + " + date_subdir)
    with open(f's3://{bucket}/{key}') as s3_source:
        for (row_index,line) in enumerate(s3_source):
            evalLine = line.split(',')
            api_ready_row = _transform_row(row_index, evalLine, data_map)
            #s3_destination.write(api_ready_row)
        with open(f's3://data-dea/outbound/pardot/apiready/{date_subdir}/{destination_file}', 'w') as s3_destination:
            pass# Business Logic
    logger.info('file i/o stream end')


def main(event):
    logger.info('Main Method')
    # Fetch S3 file based on Key/Bucket provided from event
    # consent_data_file = get_S3_file(event) #should be fine to store in Lambda memory
    # print(consent_data_file)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    init_s3_file_stream(bucket,key)



def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))
    logger.info('Recieved Event')
    # Start Data Load of S3 file to Pardot
    #main(event)

