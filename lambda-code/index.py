import json
import os
import boto3
import logging
import sys
from collections import defaultdict
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import inspect
# Missing this line:
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'local'))
import quota_update_dynamo
import aws_quotas

# Inject updateQuotaUsage function into aws_quotas module
aws_quotas.updateQuotaUsage = quota_update_dynamo.updateQuotaUsage

# Setup logger
# Setup logging
logger = logging.getLogger()
logger.setLevel("INFO")


# Setup boto3 clients
ec2 = boto3.client('ec2')
sq = boto3.client('service-quotas')
s3 = boto3.client('s3')
ddb = boto3.client('dynamodb')
elb_client = boto3.client('elb')
elbv2_client = boto3.client('elbv2')
eventbridge = boto3.client('events')



# Read env variables
bucket = os.environ['SERVICEQUOTA_BUCKET']
quotaUsageTable = os.environ['DDB_TABLE']
eventBus = os.environ['EVENT_BUS']

logger.info("Loading function")




def lambda_handler(event, context):
    """
    Lambda handler
    :param event: The event object
    :param context: The context object
    :return: a json response object with statusMessage 'OK' when succesfull
    """
    logger.info(f"Running lambda handler with event: {json.dumps(event,indent=2)}")
    key = os.environ['QUOTALIST_FILE']
    response = s3.get_object(Bucket = bucket, Key = key)
    content = response['Body']
    jsonObject = json.loads(content.read())
    logger.info(f"Using the following config: {json.dumps(jsonObject,indent=2)}")
    
    for quotaObject in jsonObject:
        logger.info(f"Processing: {quotaObject}")
        serviceCodeValue = quotaObject['ServiceCode']
        quotaCodeValue = quotaObject['QuotaCode']
        thresholdValue = quotaObject['Threshold']
        QuotaReportingFunc = quotaCodeValue.replace("-", "_")
        logger.info(f"Running function: {QuotaReportingFunc}")
        currentRegion= os.environ['AWS_REGION']
        regionList = os.environ['REGION_LIST']
        regions= regionList.split(',')
        if(quotaObject['QuotaAppliedAtLevel'] == 'Regional'):
            for region in regions:
                logger.debug("Pulling Quotas for ",region)
                if hasattr(aws_quotas,QuotaReportingFunc):
                    getattr(aws_quotas, QuotaReportingFunc)(serviceCode=serviceCodeValue,quotaCode=quotaCodeValue, threshold=thresholdValue, region=region)
                else:
                    logger.warning(f"Quota not implemented: {QuotaReportingFunc}. Skipping this check for region {region}")
        else:
            logger.debug("Pulling Quotas for current region ",currentRegion)
            if hasattr(aws_quotas,QuotaReportingFunc):
                    getattr(aws_quotas, QuotaReportingFunc)(serviceCode=serviceCodeValue,quotaCode=quotaCodeValue, threshold=thresholdValue, region=region)
            else:
                logger.warning(f"Quota not implemented: {QuotaReportingFunc}. Skipping this check for region {currentRegion}")

    response = {
                'isBase64Encoded': False,
                'statusCode': 200,
                'headers': {},
                'multiValueHeaders': {},
                'body': '{"statusMessage": "OK" }'
            }
    return response