import json
import os
import boto3
import logging
import sys
from botocore.exceptions import ClientError

# Setup logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Add a stdout handler if one doesn't exist already
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Setup boto3 clients
ddb = boto3.client('dynamodb')
eventbridge = boto3.client('events')

# Read env variables
quotaUsageTable = os.environ['DDB_TABLE']
eventBus = os.environ['EVENT_BUS']

logger.info("Loading function")

def updateQuotaUsage(region, quotaCode, serviceCode, serviceQuotaValue, usageValue, resourceListCrossingThreshold="", sendQuotaThresholdEvent=False):
    """
    Update the quota usage in the DynamoDB table
    :param quotaCode: The quota code
    :param serviceCode: The service code
    :param serviceQuotaValue: The service quota value
    :param usageValue: The usage value
    :param resourceListCrossingThreshold: The resource list crossing threshold
    :return: None
    """
    # Update the quota usage in the DynamoDB table
    logger.info(f"Updating quota usage in DynamoDB table for {serviceCode}:{quotaCode}")
    response = ddb.put_item(
        Item={
            'QuotaCode': {
                'S': quotaCode,
            },
            'ServiceCode': {
                'S': serviceCode,
            },
            'LimitValue': {
                'N': serviceQuotaValue,
            },
            'UsageValue': {
                'N': usageValue,
            },
            'ResourceList': {
                'S': resourceListCrossingThreshold,
            },
            'Region': {
                'S': region,
            },
        },
        ReturnConsumedCapacity='TOTAL',
        TableName=quotaUsageTable
    )
    
    logger.debug(response)
    if sendQuotaThresholdEvent == True:
        sendQuotaExceededEvent(region, quotaCode, serviceCode, serviceQuotaValue, usageValue, resourceListCrossingThreshold)



def sendQuotaExceededEvent(region, quotaCode, serviceCode, serviceQuotaValue, usageValue, resourceListCrossingThreshold=""):
    """
    Send the quota exceeded event to the event bridge
    :param quotaCode: The quota code
    :param serviceCode: The service code
    :param serviceQuotaValue: The service quota value
    :param usageValue: The usage value
    :param resourceListCrossingThreshold: The resource list crossing threshold
    :return: None
    """
    # Update the quota usage in the DynamoDB table
    logger.info(f"Sending quota exceeded event for {serviceCode}:{quotaCode}")    

    data = {
        "QuotaCode" : quotaCode,
        "LimitValue" : serviceQuotaValue,
        "Region" : region,
        "ResourceList" : resourceListCrossingThreshold,
        "ServiceCode" : serviceCode,
        "UsageValue" : usageValue
        }    

    response = eventbridge.put_events(
        Entries=[
            {
                'Source':'quota-guard',
                'DetailType':'quota-threshold-event',
                'Detail': json.dumps(data),
                'EventBusName': eventBus
            }
        ]
    )
    
    logger.info(response)