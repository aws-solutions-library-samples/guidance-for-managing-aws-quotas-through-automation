import json
import os
import boto3
import logging
import sys
import csv
import quota_update_csv
import aws_quotas
from quota_update_csv import updateQuotaUsage
from collections import defaultdict

# Inject updateQuotaUsage function into aws_quotas module
aws_quotas.updateQuotaUsage = updateQuotaUsage
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import inspect
import os.path

# Setup logger
# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Add a stdout handler if one doesn't exist already
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# Setup boto3 clients
ec2 = boto3.client('ec2')
sq = boto3.client('service-quotas')

# CSV file path for quota usage (defaults to quota_usage.csv in current directory if not set)
quota_csv_path = os.environ.get('QUOTA_CSV_PATH', 'quota_usage.csv')

logger.info("Loading function")

def get_quota_csv_path():
    """
    Get the path to the CSV file for quota usage tracking
    :return: The path to the CSV file
    """
    # Use the global variable that's already set from environment variables
    return quota_csv_path

def ensure_csv_exists():
    """
    Ensure that the CSV file exists with proper headers
    :return: None
    """
    csv_path = get_quota_csv_path()
    
    # Check if file exists
    if not os.path.exists(csv_path):
        logger.info(f"Creating new quota usage CSV file at {csv_path}")
        with open(csv_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Write headers
            writer.writerow(['QuotaCode', 'ServiceCode', 'Region', 'LimitValue', 'UsageValue', 'ResourceList', 'Timestamp'])
        logger.info(f"Created new CSV file with headers at {csv_path}")

# Remove duplicate function - using the one from quota_update_csv.py

    



if __name__ == "__main__":
    """
    Entry point
    """
    with open('../config/QuotaList.json', 'r') as f:
        config = json.load(f)
    logger.info(f"Using the following config: {json.dumps(config,indent=2)}")
    for quotaObject in config:
        logger.info(f"Processing: {quotaObject}")
        serviceCodeValue = quotaObject['ServiceCode']
        quotaCodeValue = quotaObject['QuotaCode']
        thresholdValue = quotaObject['Threshold']
        QuotaReportingFunc = quotaCodeValue.replace("-", "_")
        logger.info(f"Running function: {QuotaReportingFunc}")
        currentRegion= os.environ.get('AWS_REGION','us-east-1')
        regionList = os.environ.get('REGION_LIST','')
        if regionList == '':
            regions = [currentRegion]
        else:
            regions= regionList.split(',')
        if(quotaObject['QuotaAppliedAtLevel'] == 'Regional'):
            for region in regions:
                logger.debug("Pulling Quotas for ",region)
                if hasattr(aws_quotas, QuotaReportingFunc):
                    getattr(aws_quotas, QuotaReportingFunc)(serviceCode=serviceCodeValue,quotaCode=quotaCodeValue, threshold=thresholdValue, region=region)
                else:
                    logger.warning(f"Quota not implemented: {QuotaReportingFunc}. Skipping this check for region {region}")
        else:
            logger.debug("Pulling Quotas for current region ",currentRegion)
            if hasattr(aws_quotas, QuotaReportingFunc):
                    getattr(aws_quotas, QuotaReportingFunc)(serviceCode=serviceCodeValue,quotaCode=quotaCodeValue, threshold=thresholdValue, region=region)
            else:
                logger.warning(f"Quota not implemented: {QuotaReportingFunc}. Skipping this check for region {currentRegion}")


