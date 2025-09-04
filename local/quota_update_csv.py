import json
import os
import boto3
import logging
import sys
import csv
from collections import defaultdict
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

def updateQuotaUsage(region, quotaCode, serviceCode, serviceQuotaValue, usageValue, resourceListCrossingThreshold="", sendQuotaThresholdEvent=False):
    """
    Update the quota usage in the CSV file
    :param region: The AWS region
    :param quotaCode: The quota code
    :param serviceCode: The service code
    :param serviceQuotaValue: The service quota value
    :param usageValue: The usage value
    :param resourceListCrossingThreshold: The resource list crossing threshold
    :param sendQuotaThresholdEvent: Whether to send a quota threshold event
    :return: None
    """
    # Ensure CSV file exists
    ensure_csv_exists()
    csv_path = get_quota_csv_path()
    
    # Get current timestamp
    timestamp = datetime.utcnow().isoformat()
    
    # Read existing data
    existing_data = []
    try:
        with open(csv_path, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)  # Skip headers
            for row in reader:
                existing_data.append(row)
    except FileNotFoundError:
        logger.warning(f"CSV file not found at {csv_path}, will create a new one")
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        
    # Find if entry already exists for this quota code, service code, and region
    updated = False
    for i, row in enumerate(existing_data):
        if len(row) >= 3 and row[0] == quotaCode and row[1] == serviceCode and row[2] == region:
            # Update existing entry
            existing_data[i] = [quotaCode, serviceCode, region, serviceQuotaValue, usageValue, resourceListCrossingThreshold, timestamp]
            updated = True
            break
            
    if not updated:
        # Add new entry
        existing_data.append([quotaCode, serviceCode, region, serviceQuotaValue, usageValue, resourceListCrossingThreshold, timestamp])
    
    # Write back to CSV
    try:
        with open(csv_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Write headers
            writer.writerow(['QuotaCode', 'ServiceCode', 'Region', 'LimitValue', 'UsageValue', 'ResourceList', 'Timestamp'])
            # Write data
            writer.writerows(existing_data)
        logger.info(f"Updated quota usage in CSV file for {serviceCode}:{quotaCode} in region {region}")
    except Exception as e:
        logger.error(f"Error writing to CSV file: {e}")
    
    if sendQuotaThresholdEvent == True:
        logger.warning(f"Quota exceeded for {quotaCode} in {region}. Service code: {serviceCode} - quota: {serviceQuotaValue} - usage: {usageValue} - Threshold: {resourceListCrossingThreshold}")

    



