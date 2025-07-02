import json
import os
import boto3
import logging
import sys
from collections import defaultdict
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import inspect

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


def L_6408ABDE(serviceCode, quotaCode, threshold, region):
    """
    Checks the Number of instances per Elasticsearch domain
    :param serviceCode: The service code (should be 'vpc' for NAT gateway)
    :param quotaCode: The quota code for private IP addresses per NAT gateway
    :param threshold: The threshold value (e.g., 0.8 for 80%)
    :param region: The AWS region to check
    :return: None
    """
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False
    max_instance_count = 0    
    # Create boto3 clients
    es_client = boto3.client('es', region_name=region)
    sq_client = boto3.client('service-quotas', region_name=region)

    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()

    try:
        # Get the service quota
        serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        
        logger.info(f"Instances per domain for Elasticsearch quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename= f'tests/{inspect.stack()[0][3]}.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename,'r') as test_file_content:
                es_domains = json.load(test_file_content)
        else:
            # Get all Elasticsearch Domains
            paginator = es_client.get_paginator('list-domain-names')
            es_domain_names = []
            for page in paginator.paginate():
                es_domain_names.extend(page['DomainName'])

            logger.info(es_domains)
            paginator = es_client.get_paginator('describe_elasticsearch_domains(DomainNames=es_domain_names)')
            es_domains = []
            for page in paginator.paginate():
                es_domains.extend(page['DomainStatusList'])


        for es_domain in es_domains:
            es_domain_id = es_domain['DomainId']
            instance_count = es_domain['ElasticsearchClusterConfig']['InstanceCount']
            
            usage_percentage = (instance_count / serviceQuotaValue) * 100
            
            logger.info(f"Elastic Search Domain {es_domain_id}: {instance_count} instances used out of {serviceQuotaValue}")

            if max_instance_count < instance_count:
                    max_instance_count = instance_count
                    logger.info(f"Max value={max_instance_count}")

            if usage_percentage > float(threshold) * 100:
                data = {
                "resourceARN" : es_domain_id,
                "usageValue" : instance_count
                }
                resourceListCrossingThreshold.append(data)
                sendQuotaThresholdEvent = True
                logger.warning(f"ES Domain {es_domain_id} Instance count ({instance_count}) exceeds {float(threshold) * 100}% of the quota ({serviceQuotaValue})")
            
        # Update quota usage
        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_private_ip_count),json.dumps(resourceListCrossingThreshold),sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking ES Domain Instances quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}") 

    
def L_BB24F6E5(serviceCode, quotaCode, threshold, region):
    """
    Monitors the Network Address Usage (NAU) using CloudWatch metrics
    :param serviceCode: The service code (should be 'vpc' for NAU)
    :param quotaCode: The quota code for NAU
    :param threshold: The threshold value (e.g., 0.8 for 80%)
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False
    max_nau = 0
    # Create boto3 clients
    cloudwatch = boto3.client('cloudwatch', region_name=region)
    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)
    
    try:
        # Get the service quota
        serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Network Address Usage (NAU) quota: {serviceQuotaValue}")

        if is_testing_enabled:
            vpc_test_filename= f'tests/{inspect.stack()[0][3]}_describe_vpcs.json'
            logger.info(f"Detected testing enabled. Using test payload from {vpc_test_filename}")
            with open(vpc_test_filename,'r') as test_file_content:
                vpcs = json.load(test_file_content)
        else:
            # Get all VPCs
            vpcs = ec2.describe_vpcs()['Vpcs']
        
        # Set up the CloudWatch query
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=5)  # Get data for the last 5 minutes

        for vpc in vpcs:
            vpc_id = vpc['VpcId']
            if is_testing_enabled:
                metrics_test_filename= f'tests/{inspect.stack()[0][3]}_get_metric_statistics.json'
                logger.info(f"Detected testing enabled. Using test payload from {metrics_test_filename}")
                with open(metrics_test_filename,'r') as test_file_content:
                    response = json.load(test_file_content)
            else:
                response = cloudwatch.get_metric_statistics(
                    Namespace='AWS/EC2',
                    MetricName='NetworkAddressUsage',
                    Dimensions=[
                        {
                            'Name': 'Per-VPC Metrics',
                            'Value': vpc_id
                        },
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=300,  # 5 minutes
                    Statistics=['Maximum']
                )

            # Extract the most recent datapoint
            datapoints = response['Datapoints']
            if datapoints:
                latest_datapoint = max(datapoints, key=lambda x: x['Timestamp'])
                nau = float(latest_datapoint['Maximum'])

                usage_percentage = (nau / serviceQuotaValue) * 100
                
                logger.info(f"VPC {vpc_id}: NAU {nau} out of {serviceQuotaValue}")
                if max_nau < nau:
                    max_nau = nau
                    logger.info(f"Max value={max_nau}")

                if usage_percentage > float(threshold) * 100:
                    data = {
                    "resourceARN" : vpc_id,
                    "usageValue" : nau
                    }
                    resourceListCrossingThreshold.append(data)
                    sendQuotaThresholdEvent = True
                    logger.warning(f"VPC {vpc_id} NAU ({nau}) exceeds {float(threshold) * 100}% of the quota ({serviceQuotaValue})")
                
            else:
                logger.info(f"No NAU data available for VPC {vpc_id}")

        # Update quota usage
        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_nau),json.dumps(resourceListCrossingThreshold),sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error monitoring Network Address Usage: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        

        
def L_DFA99DE7(serviceCode, quotaCode, threshold, region):
    """
    Checks the Private IP address quota per NAT gateway
    :param serviceCode: The service code (should be 'vpc' for NAT gateway)
    :param quotaCode: The quota code for private IP addresses per NAT gateway
    :param threshold: The threshold value (e.g., 0.8 for 80%)
    :param region: The AWS region to check
    :return: None
    """
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False
    max_private_ip_count = 0    
    # Create boto3 clients
    ec2_client = boto3.client('ec2', region_name=region)
    sq_client = boto3.client('service-quotas', region_name=region)

    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()

    try:
        # Get the service quota
        serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        
        logger.info(f"Private IP addresses per NAT gateway quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename= f'tests/{inspect.stack()[0][3]}.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename,'r') as test_file_content:
                nat_gateways = json.load(test_file_content)
        else:
            # Get all NAT gateways
            paginator = ec2_client.get_paginator('describe_nat_gateways')
            nat_gateways = []
            for page in paginator.paginate():
                nat_gateways.extend(page['NatGateways'])


        for nat_gateway in nat_gateways:
            nat_gateway_id = nat_gateway['NatGatewayId']
            private_ip_count = float(len(nat_gateway['NatGatewayAddresses']))
            
            usage_percentage = (private_ip_count / serviceQuotaValue) * 100
            
            logger.info(f"NAT Gateway {nat_gateway_id}: {private_ip_count} private IPs used out of {serviceQuotaValue}")

            if max_private_ip_count < private_ip_count:
                    max_private_ip_count = private_ip_count
                    logger.info(f"Max value={max_private_ip_count}")

            if usage_percentage > float(threshold) * 100:
                data = {
                "resourceARN" : nat_gateway_id,
                "usageValue" : private_ip_count
                }
                resourceListCrossingThreshold.append(data)
                sendQuotaThresholdEvent = True
                logger.warning(f"NAT Gateway {nat_gateway_id} private IP usage ({private_ip_count}) exceeds {float(threshold) * 100}% of the quota ({serviceQuotaValue})")
            
        # Update quota usage
        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_private_ip_count),json.dumps(resourceListCrossingThreshold),sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking NAT gateway private IP quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")  


def L_C4B238BF(serviceCode, quotaCode, threshold,region):
    """
    Checks VPN connections per client VPN
    :param serviceCode: The service code
    :param quotaCode: The quota code
    :param threshold: The threshold value
    :return: None
    """

    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()

    maxConcurrentClientVpnConnectionsPerCVPNEndpoint = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    try:
        # Get current quota

        serviceQuotaValue = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)['Quota']['Value']

        # Create a reusable Paginator
        paginator = ec2.get_paginator('describe_client_vpn_endpoints')    
        
        if is_testing_enabled:
            test_filename_vpn_endpoints = f'tests/{inspect.stack()[0][3]}_describe_client_vpn_endpoints.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename_vpn_endpoints}")
            with open(test_filename_vpn_endpoints,'r') as test_file_content:
                test_vpn_endpoints = json.load(test_file_content)
            test_filename_vpn_connections = f'tests/{inspect.stack()[0][3]}_describe_vpn_connections.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename_vpn_connections}")
            with open(test_filename_vpn_connections,'r') as test_file_content:
                vpn_endpoint_connections_test = json.load(test_file_content)
            page_iterator = test_vpn_endpoints
        else:
            # Create a PageIterator from the Paginator
            page_iterator = paginator.paginate()
        for vpn_endpoints in page_iterator:
            for vpn_endpoint in vpn_endpoints['ClientVpnEndpoints']:
                if is_testing_enabled:
                    vpn_endpoint_connections = vpn_endpoint_connections_test['VpnConnections']
                else:
                    # Per each VPN endpoint found, call the describe API and sum the connections
                    vpn_endpoint_connections = ec2.describe_vpn_connections(
                        ClientVpnEndpointId=vpn_endpoint['ClientVpnEndpointId']
                    )['VpnConnections']
                numVpnEndpointConnections = len(vpn_endpoint_connections)
                logger.info(f"CVPN Endpoint Id={vpn_endpoint['ClientVpnEndpointId']}. Number of VpnEndpointConnections={numVpnEndpointConnections}")
                
                if numVpnEndpointConnections/serviceQuotaValue > float(threshold)/100:
                    logger.info(f"Exceeding Threshold for this CVPN Endpoint={vpn_endpoint['ClientVpnEndpointId']}")
                    sendQuotaThresholdEvent = True
                    data = {
                        "resourceARN" : vpn_endpoint['ClientVpnEndpointId'],
                        "usageValue" : numVpnEndpointConnections
                        }
                    resourceListCrossingThreshold.append(data)
                if maxConcurrentClientVpnConnectionsPerCVPNEndpoint < numVpnEndpointConnections:
                    maxConcurrentClientVpnConnectionsPerCVPNEndpoint = numVpnEndpointConnections
                    logger.info(f"Max Value: {maxConcurrentClientVpnConnectionsPerCVPNEndpoint}")
        # Update quota usage
        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxConcurrentClientVpnConnectionsPerCVPNEndpoint),json.dumps(resourceListCrossingThreshold),sendQuotaThresholdEvent)
 
    except ClientError as e:
        logger.error(f"Error checking Concurrent Client VPN Connections per Endpoint quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")   


          
def L_7E9ECCDB(serviceCode, quotaCode, threshold,region):
    """
    Checks VPC Peering Connections per VPC
    :param serviceCode: The service code
    :param quotaCode: The quota code
    :param threshold: The threshold value
    :return: None
    """

    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()

    maxConcurrentVpcPeeringConnectionsPerVPC = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    try:
        # Get current quota

        serviceQuotaValue = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)['Quota']['Value']

        # Create a reusable Paginator
        paginator = ec2.get_paginator('describe_vpcs')    
        
        if is_testing_enabled:
            test_filename_vpcs = f'tests/{inspect.stack()[0][3]}_describe_vpcs.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename_vpcs}")
            with open(test_filename_vpcs,'r') as test_file_content:
                test_vpcs = json.load(test_file_content)
            test_filename_vpc_peering_connections = f'tests/{inspect.stack()[0][3]}_describe_vpc_peering_connections.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename_vpc_peering_connections}")
            with open(test_filename_vpc_peering_connections,'r') as test_file_content:
                vpc_peering_connections_test = json.load(test_file_content)
            page_iterator = test_vpcs
        else:
            # Create a PageIterator from the Paginator
            page_iterator = paginator.paginate()
        for vpcs in page_iterator:
            for vpc in vpcs['Vpcs']:
                if is_testing_enabled:
                    vpc_peering_connections_accepted = vpc_peering_connections_test
                    vpc_peering_connections_requested = vpc_peering_connections_test
                else:
                    # Per each VPC found, call the describe API and sum the connections
                    vpc_peering_connections_accepted = ec2.describe_vpc_peering_connections(
                        Filters=[
                            {
                                'Name': 'accepter-vpc-info.vpc-id',
                                'Values': [
                                    vpc['VpcId'],
                                ],
                            },
                            ]
                    )['VpcPeeringConnections']

                    vpc_peering_connections_requested = ec2.describe_vpc_peering_connections(
                        Filters=[
                            {
                                'Name': 'requester-vpc-info.vpc-id',
                                'Values': [
                                    vpc['VpcId'],
                                ],
                            },
                            ]
                    )['VpcPeeringConnections']

                numVpcPeeringConnections = len(vpc_peering_connections_accepted) + len(vpc_peering_connections_requested)
                logger.info(f"VPC Id={vpc['VpcId']}. Number of VpcPeeringConnections={numVpcPeeringConnections}")
                
                if numVpcPeeringConnections/serviceQuotaValue > float(threshold)/100:
                    logger.info(f"Exceeding Threshold for this VPC Id={vpc['VpcId']}")
                    sendQuotaThresholdEvent = True
                    data = {
                        "resourceARN" : vpc['VpcId'],
                        "usageValue" : numVpcPeeringConnections
                        }
                    resourceListCrossingThreshold.append(data)
                if maxConcurrentVpcPeeringConnectionsPerVPC < numVpcPeeringConnections:
                    maxConcurrentVpcPeeringConnectionsPerVPC = numVpcPeeringConnections
                    logger.info(f"Max Value: {maxConcurrentVpcPeeringConnectionsPerVPC}")
        # Update quota usage
        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxConcurrentVpcPeeringConnectionsPerVPC),json.dumps(resourceListCrossingThreshold),sendQuotaThresholdEvent)
 
    except ClientError as e:
        logger.error(f"Error checking VPC peering connections per VPC quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

def L_407747CB(serviceCode, quotaCode, threshold,region):
    """
    Checks Subnets per VPC
    :param serviceCode: The service code
    :param quotaCode: The quota code
    :param threshold: The threshold value
    :return: None
    """

    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()

    maxSubnetsPerVPC = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    try:
        # Get current quota

        serviceQuotaValue = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)['Quota']['Value']

        # Create a reusable Paginator
        paginator = ec2.get_paginator('describe_vpcs')    
        
        if is_testing_enabled:
            test_filename_vpcs = f'tests/{inspect.stack()[0][3]}_describe_vpcs.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename_vpcs}")
            with open(test_filename_vpcs,'r') as test_file_content:
                test_vpcs = json.load(test_file_content)
            test_filename_vpc_subnets = f'tests/{inspect.stack()[0][3]}_describe_subnets.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename_vpc_subnets}")
            with open(test_filename_vpc_subnets,'r') as test_file_content:
                vpc_subnets_test = json.load(test_file_content)
            page_iterator = test_vpcs
        else:
            # Create a PageIterator from the Paginator
            page_iterator = paginator.paginate()
        for vpcs in page_iterator:
            for vpc in vpcs['Vpcs']:
                if is_testing_enabled:
                    vpc_subnets = vpc_subnets_test
                else:
                    # Per each VPC found, call the describe API and sum the connections
                    vpc_subnets = ec2.describe_subnets(
                        Filters=[
                            {
                                'Name': 'vpc-id',
                                'Values': [
                                    vpc['VpcId'],
                                ],
                            },
                            ]
                    )['Subnets']

                    

                numVpcSubnets = len(vpc_subnets)
                logger.info(f"VPC Id={vpc['VpcId']}. Number of Subnets={numVpcSubnets}")
                
                if numVpcSubnets/serviceQuotaValue > float(threshold)/100:
                    logger.info(f"Exceeding Threshold for this VPC Id={vpc['VpcId']}")
                    sendQuotaThresholdEvent = True
                    data = {
                        "resourceARN" : vpc['VpcId'],
                        "usageValue" : numVpcSubnets
                        }
                    resourceListCrossingThreshold.append(data)
                if maxSubnetsPerVPC < numVpcSubnets:
                    maxSubnetsPerVPC = numVpcSubnets
                    logger.info(f"Max Value: {maxSubnetsPerVPC}")
        # Update quota usage
        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxSubnetsPerVPC),json.dumps(resourceListCrossingThreshold),sendQuotaThresholdEvent)
 
    except ClientError as e:
        logger.error(f"Error checking Subnets per VPC quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")   


def L_DF5E4CA3(serviceCode, quotaCode, threshold,region):
    """
    Checks Network interface total usage
    :param serviceCode: The service code
    :param quotaCode: The quota code
    :param threshold: The threshold value
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    numNetworkInterfacesPerRegion = 0
    sendQuotaThresholdEvent = False
    try:
        serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    except Exception as e:
        logger.info(f"Error calling get_service_quota: {e}")
        serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        
    serviceQuotaValue = serviceQuota['Quota']['Value']
    
    if is_testing_enabled:
        test_filename= f'tests/{inspect.stack()[0][3]}_describe_network_interfaces.json'
        logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
        with open(test_filename,'r') as test_file_content:
            page_iterator = json.load(test_file_content)
    else:
        # Create a reusable Paginator
        paginator = ec2.get_paginator('describe_network_interfaces')
        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate()

    for response in page_iterator:
        networkInterfacesListObject = response['NetworkInterfaces']
        numNetworkInterfacesPerRegion += len(networkInterfacesListObject)
    if numNetworkInterfacesPerRegion/serviceQuotaValue > float(threshold)/100:
        logger.info(f"Exceeding Threshold for No of Network Interfaces={numNetworkInterfacesPerRegion}")
        sendQuotaThresholdEvent = True

        
    updateQuotaUsage(region,quotaCode,serviceCode, str(serviceQuotaValue), str(numNetworkInterfacesPerRegion),"",sendQuotaThresholdEvent)




def L_D18FCD1D(serviceCode, quotaCode, threshold,region):
    """
    Checks the total general purpose SSD gp2 storage
    :param serviceCode: The service code
    :param quotaCode: The quota code
    :param threshold: The threshold value
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False
    totalGeneralPurposeSSDGP2Storage = 0
    serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    if is_testing_enabled:
        test_filename= f'tests/{inspect.stack()[0][3]}_describe_volumes.json'
        logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
        with open(test_filename,'r') as test_file_content:
            page_iterator = json.load(test_file_content)
    else:
        # Create a reusable Paginator
        paginator = ec2.get_paginator('describe_volumes')
        
        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate(
            Filters=[
            {
                'Name': 'volume-type',
                'Values': [
                    'gp2',
                ],
            },
            ]
        )
    
    for response in page_iterator:
        volumesObject = response['Volumes']
    # Compute EBS Total Usage
        for item in volumesObject: 
            totalGeneralPurposeSSDGP2Storage += item['Size']
    if totalGeneralPurposeSSDGP2Storage/serviceQuotaValue > float(threshold)/100:
        logger.info(f"Exceeding Threshold for total general purpose SSD gp2 storage={totalGeneralPurposeSSDGP2Storage}")
        sendQuotaThresholdEvent = True

    totalGeneralPurposeSSDGP2Storage /= 1024
    logger.info(f"Total (TiB) of general purpose SSD gp2 storage = {totalGeneralPurposeSSDGP2Storage}")
    updateQuotaUsage(region,quotaCode,serviceCode, str(serviceQuotaValue), str(totalGeneralPurposeSSDGP2Storage),"",sendQuotaThresholdEvent)
    
def L_CE3125E5(serviceCode, quotaCode, threshold, region):
    """
    Counts the number of instances behind each load balancer and gives the sum of all instances
    
    :param serviceCode: AWS service code for quotas
    :param quotaCode: Specific quota code to check
    :param threshold: Percentage threshold to trigger warning (string or float)
    :param region: The AWS region to check
    :return: None
    """
    try:
        # Convert threshold to float
        threshold = float(threshold)
        
        # Create boto3 clients
        elb_client = boto3.client('elb', region_name=region)
        elbv2_client = boto3.client('elbv2', region_name=region)
        sq = boto3.client('service-quotas', region_name=region)
        
        total_instances = 0
        lb_instance_counts = {}
        
        # Get the service quota value
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
            serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        except ClientError as e:
            logger.error(f"Error getting service quota: {e}")
            return

        # Count instances for Classic Load Balancers
        try:
            paginator = elb_client.get_paginator('describe_load_balancers')
            for page in paginator.paginate():
                for lb in page['LoadBalancerDescriptions']:
                    lb_name = lb['LoadBalancerName']
                    instance_count = len(lb['Instances'])
                    lb_instance_counts[lb_name] = instance_count
                    total_instances += instance_count
        except ClientError as e:
            logger.error(f"Error describing Classic Load Balancers: {e}")

        # Count instances for Application and Network Load Balancers
        try:
            paginator = elbv2_client.get_paginator('describe_load_balancers')
            for page in paginator.paginate():
                for lb in page['LoadBalancers']:
                    lb_arn = lb['LoadBalancerArn']
                    lb_name = lb['LoadBalancerName']
                    
                    # Get target groups for this load balancer
                    try:
                        target_groups = elbv2_client.describe_target_groups(
                            LoadBalancerArn=lb_arn
                        )['TargetGroups']
                        
                        # Count unique instances across all target groups
                        instances = set()
                        for tg in target_groups:
                            try:
                                targets = elbv2_client.describe_target_health(
                                    TargetGroupArn=tg['TargetGroupArn']
                                )['TargetHealthDescriptions']
                                
                                for target in targets:
                                    if 'Id' in target['Target']:
                                        instances.add(target['Target']['Id'])
                            except ClientError as e:
                                logger.error(f"Error describing target health: {e}")
                                continue
                        
                        instance_count = len(instances)
                        lb_instance_counts[lb_name] = instance_count
                        total_instances += instance_count
                        
                    except ClientError as e:
                        logger.error(f"Error describing target groups: {e}")
                        continue
                        
        except ClientError as e:
            logger.error(f"Error describing Application/Network Load Balancers: {e}")

        # Log results
        logger.info("Number of instances behind each load balancer:")
        for lb_name, count in lb_instance_counts.items():
            logger.info(f"{lb_name}: {count} instances")

        logger.info(f"\nTotal number of instances across all load balancers: {total_instances}")

        # Calculate usage percentage
        if serviceQuotaValue > 0:  # Prevent division by zero
            usage_percentage = (total_instances / serviceQuotaValue) * 100

            # Check if usage exceeds threshold
            if usage_percentage >= threshold:
                logger.warning(
                    f"WARNING: Resource usage has reached {usage_percentage:.2f}% of the quota limit!\n"
                    f"Current usage: {total_instances}\n"
                    f"Quota limit: {serviceQuotaValue}\n"
                    f"Service: {serviceCode}\n"
                    f"Region: {region}"
                )
        else:
            logger.error("Service quota value is 0 or invalid")
            return

        # Update quota usage tracking
        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(total_instances))

    except Exception as e:
        logger.error(f"Unexpected error in L_CE3125E5: {str(e)}")
        raise

def L_43872EB7(serviceCode, quotaCode, threshold,region):
    """
    Checks route tables per transit gateway
    :param serviceCode: The service code
    :param quotaCode: The quota code
    :param threshold: The threshold value
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxTransitGatewayRouteTablesPerTgw = 0
    sendQuotaThresholdEvent = False
    resourceListCrossingThreshold = []
    try:
        serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    except Exception as e:
        logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
        serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        
    serviceQuotaValue = serviceQuota['Quota']['Value']
    logger.info(serviceQuota['Quota']['Value'])
    if is_testing_enabled:
        test_filename= f'tests/{inspect.stack()[0][3]}_describe_transit_gateways.json'
        logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
        with open(test_filename,'r') as test_file_content:
            page_iterator = json.load(test_file_content)
    else:
        # Create a reusable Paginator
        paginator = ec2.get_paginator('describe_transit_gateways')
        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate()
    for response in page_iterator:
        tgwListObject = response['TransitGateways']
        # Iterate through the JSON array 
        # Compute TGW Route Tables Usage
        for item in tgwListObject: 
            transitGatewayId = item['TransitGatewayId']
            if is_testing_enabled:
                test_filename= f'tests/{inspect.stack()[0][3]}_describe_transit_gateway_route_tables.json'
                logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
                with open(test_filename,'r') as test_file_content:
                    response_rt = json.load(test_file_content)
            else:
                response_rt = ec2.describe_transit_gateway_route_tables(
                    Filters=[
                    {
                        'Name': 'transit-gateway-id',
                        'Values': [
                            transitGatewayId,
                        ],
                    },
                    ]
                )
            tgwRouteTablesListObject=response_rt['TransitGatewayRouteTables']
            numTransitGatewayRouteTables = len(tgwRouteTablesListObject)
            logger.info(f"TGW_IF={transitGatewayId}. Number of Transit Gateway Route Tables={numTransitGatewayRouteTables}")
            
            if maxTransitGatewayRouteTablesPerTgw < numTransitGatewayRouteTables:
                maxTransitGatewayRouteTablesPerTgw = numTransitGatewayRouteTables
                logger.info(f"Max value={maxTransitGatewayRouteTablesPerTgw}")
                
            if numTransitGatewayRouteTables/serviceQuotaValue > float(threshold)/100:
                logger.info(f"Exceeding Threshold for this TWG_ID={transitGatewayId}")
                sendQuotaThresholdEvent = True
                data = {
                    "resourceARN" : transitGatewayId,
                    "usageValue" : numTransitGatewayRouteTables
                    }
                resourceListCrossingThreshold.append(data)
        
    updateQuotaUsage(region,quotaCode,serviceCode, str(serviceQuotaValue), str(maxTransitGatewayRouteTablesPerTgw), json.dumps(resourceListCrossingThreshold),sendQuotaThresholdEvent)

def L_1B52E74A(serviceCode, quotaCode, threshold,region):
    """
    Checks gateway VPC endpoints per region
    :param serviceCode: The service code
    :param quotaCode: The quota code
    :param threshold: The threshold value
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    numGatewayVPCEndpointsPerRegion = 0
    sendQuotaThresholdEvent = False
    try:
        serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    except Exception as e:
        logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
        serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']
    if is_testing_enabled:
        test_filename= f'tests/{inspect.stack()[0][3]}_describe_vpc_endpoints.json'
        logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
        with open(test_filename,'r') as test_file_content:
            page_iterator = json.load(test_file_content)
    else:
        # Create a reusable Paginator
        paginator = ec2.get_paginator('describe_vpc_endpoints')
        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate(
                Filters=[
            {
                'Name': 'vpc-endpoint-type',
                'Values': [
                    'Gateway',
                ]
            },
        ])

    for response in page_iterator:
        gwEndpointListObject = response['VpcEndpoints']
        numGatewayVPCEndpointsPerRegion += len(gwEndpointListObject)
    if numGatewayVPCEndpointsPerRegion/serviceQuotaValue > float(threshold)/100:
        logger.info(f"Exceeding Threshold for No of Gateway Endpoints Per Region={numGatewayVPCEndpointsPerRegion}")
        sendQuotaThresholdEvent = True
    else:
        sendQuotaThresholdEvent = False
        
    updateQuotaUsage(region,quotaCode,serviceCode, str(serviceQuotaValue), str(numGatewayVPCEndpointsPerRegion),"",sendQuotaThresholdEvent)


def L_45FE3B85(serviceCode, quotaCode, threshold,region):
    """
    Checks Egress Only Internet Gateways per region
    :param serviceCode: The service code
    :param quotaCode: The quota code
    :param threshold: The threshold value
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    numGatewayVPCEndpointsPerRegion = 0
    sendQuotaThresholdEvent = False
    try:
        serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    except Exception as e:
        logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
        serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']
    if is_testing_enabled:
        test_filename= f'tests/{inspect.stack()[0][3]}_describe_vpc_endpoints.json'
        logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
        with open(test_filename,'r') as test_file_content:
            page_iterator = json.load(test_file_content)
    else:
        # Create a reusable Paginator
        paginator = ec2.get_paginator('describe_egress_only_internet_gateways')
        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate()

    for response in page_iterator:
        egressGatewayListObject = response['EgressOnlyInternetGateways']
        numEgressGatewaysPerRegion += len(egressGatewayListObject)
    if numEgressGatewaysPerRegion/serviceQuotaValue > float(threshold)/100:
        logger.info(f"Exceeding Threshold for No of Egress Only Internet Gateway  Per Region={numEgressGatewaysPerRegion}")
        sendQuotaThresholdEvent = True
    else:
        sendQuotaThresholdEvent = False
        
    updateQuotaUsage(region,quotaCode,serviceCode, str(serviceQuotaValue), str(numEgressGatewaysPerRegion),"",sendQuotaThresholdEvent)


def L_DC2B2D3D(serviceCode, quotaCode, region, threshold):
    # check for number of S3 buckets
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False
    bucket_count = 0
    try:
        serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    except Exception as e:
        logger.error(f"Error calling get_service_quota: {e}")
        serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    
    serviceQuotaValue = serviceQuota['Quota']['Value']
    logger.info(f"S3 Bucket Quota: {serviceQuotaValue}")

    # Create a new S3 client for the specified region
    s3_regional = boto3.client('s3', region_name=region)


    if is_testing_enabled:
        test_filename= f'tests/{inspect.stack()[0][3]}_list_buckets.json'
        logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
        with open(test_filename,'r') as test_file_content:
            response = json.load(test_file_content)
    else:
        response = s3_regional.list_buckets()

    bucket_count = 0
    bucket_count += len(response['Buckets'])    


    logger.info(f"Current S3 Bucket Count : {bucket_count}")

    # Calculate the usage percentage
    usage_percentage = (bucket_count / serviceQuotaValue) * 100
    logger.info(f"S3 Bucket Usage: {usage_percentage:.2f}%")

    # Check if usage exceeds the threshold
    if usage_percentage >= float(threshold):
        sendQuotaThresholdEvent = True
        logger.warning(f"S3 Bucket usage ({usage_percentage:.2f}%) exceeds the threshold of {threshold}%")

    # Update the quota usage in DynamoDB
    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(bucket_count),"",sendQuotaThresholdEvent)

def L_0DA4ABF3(serviceCode, quotaCode, region, threshold):
    """
    Checks policies attached to a role 
    :param serviceCode: The service code
    :param quotaCode: The quota code
    :param threshold: The threshold value
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode,QuotaCode=quotaCode )
    # Quota code for managed policies per role 
    serviceQuotaValue = serviceQuota['Quota']['Value']

    # List all roles
    
    maxAttachedPolicyPerRole=0
    resourceListCrossingThreshold = []
    if is_testing_enabled:
        list_roles_filename = f'tests/{inspect.stack()[0][3]}_list_roles.json'
        logger.info(f"Detected testing enabled. Using test payload from {list_roles_filename}")
        with open(list_roles_filename,'r') as test_file_content:
            role_paginator = json.load(test_file_content)
    else:
        role_paginator = iam_client.get_paginator('list_roles').paginate()
     # Create a PageIterator from the Paginator

    for role_page in role_paginator:
        for role in role_page['Roles']:
            role_name = role['RoleName'] 
            role_arn = role['Arn']
            # Get attached managed policies
            if is_testing_enabled:
                list_attached_role_policies_filename = f'tests/{inspect.stack()[0][3]}_list_attached_role_policies.json'
                logger.info(f"Detected testing enabled. Using test payload from {list_attached_role_policies_filename}")
                with open(list_attached_role_policies_filename,'r') as test_file_content:
                    attached_policies = json.load(test_file_content)['AttachedPolicies']
            else:
                attached_policies = iam_client.list_attached_role_policies(RoleName=role_name)['AttachedPolicies']

            attached_policy_count = len(attached_policies)
              
            # Check if the number of attached managed policies exceeds the service quota
            if attached_policy_count/serviceQuotaValue > float(threshold)/100:
                sendQuotaThresholdEvent = True
                logger.warning(f"Exceeding Threshold for this Role={role_name}")

                data = {
                  "resourceARN" : role_arn,
                  "usageValue" : attached_policy_count
                }
                resourceListCrossingThreshold.append(data)
            if maxAttachedPolicyPerRole < attached_policy_count:
                maxAttachedPolicyPerRole = attached_policy_count
                   
    
    updateQuotaUsage(region,quotaCode,serviceCode, str(serviceQuotaValue), str(maxAttachedPolicyPerRole), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)




def L_BF35879D(serviceCode, quotaCode, region, threshold):
    """
    Checks IAM certificates 
    :param serviceCode: The service code
    :param quotaCode: The quota code
    :param threshold: The threshold value
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False
   
    # Create an IAM client

    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')

    # Get the service quota for server certificates
    
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode,QuotaCode=quotaCode )
    

    # Get the service quota for server certificates

    serviceQuotaValue = serviceQuota['Quota']['Value']
    # Initialize certificate counter
    certificate_count = 0
    certificate_count_exceeds_threshold = 0
    if is_testing_enabled:
        list_server_certs_filename = f'tests/{inspect.stack()[0][3]}_list_server_certificates.json'
        logger.info(f"Detected testing enabled. Using test payload from {list_server_certs_filename}")
        with open(list_server_certs_filename,'r') as test_file_content:
            paginator = json.load(test_file_content)
    else:
        # Create paginator for listing server certificates
        paginator = iam_client.get_paginator('list_server_certificates').paginate()
        
    # Iterate through all pages and count certificates
    for page in paginator:
        certificates = page['ServerCertificateMetadataList']
        page_count = len(certificates)
        certificate_count += page_count
    
    logger.info(f"\nTotal number of IAM server certificates: {certificate_count}")

    # Check if the number of server certificates exceeds the service quota
    if certificate_count/serviceQuotaValue > float(threshold)/100:
        sendQuotaThresholdEvent = True
        certificate_count_exceeds_threshold = certificate_count
        logger.warning(f"Warning: The number of server certificates ({certificate_count}) exceeds the threshol ({threshold}")
    
    # Update the quota usage in DynamoDB
    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(certificate_count_exceeds_threshold),"", sendQuotaThresholdEvent)





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
                if hasattr(sys.modules[__name__],QuotaReportingFunc):
                    getattr(sys.modules[__name__], QuotaReportingFunc)(serviceCode=serviceCodeValue,quotaCode=quotaCodeValue, threshold=thresholdValue, region=region)
                else:
                    logger.warning(f"Quota not implemented: {QuotaReportingFunc}. Skipping this check for region {region}")
        else:
            logger.debug("Pulling Quotas for current region ",currentRegion)
            if hasattr(sys.modules[__name__],QuotaReportingFunc):
                    getattr(sys.modules[__name__], QuotaReportingFunc)(serviceCode=serviceCodeValue,quotaCode=quotaCodeValue, threshold=thresholdValue, region=region)
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


