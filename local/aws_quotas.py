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


# Setup boto3 clients - commented out to use region-specific clients in functions
# ec2 = boto3.client('ec2')
# sq = boto3.client('service-quotas')


    
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

    # Create region-specific boto3 clients
    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

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
        logger.error(f"Error checking NAT gateway private IP quota: {e}")
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
    
    # Create region-specific boto3 clients
    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)
    
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
    
    # Create region-specific boto3 clients
    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)
    
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
        resourceListCrossingThreshold = []
        sendQuotaThresholdEvent = False
        
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
                sendQuotaThresholdEvent = True
                resourceListCrossingThreshold = [{"usageValue": total_instances}]
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
        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(total_instances), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

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
    
    # Create region-specific boto3 clients
    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)
    
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
    
    # Create region-specific boto3 clients
    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)
    
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

def L_DC2B2D3D(serviceCode, quotaCode, region, threshold):
    # check for number of S3 buckets
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False
    bucket_count = 0
    
    # Create region-specific boto3 clients
    sq = boto3.client('service-quotas', region_name=region)
    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.error(f"Error calling get_service_quota: {e}")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    except Exception as e:
        logger.error(f"Error calling get_aws_default_service_quota for {serviceCode} and {quotaCode}: {e}")
        return
   
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
    sq_client = boto3.client('service-quotas', region_name=region)
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
    sq_client = boto3.client('service-quotas', region_name=region)

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





def L_CD17FD4B(serviceCode, quotaCode, threshold, region):
    """
    Monitors the Peered Network Address Usage (Peered NAU) using CloudWatch metrics
    :param serviceCode: The service code (should be 'vpc' for Peered NAU)
    :param quotaCode: The quota code for Peered NAU (L-CD17FD4B)
    :param threshold: The threshold value (e.g., 0.8 for 80%)
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False
    max_peered_nau = 0
    # Create boto3 clients
    cloudwatch = boto3.client('cloudwatch', region_name=region)
    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)
    
    try:
        # Get the service quota for L-CD17FD4B
        serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Peered Network Address Usage (Peered NAU) quota: {serviceQuotaValue}")

        if is_testing_enabled:
            vpc_test_filename= f'tests/{inspect.stack()[0][3]}_describe_vpcs.json'
            logger.info(f"Detected testing enabled. Using test payload from {vpc_test_filename}")
            with open(vpc_test_filename,'r') as test_file_content:
                vpcs = json.load(test_file_content)
        else:
            # Get all VPCs that have peering connections
            vpcs = ec2.describe_vpcs()['Vpcs']
        
        # Set up the CloudWatch query
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=5)  # Get data for the last 5 minutes

        for vpc in vpcs:
            vpc_id = vpc['VpcId']
            
            # Check if VPC has peering connections
            peering_connections = ec2.describe_vpc_peering_connections(
                Filters=[
                    {
                        'Name': 'accepter-vpc-info.vpc-id',
                        'Values': [vpc_id]
                    }
                ]
            )['VpcPeeringConnections']
            
            requester_peering = ec2.describe_vpc_peering_connections(
                Filters=[
                    {
                        'Name': 'requester-vpc-info.vpc-id', 
                        'Values': [vpc_id]
                    }
                ]
            )['VpcPeeringConnections']
            
            # Only check VPCs that have peering connections
            if peering_connections or requester_peering:
                if is_testing_enabled:
                    metrics_test_filename= f'tests/{inspect.stack()[0][3]}_get_metric_statistics.json'
                    logger.info(f"Detected testing enabled. Using test payload from {metrics_test_filename}")
                    with open(metrics_test_filename,'r') as test_file_content:
                        response = json.load(test_file_content)
                else:
                    response = cloudwatch.get_metric_statistics(
                        Namespace='AWS/EC2',
                        MetricName='PeeredNetworkAddressUsage',
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
                    peered_nau = float(latest_datapoint['Maximum'])

                    usage_percentage = (peered_nau / serviceQuotaValue) * 100
                    
                    logger.info(f"VPC {vpc_id}: Peered NAU {peered_nau} out of {serviceQuotaValue}")
                    if max_peered_nau < peered_nau:
                        max_peered_nau = peered_nau
                        logger.info(f"Max Peered NAU value={max_peered_nau}")

                    if usage_percentage > float(threshold) * 100:
                        data = {
                        "resourceARN" : vpc_id,
                        "usageValue" : peered_nau
                        }
                        resourceListCrossingThreshold.append(data)
                        sendQuotaThresholdEvent = True
                        logger.warning(f"VPC {vpc_id} Peered NAU ({peered_nau}) exceeds {float(threshold) * 100}% of the quota ({serviceQuotaValue})")
                    
                else:
                    logger.info(f"No Peered NAU data available for VPC {vpc_id}")

        # Update quota usage
        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_peered_nau),json.dumps(resourceListCrossingThreshold),sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error monitoring Peered Network Address Usage: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

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
            domain_names_response = es_client.list_domain_names()
            es_domain_names = [domain['DomainName'] for domain in domain_names_response['DomainNames']]
            
            if es_domain_names:
                es_domains_response = es_client.describe_elasticsearch_domains(DomainNames=es_domain_names)
                es_domains = es_domains_response['DomainStatusList']
            else:
                es_domains = []


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
        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_instance_count),json.dumps(resourceListCrossingThreshold),sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking ES Domain Instances quota: {e}")
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

    # Create region-specific boto3 clients
    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

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

    # Create region-specific boto3 clients
    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

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

def L_45FE3B85(serviceCode, quotaCode, threshold,region):
    """
    Checks Egress Only Internet Gateways per region
    :param serviceCode: The service code
    :param quotaCode: The quota code
    :param threshold: The threshold value
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    numEgressGatewaysPerRegion = 0
    sendQuotaThresholdEvent = False
    
    # Create region-specific boto3 clients
    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)
    
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




def L_FE5A380F(serviceCode, quotaCode, threshold, region):
    """
    Checks NAT gateways per Availability Zone
    :param serviceCode: The service code (vpc)
    :param quotaCode: The quota code (L-FE5A380F)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxNatGatewaysPerAZ = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"NAT gateways per AZ quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_nat_gateways.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                nat_gateways = json.load(test_file_content)
        else:
            paginator = ec2.get_paginator('describe_nat_gateways')
            nat_gateways = []
            for page in paginator.paginate(Filters=[{'Name': 'state', 'Values': ['available']}]):
                nat_gateways.extend(page['NatGateways'])

        # Count NAT gateways per AZ
        az_counts = defaultdict(int)
        for ngw in nat_gateways:
            subnet_id = ngw['SubnetId']
            if is_testing_enabled:
                az = ngw.get('AvailabilityZone', 'unknown')
            else:
                subnet_info = ec2.describe_subnets(SubnetIds=[subnet_id])['Subnets']
                az = subnet_info[0]['AvailabilityZone'] if subnet_info else 'unknown'
            az_counts[az] += 1

        for az, count in az_counts.items():
            logger.info(f"AZ {az}: {count} NAT gateways out of {serviceQuotaValue}")
            if maxNatGatewaysPerAZ < count:
                maxNatGatewaysPerAZ = count
                logger.info(f"Max value={maxNatGatewaysPerAZ}")
            if count / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                data = {"resourceARN": az, "usageValue": count}
                resourceListCrossingThreshold.append(data)
                logger.warning(f"AZ {az} NAT gateway count ({count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxNatGatewaysPerAZ), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking NAT gateways per AZ quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_83CA0A9D(serviceCode, quotaCode, threshold, region):
    """
    Checks IPv4 CIDR blocks per VPC
    :param serviceCode: The service code (vpc)
    :param quotaCode: The quota code (L-83CA0A9D)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxCidrBlocksPerVPC = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"IPv4 CIDR blocks per VPC quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_vpcs.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                test_vpcs = json.load(test_file_content)
            page_iterator = test_vpcs
        else:
            paginator = ec2.get_paginator('describe_vpcs')
            page_iterator = paginator.paginate()

        for vpcs in page_iterator:
            for vpc in vpcs['Vpcs']:
                vpc_id = vpc['VpcId']
                # Count IPv4 CIDR block associations
                cidr_count = len(vpc.get('CidrBlockAssociationSet', []))
                logger.info(f"VPC {vpc_id}: {cidr_count} IPv4 CIDR blocks out of {serviceQuotaValue}")

                if maxCidrBlocksPerVPC < cidr_count:
                    maxCidrBlocksPerVPC = cidr_count
                    logger.info(f"Max value={maxCidrBlocksPerVPC}")
                if cidr_count / serviceQuotaValue > float(threshold) / 100:
                    sendQuotaThresholdEvent = True
                    data = {"resourceARN": vpc_id, "usageValue": cidr_count}
                    resourceListCrossingThreshold.append(data)
                    logger.warning(f"VPC {vpc_id} CIDR block count ({cidr_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxCidrBlocksPerVPC), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking IPv4 CIDR blocks per VPC quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_93826ACB(serviceCode, quotaCode, threshold, region):
    """
    Checks Routes per route table
    :param serviceCode: The service code (vpc)
    :param quotaCode: The quota code (L-93826ACB)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxRoutesPerRouteTable = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Routes per route table quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_route_tables.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                page_iterator = json.load(test_file_content)
        else:
            paginator = ec2.get_paginator('describe_route_tables')
            page_iterator = paginator.paginate()

        for response in page_iterator:
            for rt in response['RouteTables']:
                rt_id = rt['RouteTableId']
                route_count = len(rt.get('Routes', []))
                logger.info(f"Route Table {rt_id}: {route_count} routes out of {serviceQuotaValue}")

                if maxRoutesPerRouteTable < route_count:
                    maxRoutesPerRouteTable = route_count
                    logger.info(f"Max value={maxRoutesPerRouteTable}")
                if route_count / serviceQuotaValue > float(threshold) / 100:
                    sendQuotaThresholdEvent = True
                    data = {"resourceARN": rt_id, "usageValue": route_count}
                    resourceListCrossingThreshold.append(data)
                    logger.warning(f"Route Table {rt_id} route count ({route_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxRoutesPerRouteTable), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking routes per route table quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_0EA8095F(serviceCode, quotaCode, threshold, region):
    """
    Checks Inbound or outbound rules per security group
    :param serviceCode: The service code (vpc)
    :param quotaCode: The quota code (L-0EA8095F)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxRulesPerSG = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Inbound or outbound rules per security group quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_security_groups.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                page_iterator = json.load(test_file_content)
        else:
            paginator = ec2.get_paginator('describe_security_groups')
            page_iterator = paginator.paginate()

        for response in page_iterator:
            for sg in response['SecurityGroups']:
                sg_id = sg['GroupId']
                inbound_count = len(sg.get('IpPermissions', []))
                outbound_count = len(sg.get('IpPermissionsEgress', []))
                max_direction_count = max(inbound_count, outbound_count)
                logger.info(f"Security Group {sg_id}: inbound={inbound_count}, outbound={outbound_count}")

                if maxRulesPerSG < max_direction_count:
                    maxRulesPerSG = max_direction_count
                    logger.info(f"Max value={maxRulesPerSG}")
                if max_direction_count / serviceQuotaValue > float(threshold) / 100:
                    sendQuotaThresholdEvent = True
                    data = {"resourceARN": sg_id, "usageValue": max_direction_count}
                    resourceListCrossingThreshold.append(data)
                    logger.warning(f"Security Group {sg_id} rule count ({max_direction_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxRulesPerSG), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking rules per security group quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_2AEEBF1A(serviceCode, quotaCode, threshold, region):
    """
    Checks Rules per network ACL
    :param serviceCode: The service code (vpc)
    :param quotaCode: The quota code (L-2AEEBF1A)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxRulesPerNACL = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Rules per network ACL quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_network_acls.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                page_iterator = json.load(test_file_content)
        else:
            paginator = ec2.get_paginator('describe_network_acls')
            page_iterator = paginator.paginate()

        for response in page_iterator:
            for nacl in response['NetworkAcls']:
                nacl_id = nacl['NetworkAclId']
                entries = nacl.get('Entries', [])
                # Count inbound and outbound rules separately (excluding default deny-all rules)
                inbound_count = len([e for e in entries if not e['Egress'] and e['RuleNumber'] != 32767])
                outbound_count = len([e for e in entries if e['Egress'] and e['RuleNumber'] != 32767])
                max_direction_count = max(inbound_count, outbound_count)
                logger.info(f"Network ACL {nacl_id}: inbound={inbound_count}, outbound={outbound_count}")

                if maxRulesPerNACL < max_direction_count:
                    maxRulesPerNACL = max_direction_count
                    logger.info(f"Max value={maxRulesPerNACL}")
                if max_direction_count / serviceQuotaValue > float(threshold) / 100:
                    sendQuotaThresholdEvent = True
                    data = {"resourceARN": nacl_id, "usageValue": max_direction_count}
                    resourceListCrossingThreshold.append(data)
                    logger.warning(f"Network ACL {nacl_id} rule count ({max_direction_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxRulesPerNACL), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking rules per network ACL quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_D0B7243C(serviceCode, quotaCode, threshold, region):
    """
    Checks New Reserved Instances per month
    :param serviceCode: The service code (ec2)
    :param quotaCode: The quota code (L-D0B7243C)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False
    totalNewRIsThisMonth = 0

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"New Reserved Instances per month quota: {serviceQuotaValue}")

        # Calculate start of current month
        now = datetime.utcnow()
        start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_reserved_instances.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                reserved_instances = json.load(test_file_content)
        else:
            # Describe reserved instances purchased this month
            response = ec2.describe_reserved_instances(
                Filters=[
                    {
                        'Name': 'state',
                        'Values': ['active', 'payment-pending']
                    }
                ]
            )
            reserved_instances = response['ReservedInstances']

        # Count RIs purchased this month
        for ri in reserved_instances:
            start_time = ri['Start'] if isinstance(ri['Start'], datetime) else datetime.fromisoformat(str(ri['Start']).replace('Z', '+00:00')).replace(tzinfo=None)
            if start_time >= start_of_month:
                totalNewRIsThisMonth += ri.get('InstanceCount', 1)

        logger.info(f"New Reserved Instances this month: {totalNewRIsThisMonth} out of {serviceQuotaValue}")

        if totalNewRIsThisMonth / serviceQuotaValue > float(threshold) / 100:
            logger.info(f"Exceeding Threshold for New Reserved Instances per month={totalNewRIsThisMonth}")
            sendQuotaThresholdEvent = True

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(totalNewRIsThisMonth), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking New Reserved Instances per month quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_C673935A(serviceCode, quotaCode, threshold, region):
    """
    Checks Multicast Network Interfaces per transit gateway
    :param serviceCode: The service code (ec2)
    :param quotaCode: The quota code (L-C673935A)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxMulticastNIsPerTgw = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Multicast Network Interfaces per transit gateway quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_transit_gateways.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                page_iterator = json.load(test_file_content)
        else:
            paginator = ec2.get_paginator('describe_transit_gateways')
            page_iterator = paginator.paginate()

        for response in page_iterator:
            for tgw in response['TransitGateways']:
                tgw_id = tgw['TransitGatewayId']

                if is_testing_enabled:
                    test_filename_mc = f'tests/{inspect.stack()[0][3]}_describe_transit_gateway_multicast_domains.json'
                    logger.info(f"Detected testing enabled. Using test payload from {test_filename_mc}")
                    with open(test_filename_mc, 'r') as test_file_content:
                        multicast_domains = json.load(test_file_content)
                else:
                    multicast_domains_response = ec2.describe_transit_gateway_multicast_domains(
                        Filters=[{'Name': 'transit-gateway-id', 'Values': [tgw_id]}]
                    )
                    multicast_domains = multicast_domains_response['TransitGatewayMulticastDomains']

                # Count unique network interfaces across all multicast domains for this TGW
                multicast_nis = set()
                for domain in multicast_domains:
                    domain_id = domain['TransitGatewayMulticastDomainId']
                    if is_testing_enabled:
                        test_filename_members = f'tests/{inspect.stack()[0][3]}_search_transit_gateway_multicast_groups.json'
                        logger.info(f"Detected testing enabled. Using test payload from {test_filename_members}")
                        with open(test_filename_members, 'r') as test_file_content:
                            members = json.load(test_file_content)
                    else:
                        members_response = ec2.search_transit_gateway_multicast_groups(
                            TransitGatewayMulticastDomainId=domain_id
                        )
                        members = members_response.get('MulticastGroups', [])
                    for member in members:
                        if 'NetworkInterfaceId' in member:
                            multicast_nis.add(member['NetworkInterfaceId'])

                ni_count = len(multicast_nis)
                logger.info(f"TGW {tgw_id}: {ni_count} multicast network interfaces out of {serviceQuotaValue}")

                if maxMulticastNIsPerTgw < ni_count:
                    maxMulticastNIsPerTgw = ni_count
                    logger.info(f"Max value={maxMulticastNIsPerTgw}")
                if ni_count / serviceQuotaValue > float(threshold) / 100:
                    sendQuotaThresholdEvent = True
                    data = {"resourceARN": tgw_id, "usageValue": ni_count}
                    resourceListCrossingThreshold.append(data)
                    logger.warning(f"TGW {tgw_id} multicast NI count ({ni_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxMulticastNIsPerTgw), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Multicast Network Interfaces per TGW quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_59C8FC87(serviceCode, quotaCode, threshold, region):
    """
    Checks Storage modifications for General Purpose SSD (gp3) volumes, in TiB
    :param serviceCode: The service code (ebs)
    :param quotaCode: The quota code (L-59C8FC87)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False
    totalGP3StorageTiB = 0

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Storage modifications for gp3 volumes quota (TiB): {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_volumes_modifications.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                page_iterator = json.load(test_file_content)
        else:
            paginator = ec2.get_paginator('describe_volumes_modifications')
            page_iterator = paginator.paginate(
                Filters=[
                    {'Name': 'modification-state', 'Values': ['modifying', 'optimizing', 'completed']},
                    {'Name': 'volume-type', 'Values': ['gp3']}  # Note: This filter may not be available; fallback below
                ]
            )

        for response in page_iterator:
            for mod in response['VolumesModifications']:
                # Sum the target size of gp3 modifications
                totalGP3StorageTiB += mod.get('TargetSize', 0)

        # Convert GiB to TiB
        totalGP3StorageTiB /= 1024
        logger.info(f"Total gp3 storage modifications (TiB): {totalGP3StorageTiB}")

        if totalGP3StorageTiB / serviceQuotaValue > float(threshold) / 100:
            logger.info(f"Exceeding Threshold for gp3 storage modifications={totalGP3StorageTiB}")
            sendQuotaThresholdEvent = True

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(totalGP3StorageTiB), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking gp3 storage modifications quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_F786B2E5(serviceCode, quotaCode, threshold, region):
    """
    Checks Classic Load Balancers per Auto Scaling group
    :param serviceCode: The service code (autoscaling)
    :param quotaCode: The quota code (L-F786B2E5)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxCLBsPerASG = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    asg_client = boto3.client('autoscaling', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Classic Load Balancers per ASG quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_auto_scaling_groups.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                page_iterator = json.load(test_file_content)
        else:
            paginator = asg_client.get_paginator('describe_auto_scaling_groups')
            page_iterator = paginator.paginate()

        for response in page_iterator:
            for asg in response['AutoScalingGroups']:
                asg_name = asg['AutoScalingGroupName']
                clb_count = len(asg.get('LoadBalancerNames', []))
                logger.info(f"ASG {asg_name}: {clb_count} Classic Load Balancers out of {serviceQuotaValue}")

                if maxCLBsPerASG < clb_count:
                    maxCLBsPerASG = clb_count
                    logger.info(f"Max value={maxCLBsPerASG}")
                if clb_count / serviceQuotaValue > float(threshold) / 100:
                    sendQuotaThresholdEvent = True
                    data = {"resourceARN": asg['AutoScalingGroupARN'], "usageValue": clb_count}
                    resourceListCrossingThreshold.append(data)
                    logger.warning(f"ASG {asg_name} CLB count ({clb_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxCLBsPerASG), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Classic Load Balancers per ASG quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_F0B00D71(serviceCode, quotaCode, threshold, region):
    """
    Checks Scheduled actions per Auto Scaling group
    :param serviceCode: The service code (autoscaling)
    :param quotaCode: The quota code (L-F0B00D71)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxScheduledActionsPerASG = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    asg_client = boto3.client('autoscaling', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Scheduled actions per ASG quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_auto_scaling_groups.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                page_iterator = json.load(test_file_content)
        else:
            paginator = asg_client.get_paginator('describe_auto_scaling_groups')
            page_iterator = paginator.paginate()

        for response in page_iterator:
            for asg in response['AutoScalingGroups']:
                asg_name = asg['AutoScalingGroupName']

                if is_testing_enabled:
                    test_filename_sa = f'tests/{inspect.stack()[0][3]}_describe_scheduled_actions.json'
                    logger.info(f"Detected testing enabled. Using test payload from {test_filename_sa}")
                    with open(test_filename_sa, 'r') as test_file_content:
                        scheduled_actions = json.load(test_file_content).get('ScheduledUpdateGroupActions', [])
                else:
                    sa_response = asg_client.describe_scheduled_actions(AutoScalingGroupName=asg_name)
                    scheduled_actions = sa_response.get('ScheduledUpdateGroupActions', [])

                sa_count = len(scheduled_actions)
                logger.info(f"ASG {asg_name}: {sa_count} scheduled actions out of {serviceQuotaValue}")

                if maxScheduledActionsPerASG < sa_count:
                    maxScheduledActionsPerASG = sa_count
                    logger.info(f"Max value={maxScheduledActionsPerASG}")
                if sa_count / serviceQuotaValue > float(threshold) / 100:
                    sendQuotaThresholdEvent = True
                    data = {"resourceARN": asg['AutoScalingGroupARN'], "usageValue": sa_count}
                    resourceListCrossingThreshold.append(data)
                    logger.warning(f"ASG {asg_name} scheduled action count ({sa_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxScheduledActionsPerASG), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Scheduled actions per ASG quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_72753F6F(serviceCode, quotaCode, threshold, region):
    """
    Checks Scaling policies per Auto Scaling group
    :param serviceCode: The service code (autoscaling)
    :param quotaCode: The quota code (L-72753F6F)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxScalingPoliciesPerASG = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    asg_client = boto3.client('autoscaling', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Scaling policies per ASG quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_auto_scaling_groups.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                page_iterator = json.load(test_file_content)
        else:
            paginator = asg_client.get_paginator('describe_auto_scaling_groups')
            page_iterator = paginator.paginate()

        for response in page_iterator:
            for asg in response['AutoScalingGroups']:
                asg_name = asg['AutoScalingGroupName']

                if is_testing_enabled:
                    test_filename_pol = f'tests/{inspect.stack()[0][3]}_describe_policies.json'
                    logger.info(f"Detected testing enabled. Using test payload from {test_filename_pol}")
                    with open(test_filename_pol, 'r') as test_file_content:
                        policies = json.load(test_file_content).get('ScalingPolicies', [])
                else:
                    pol_response = asg_client.describe_policies(AutoScalingGroupName=asg_name)
                    policies = pol_response.get('ScalingPolicies', [])

                pol_count = len(policies)
                logger.info(f"ASG {asg_name}: {pol_count} scaling policies out of {serviceQuotaValue}")

                if maxScalingPoliciesPerASG < pol_count:
                    maxScalingPoliciesPerASG = pol_count
                    logger.info(f"Max value={maxScalingPoliciesPerASG}")
                if pol_count / serviceQuotaValue > float(threshold) / 100:
                    sendQuotaThresholdEvent = True
                    data = {"resourceARN": asg['AutoScalingGroupARN'], "usageValue": pol_count}
                    resourceListCrossingThreshold.append(data)
                    logger.warning(f"ASG {asg_name} scaling policy count ({pol_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxScalingPoliciesPerASG), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Scaling policies per ASG quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_CEE5E714(serviceCode, quotaCode, threshold, region):
    """
    Checks SNS topics per Auto Scaling group
    :param serviceCode: The service code (autoscaling)
    :param quotaCode: The quota code (L-CEE5E714)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxSNSTopicsPerASG = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    asg_client = boto3.client('autoscaling', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"SNS topics per ASG quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_auto_scaling_groups.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                page_iterator = json.load(test_file_content)
        else:
            paginator = asg_client.get_paginator('describe_auto_scaling_groups')
            page_iterator = paginator.paginate()

        for response in page_iterator:
            for asg in response['AutoScalingGroups']:
                asg_name = asg['AutoScalingGroupName']

                if is_testing_enabled:
                    test_filename_nc = f'tests/{inspect.stack()[0][3]}_describe_notification_configurations.json'
                    logger.info(f"Detected testing enabled. Using test payload from {test_filename_nc}")
                    with open(test_filename_nc, 'r') as test_file_content:
                        notifications = json.load(test_file_content).get('NotificationConfigurations', [])
                else:
                    nc_response = asg_client.describe_notification_configurations(
                        AutoScalingGroupNames=[asg_name]
                    )
                    notifications = nc_response.get('NotificationConfigurations', [])

                # Count unique SNS topic ARNs
                unique_topics = set()
                for notif in notifications:
                    unique_topics.add(notif['TopicARN'])
                topic_count = len(unique_topics)
                logger.info(f"ASG {asg_name}: {topic_count} SNS topics out of {serviceQuotaValue}")

                if maxSNSTopicsPerASG < topic_count:
                    maxSNSTopicsPerASG = topic_count
                    logger.info(f"Max value={maxSNSTopicsPerASG}")
                if topic_count / serviceQuotaValue > float(threshold) / 100:
                    sendQuotaThresholdEvent = True
                    data = {"resourceARN": asg['AutoScalingGroupARN'], "usageValue": topic_count}
                    resourceListCrossingThreshold.append(data)
                    logger.warning(f"ASG {asg_name} SNS topic count ({topic_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxSNSTopicsPerASG), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking SNS topics per ASG quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_1312BBBF(serviceCode, quotaCode, threshold, region):
    """
    Checks Lifecycle hooks per Auto Scaling group
    :param serviceCode: The service code (autoscaling)
    :param quotaCode: The quota code (L-1312BBBF)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxLifecycleHooksPerASG = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    asg_client = boto3.client('autoscaling', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Lifecycle hooks per ASG quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_auto_scaling_groups.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                page_iterator = json.load(test_file_content)
        else:
            paginator = asg_client.get_paginator('describe_auto_scaling_groups')
            page_iterator = paginator.paginate()

        for response in page_iterator:
            for asg in response['AutoScalingGroups']:
                asg_name = asg['AutoScalingGroupName']

                if is_testing_enabled:
                    test_filename_lh = f'tests/{inspect.stack()[0][3]}_describe_lifecycle_hooks.json'
                    logger.info(f"Detected testing enabled. Using test payload from {test_filename_lh}")
                    with open(test_filename_lh, 'r') as test_file_content:
                        hooks = json.load(test_file_content).get('LifecycleHooks', [])
                else:
                    lh_response = asg_client.describe_lifecycle_hooks(AutoScalingGroupName=asg_name)
                    hooks = lh_response.get('LifecycleHooks', [])

                hook_count = len(hooks)
                logger.info(f"ASG {asg_name}: {hook_count} lifecycle hooks out of {serviceQuotaValue}")

                if maxLifecycleHooksPerASG < hook_count:
                    maxLifecycleHooksPerASG = hook_count
                    logger.info(f"Max value={maxLifecycleHooksPerASG}")
                if hook_count / serviceQuotaValue > float(threshold) / 100:
                    sendQuotaThresholdEvent = True
                    data = {"resourceARN": asg['AutoScalingGroupARN'], "usageValue": hook_count}
                    resourceListCrossingThreshold.append(data)
                    logger.warning(f"ASG {asg_name} lifecycle hook count ({hook_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxLifecycleHooksPerASG), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Lifecycle hooks per ASG quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_05CB8B12(serviceCode, quotaCode, threshold, region):
    """
    Checks Target groups per Auto Scaling group
    :param serviceCode: The service code (autoscaling)
    :param quotaCode: The quota code (L-05CB8B12)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxTargetGroupsPerASG = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    asg_client = boto3.client('autoscaling', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Target groups per ASG quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_auto_scaling_groups.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                page_iterator = json.load(test_file_content)
        else:
            paginator = asg_client.get_paginator('describe_auto_scaling_groups')
            page_iterator = paginator.paginate()

        for response in page_iterator:
            for asg in response['AutoScalingGroups']:
                asg_name = asg['AutoScalingGroupName']
                tg_count = len(asg.get('TargetGroupARNs', []))
                logger.info(f"ASG {asg_name}: {tg_count} target groups out of {serviceQuotaValue}")

                if maxTargetGroupsPerASG < tg_count:
                    maxTargetGroupsPerASG = tg_count
                    logger.info(f"Max value={maxTargetGroupsPerASG}")
                if tg_count / serviceQuotaValue > float(threshold) / 100:
                    sendQuotaThresholdEvent = True
                    data = {"resourceARN": asg['AutoScalingGroupARN'], "usageValue": tg_count}
                    resourceListCrossingThreshold.append(data)
                    logger.warning(f"ASG {asg_name} target group count ({tg_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxTargetGroupsPerASG), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Target groups per ASG quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_6C2A2F6E(serviceCode, quotaCode, threshold, region):
    """
    Checks Step adjustments per step scaling policy
    :param serviceCode: The service code (autoscaling)
    :param quotaCode: The quota code (L-6C2A2F6E)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxStepAdjustmentsPerPolicy = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    asg_client = boto3.client('autoscaling', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Step adjustments per step scaling policy quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_auto_scaling_groups.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                page_iterator = json.load(test_file_content)
        else:
            paginator = asg_client.get_paginator('describe_auto_scaling_groups')
            page_iterator = paginator.paginate()

        for response in page_iterator:
            for asg in response['AutoScalingGroups']:
                asg_name = asg['AutoScalingGroupName']

                if is_testing_enabled:
                    test_filename_pol = f'tests/{inspect.stack()[0][3]}_describe_policies.json'
                    logger.info(f"Detected testing enabled. Using test payload from {test_filename_pol}")
                    with open(test_filename_pol, 'r') as test_file_content:
                        policies = json.load(test_file_content).get('ScalingPolicies', [])
                else:
                    pol_response = asg_client.describe_policies(AutoScalingGroupName=asg_name)
                    policies = pol_response.get('ScalingPolicies', [])

                for policy in policies:
                    if policy.get('PolicyType') == 'StepScaling':
                        policy_name = policy['PolicyName']
                        step_count = len(policy.get('StepAdjustments', []))
                        logger.info(f"ASG {asg_name} Policy {policy_name}: {step_count} step adjustments out of {serviceQuotaValue}")

                        if maxStepAdjustmentsPerPolicy < step_count:
                            maxStepAdjustmentsPerPolicy = step_count
                            logger.info(f"Max value={maxStepAdjustmentsPerPolicy}")
                        if step_count / serviceQuotaValue > float(threshold) / 100:
                            sendQuotaThresholdEvent = True
                            data = {"resourceARN": policy.get('PolicyARN', policy_name), "usageValue": step_count}
                            resourceListCrossingThreshold.append(data)
                            logger.warning(f"Policy {policy_name} step adjustment count ({step_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxStepAdjustmentsPerPolicy), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Step adjustments per step scaling policy quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


# ============================================================
# EBS Concurrent Snapshot Quotas
# ============================================================

def _ebs_concurrent_snapshots_by_volume_type(serviceCode, quotaCode, threshold, region, volume_type, quota_name):
    """
    Helper: Checks concurrent snapshots per volume type.
    Counts in-progress (pending) snapshots for volumes of the given type.
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxConcurrentSnapshots = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"{quota_name} quota: {serviceQuotaValue}")

        # Get all volumes of the specified type
        if is_testing_enabled:
            test_filename = f'tests/{quotaCode}_describe_volumes.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                volumes = json.load(test_file_content)
        else:
            paginator = ec2.get_paginator('describe_volumes')
            volumes = []
            for page in paginator.paginate(Filters=[{'Name': 'volume-type', 'Values': [volume_type]}]):
                volumes.extend(page['Volumes'])

        # For each volume, count pending snapshots
        for vol in volumes:
            vol_id = vol['VolumeId']
            if is_testing_enabled:
                test_filename = f'tests/{quotaCode}_describe_snapshots.json'
                logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
                with open(test_filename, 'r') as test_file_content:
                    snapshots = json.load(test_file_content)
            else:
                snap_response = ec2.describe_snapshots(
                    Filters=[
                        {'Name': 'volume-id', 'Values': [vol_id]},
                        {'Name': 'status', 'Values': ['pending']}
                    ],
                    OwnerIds=['self']
                )
                snapshots = snap_response['Snapshots']

            pending_count = len(snapshots)
            logger.info(f"Volume {vol_id} ({volume_type}): {pending_count} pending snapshots")

            if maxConcurrentSnapshots < pending_count:
                maxConcurrentSnapshots = pending_count
                logger.info(f"Max value={maxConcurrentSnapshots}")

            if pending_count / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                data = {"resourceARN": vol_id, "usageValue": pending_count}
                resourceListCrossingThreshold.append(data)
                logger.warning(f"Volume {vol_id} pending snapshot count ({pending_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxConcurrentSnapshots), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking {quota_name}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_835364B2(serviceCode, quotaCode, threshold, region):
    """
    Concurrent snapshots per General Purpose SSD (gp2) volume
    """
    _ebs_concurrent_snapshots_by_volume_type(serviceCode, quotaCode, threshold, region, 'gp2', 'Concurrent snapshots per gp2 volume')


def L_DB70D580(serviceCode, quotaCode, threshold, region):
    """
    Concurrent snapshots per Provisioned IOPS SSD (io1) volume
    """
    _ebs_concurrent_snapshots_by_volume_type(serviceCode, quotaCode, threshold, region, 'io1', 'Concurrent snapshots per io1 volume')


def L_D0291BE3(serviceCode, quotaCode, threshold, region):
    """
    Concurrent snapshots per Provisioned IOPS SSD (io2) volume
    """
    _ebs_concurrent_snapshots_by_volume_type(serviceCode, quotaCode, threshold, region, 'io2', 'Concurrent snapshots per io2 volume')


def L_9F6E7C4E(serviceCode, quotaCode, threshold, region):
    """
    Concurrent snapshots per Throughput Optimized HDD (st1) volume
    """
    _ebs_concurrent_snapshots_by_volume_type(serviceCode, quotaCode, threshold, region, 'st1', 'Concurrent snapshots per st1 volume')


def L_915A3DBB(serviceCode, quotaCode, threshold, region):
    """
    Concurrent snapshots per Cold HDD (sc1) volume
    """
    _ebs_concurrent_snapshots_by_volume_type(serviceCode, quotaCode, threshold, region, 'sc1', 'Concurrent snapshots per sc1 volume')


def L_D8F37C68(serviceCode, quotaCode, threshold, region):
    """
    Concurrent snapshots per General Purpose SSD (gp3) volume
    """
    _ebs_concurrent_snapshots_by_volume_type(serviceCode, quotaCode, threshold, region, 'gp3', 'Concurrent snapshots per gp3 volume')


def L_750405C3(serviceCode, quotaCode, threshold, region):
    """
    Concurrent snapshots per Magnetic (standard) volume
    """
    _ebs_concurrent_snapshots_by_volume_type(serviceCode, quotaCode, threshold, region, 'standard', 'Concurrent snapshots per standard volume')


def L_8656991D(serviceCode, quotaCode, threshold, region):
    """
    Concurrent snapshot copies per destination Region
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Concurrent snapshot copies per destination Region quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_snapshots.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                snapshots = json.load(test_file_content)
        else:
            # Count all pending snapshots in this region (destination region for copies)
            paginator = ec2.get_paginator('describe_snapshots')
            snapshots = []
            for page in paginator.paginate(OwnerIds=['self'], Filters=[{'Name': 'status', 'Values': ['pending']}]):
                snapshots.extend(page['Snapshots'])

        pendingCount = len(snapshots)
        logger.info(f"Concurrent pending snapshot copies in region {region}: {pendingCount}")

        if pendingCount / serviceQuotaValue > float(threshold) / 100:
            sendQuotaThresholdEvent = True
            logger.warning(f"Pending snapshot copies ({pendingCount}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(pendingCount), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking concurrent snapshot copies quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


# ============================================================
# EC2 Quotas
# ============================================================

def L_350B2172(serviceCode, quotaCode, threshold, region):
    """
    Direct Connect gateways per transit gateway
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxDxGwPerTgw = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    dx = boto3.client('directconnect', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Direct Connect gateways per transit gateway quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_transit_gateways.json'
            with open(test_filename, 'r') as f:
                page_iterator = json.load(f)
        else:
            paginator = ec2.get_paginator('describe_transit_gateways')
            page_iterator = paginator.paginate()

        for response in page_iterator:
            for tgw in response['TransitGateways']:
                tgw_id = tgw['TransitGatewayId']

                if is_testing_enabled:
                    test_filename = f'tests/{inspect.stack()[0][3]}_describe_direct_connect_gateway_attachments.json'
                    with open(test_filename, 'r') as f:
                        attachments = json.load(f)
                else:
                    attachments = dx.describe_direct_connect_gateway_attachments(
                        virtualInterfaceId=None
                    ).get('directConnectGatewayAttachments', [])
                    # Filter to attachments for this TGW
                    attachments = [a for a in attachments if a.get('virtualInterfaceOwnerAccount') and a.get('stateChangeError') is None]

                    # Use EC2 TGW attachments to find DX gateway associations
                    dx_attach_response = ec2.describe_transit_gateway_attachments(
                        Filters=[
                            {'Name': 'transit-gateway-id', 'Values': [tgw_id]},
                            {'Name': 'resource-type', 'Values': ['direct-connect-gateway']}
                        ]
                    )
                    attachments = dx_attach_response.get('TransitGatewayAttachments', [])

                dx_gw_count = len(attachments)
                logger.info(f"TGW {tgw_id}: {dx_gw_count} Direct Connect gateway attachments")

                if maxDxGwPerTgw < dx_gw_count:
                    maxDxGwPerTgw = dx_gw_count
                    logger.info(f"Max value={maxDxGwPerTgw}")

                if dx_gw_count / serviceQuotaValue > float(threshold) / 100:
                    sendQuotaThresholdEvent = True
                    data = {"resourceARN": tgw_id, "usageValue": dx_gw_count}
                    resourceListCrossingThreshold.append(data)
                    logger.warning(f"TGW {tgw_id} DX gateway count ({dx_gw_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxDxGwPerTgw), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Direct Connect gateways per TGW quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_862D9275(serviceCode, quotaCode, threshold, region):
    """
    Number of Elastic Graphics accelerators
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Elastic Graphics accelerators quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_elastic_gpus.json'
            with open(test_filename, 'r') as f:
                accelerators = json.load(f)
        else:
            response = ec2.describe_elastic_gpus()
            accelerators = response.get('ElasticGpuSet', [])

        totalAccelerators = len(accelerators)
        logger.info(f"Total Elastic Graphics accelerators: {totalAccelerators}")

        if totalAccelerators / serviceQuotaValue > float(threshold) / 100:
            sendQuotaThresholdEvent = True
            logger.warning(f"Elastic Graphics count ({totalAccelerators}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(totalAccelerators), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Elastic Graphics quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_6B192186(serviceCode, quotaCode, threshold, region):
    """
    Transit gateways per Direct Connect Gateway
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxTgwPerDxGw = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    dx = boto3.client('directconnect', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Transit gateways per Direct Connect Gateway quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_direct_connect_gateways.json'
            with open(test_filename, 'r') as f:
                dx_gateways = json.load(f)
        else:
            dx_gateways = dx.describe_direct_connect_gateways().get('directConnectGateways', [])

        for dx_gw in dx_gateways:
            dx_gw_id = dx_gw['directConnectGatewayId']

            if is_testing_enabled:
                test_filename = f'tests/{inspect.stack()[0][3]}_describe_direct_connect_gateway_associations.json'
                with open(test_filename, 'r') as f:
                    associations = json.load(f)
            else:
                associations = dx.describe_direct_connect_gateway_associations(
                    directConnectGatewayId=dx_gw_id
                ).get('directConnectGatewayAssociations', [])
                # Filter to transit gateway associations only
                associations = [a for a in associations if a.get('associatedGateway', {}).get('type') == 'transitGateway']

            tgw_count = len(associations)
            logger.info(f"DX Gateway {dx_gw_id}: {tgw_count} transit gateway associations")

            if maxTgwPerDxGw < tgw_count:
                maxTgwPerDxGw = tgw_count
                logger.info(f"Max value={maxTgwPerDxGw}")

            if tgw_count / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                data = {"resourceARN": dx_gw_id, "usageValue": tgw_count}
                resourceListCrossingThreshold.append(data)
                logger.warning(f"DX Gateway {dx_gw_id} TGW count ({tgw_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxTgwPerDxGw), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Transit gateways per DX Gateway quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_3829BC77(serviceCode, quotaCode, threshold, region):
    """
    Verified Access Groups
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Verified Access Groups quota: {serviceQuotaValue}")

        totalGroups = 0
        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_verified_access_groups.json'
            with open(test_filename, 'r') as f:
                page_iterator = json.load(f)
        else:
            paginator = ec2.get_paginator('describe_verified_access_groups')
            page_iterator = paginator.paginate()

        for response in page_iterator:
            totalGroups += len(response.get('VerifiedAccessGroups', []))

        logger.info(f"Total Verified Access Groups: {totalGroups}")

        if totalGroups / serviceQuotaValue > float(threshold) / 100:
            sendQuotaThresholdEvent = True
            logger.warning(f"Verified Access Groups ({totalGroups}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(totalGroups), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Verified Access Groups quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_8FBBDF0C(serviceCode, quotaCode, threshold, region):
    """
    Amazon FPGA images (AFIs)
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Amazon FPGA images (AFIs) quota: {serviceQuotaValue}")

        totalAFIs = 0
        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_fpga_images.json'
            with open(test_filename, 'r') as f:
                page_iterator = json.load(f)
        else:
            paginator = ec2.get_paginator('describe_fpga_images')
            page_iterator = paginator.paginate(Owners=['self'])

        for response in page_iterator:
            totalAFIs += len(response.get('FpgaImages', []))

        logger.info(f"Total FPGA images: {totalAFIs}")

        if totalAFIs / serviceQuotaValue > float(threshold) / 100:
            sendQuotaThresholdEvent = True
            logger.warning(f"FPGA image count ({totalAFIs}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(totalAFIs), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking FPGA images quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_92B73F21(serviceCode, quotaCode, threshold, region):
    """
    Dynamic routes advertised from CGW to VPN connection
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxRoutes = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Dynamic routes advertised from CGW to VPN connection quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_vpn_connections.json'
            with open(test_filename, 'r') as f:
                vpn_connections = json.load(f)
        else:
            vpn_response = ec2.describe_vpn_connections(
                Filters=[{'Name': 'state', 'Values': ['available']}]
            )
            vpn_connections = vpn_response.get('VpnConnections', [])

        for vpn in vpn_connections:
            vpn_id = vpn['VpnConnectionId']
            # Routes from CGW are in the VGW telemetry or BGP routes
            routes = vpn.get('Routes', [])
            route_count = len(routes)
            logger.info(f"VPN {vpn_id}: {route_count} routes from CGW")

            if maxRoutes < route_count:
                maxRoutes = route_count
                logger.info(f"Max value={maxRoutes}")

            if route_count / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                data = {"resourceARN": vpn_id, "usageValue": route_count}
                resourceListCrossingThreshold.append(data)
                logger.warning(f"VPN {vpn_id} route count ({route_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxRoutes), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking dynamic routes from CGW quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_DB0BBC4E(serviceCode, quotaCode, threshold, region):
    """
    Routes advertised from VPN connection to CGW
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxRoutes = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Routes advertised from VPN connection to CGW quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_vpn_connections.json'
            with open(test_filename, 'r') as f:
                vpn_connections = json.load(f)
        else:
            vpn_response = ec2.describe_vpn_connections(
                Filters=[{'Name': 'state', 'Values': ['available']}]
            )
            vpn_connections = vpn_response.get('VpnConnections', [])

        for vpn in vpn_connections:
            vpn_id = vpn['VpnConnectionId']
            routes = vpn.get('Routes', [])
            route_count = len(routes)
            logger.info(f"VPN {vpn_id}: {route_count} routes advertised to CGW")

            if maxRoutes < route_count:
                maxRoutes = route_count
                logger.info(f"Max value={maxRoutes}")

            if route_count / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                data = {"resourceARN": vpn_id, "usageValue": route_count}
                resourceListCrossingThreshold.append(data)
                logger.warning(f"VPN {vpn_id} route count ({route_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxRoutes), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking routes advertised to CGW quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_AF309E5E(serviceCode, quotaCode, threshold, region):
    """
    Verified Access Trust Providers
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Verified Access Trust Providers quota: {serviceQuotaValue}")

        totalProviders = 0
        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_verified_access_trust_providers.json'
            with open(test_filename, 'r') as f:
                page_iterator = json.load(f)
        else:
            paginator = ec2.get_paginator('describe_verified_access_trust_providers')
            page_iterator = paginator.paginate()

        for response in page_iterator:
            totalProviders += len(response.get('VerifiedAccessTrustProviders', []))

        logger.info(f"Total Verified Access Trust Providers: {totalProviders}")

        if totalProviders / serviceQuotaValue > float(threshold) / 100:
            sendQuotaThresholdEvent = True
            logger.warning(f"Verified Access Trust Providers ({totalProviders}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(totalProviders), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Verified Access Trust Providers quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_D92B9F5B(serviceCode, quotaCode, threshold, region):
    """
    VPC Attachment Bandwidth (per transit gateway VPC attachment)
    This is a per-attachment bandwidth limit. We count TGW VPC attachments.
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"VPC Attachment Bandwidth quota: {serviceQuotaValue}")

        totalVpcAttachments = 0
        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_transit_gateway_vpc_attachments.json'
            with open(test_filename, 'r') as f:
                page_iterator = json.load(f)
        else:
            paginator = ec2.get_paginator('describe_transit_gateway_vpc_attachments')
            page_iterator = paginator.paginate()

        for response in page_iterator:
            totalVpcAttachments += len(response.get('TransitGatewayVpcAttachments', []))

        logger.info(f"Total TGW VPC attachments: {totalVpcAttachments}")

        # This quota is bandwidth per attachment, so we report the count of attachments
        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(totalVpcAttachments), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking VPC Attachment Bandwidth quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_5D439CF7(serviceCode, quotaCode, threshold, region):
    """
    Verified Access Endpoints
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Verified Access Endpoints quota: {serviceQuotaValue}")

        totalEndpoints = 0
        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_verified_access_endpoints.json'
            with open(test_filename, 'r') as f:
                page_iterator = json.load(f)
        else:
            paginator = ec2.get_paginator('describe_verified_access_endpoints')
            page_iterator = paginator.paginate()

        for response in page_iterator:
            totalEndpoints += len(response.get('VerifiedAccessEndpoints', []))

        logger.info(f"Total Verified Access Endpoints: {totalEndpoints}")

        if totalEndpoints / serviceQuotaValue > float(threshold) / 100:
            sendQuotaThresholdEvent = True
            logger.warning(f"Verified Access Endpoints ({totalEndpoints}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(totalEndpoints), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Verified Access Endpoints quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_ED8A7771(serviceCode, quotaCode, threshold, region):
    """
    Concurrent operations per Client VPN endpoint
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxConcurrentOps = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Concurrent operations per Client VPN endpoint quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_client_vpn_endpoints.json'
            with open(test_filename, 'r') as f:
                endpoints = json.load(f)
        else:
            paginator = ec2.get_paginator('describe_client_vpn_endpoints')
            endpoints = []
            for page in paginator.paginate():
                endpoints.extend(page.get('ClientVpnEndpoints', []))

        for endpoint in endpoints:
            endpoint_id = endpoint['ClientVpnEndpointId']

            if is_testing_enabled:
                test_filename = f'tests/{inspect.stack()[0][3]}_describe_client_vpn_connections.json'
                with open(test_filename, 'r') as f:
                    connections = json.load(f)
            else:
                conn_response = ec2.describe_client_vpn_connections(
                    ClientVpnEndpointId=endpoint_id,
                    Filters=[{'Name': 'status', 'Values': ['active']}]
                )
                connections = conn_response.get('Connections', [])

            conn_count = len(connections)
            logger.info(f"Client VPN endpoint {endpoint_id}: {conn_count} active connections")

            if maxConcurrentOps < conn_count:
                maxConcurrentOps = conn_count
                logger.info(f"Max value={maxConcurrentOps}")

            if conn_count / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                data = {"resourceARN": endpoint_id, "usageValue": conn_count}
                resourceListCrossingThreshold.append(data)
                logger.warning(f"Client VPN {endpoint_id} connection count ({conn_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxConcurrentOps), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking concurrent operations per Client VPN quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_17A8BD20(serviceCode, quotaCode, threshold, region):
    """
    Verified Access Instances
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Verified Access Instances quota: {serviceQuotaValue}")

        totalInstances = 0
        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_verified_access_instances.json'
            with open(test_filename, 'r') as f:
                page_iterator = json.load(f)
        else:
            paginator = ec2.get_paginator('describe_verified_access_instances')
            page_iterator = paginator.paginate()

        for response in page_iterator:
            totalInstances += len(response.get('VerifiedAccessInstances', []))

        logger.info(f"Total Verified Access Instances: {totalInstances}")

        if totalInstances / serviceQuotaValue > float(threshold) / 100:
            sendQuotaThresholdEvent = True
            logger.warning(f"Verified Access Instances ({totalInstances}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(totalInstances), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Verified Access Instances quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_6AF8B990(serviceCode, quotaCode, threshold, region):
    """
    Entries in a client certificate revocation list for Client VPN endpoints
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxEntries = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Client certificate revocation list entries quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_client_vpn_endpoints.json'
            with open(test_filename, 'r') as f:
                endpoints = json.load(f)
        else:
            paginator = ec2.get_paginator('describe_client_vpn_endpoints')
            endpoints = []
            for page in paginator.paginate():
                endpoints.extend(page.get('ClientVpnEndpoints', []))

        for endpoint in endpoints:
            endpoint_id = endpoint['ClientVpnEndpointId']

            if is_testing_enabled:
                test_filename = f'tests/{inspect.stack()[0][3]}_export_client_vpn_client_certificate_revocation_list.json'
                with open(test_filename, 'r') as f:
                    crl_data = json.load(f)
            else:
                crl_response = ec2.export_client_vpn_client_certificate_revocation_list(
                    ClientVpnEndpointId=endpoint_id
                )
                crl_data = crl_response

            # The CRL is returned as a PEM string; count entries by parsing serial numbers
            crl_string = crl_data.get('CertificateRevocationList', '')
            # Approximate entry count by counting serial number lines in the CRL
            entry_count = crl_string.count('Serial Number:') if crl_string else 0
            logger.info(f"Client VPN {endpoint_id}: {entry_count} CRL entries")

            if maxEntries < entry_count:
                maxEntries = entry_count
                logger.info(f"Max value={maxEntries}")

            if entry_count / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                data = {"resourceARN": endpoint_id, "usageValue": entry_count}
                resourceListCrossingThreshold.append(data)
                logger.warning(f"Client VPN {endpoint_id} CRL entries ({entry_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxEntries), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking CRL entries quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_D060B150(serviceCode, quotaCode, threshold, region):
    """
    Checks Shards per cluster (Redis cluster mode disabled)
    For cluster mode disabled, each replication group has exactly one node group (shard).
    We report the max shard count across replication groups that have cluster mode disabled.
    :param serviceCode: The service code (elasticache)
    :param quotaCode: The quota code (L-D060B150)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxShardsPerCluster = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    elasticache = boto3.client('elasticache', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Shards per cluster (Redis cluster mode disabled) quota: {serviceQuotaValue}")

        paginator = elasticache.get_paginator('describe_replication_groups')
        for page in paginator.paginate():
            for rg in page['ReplicationGroups']:
                # Cluster mode disabled means ClusterEnabled is False
                if not rg.get('ClusterEnabled', False):
                    rg_id = rg['ReplicationGroupId']
                    shard_count = len(rg.get('NodeGroups', []))
                    logger.info(f"Replication Group {rg_id} (cluster mode disabled): {shard_count} shards")

                    if maxShardsPerCluster < shard_count:
                        maxShardsPerCluster = shard_count
                        logger.info(f"Max value={maxShardsPerCluster}")
                    if shard_count / serviceQuotaValue > float(threshold) / 100:
                        sendQuotaThresholdEvent = True
                        data = {"resourceARN": rg_id, "usageValue": shard_count}
                        resourceListCrossingThreshold.append(data)
                        logger.warning(f"Replication Group {rg_id} shard count ({shard_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxShardsPerCluster), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking shards per cluster (cluster mode disabled) quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_7D6587E6(serviceCode, quotaCode, threshold, region):
    """
    Checks Nodes per shard (Redis)
    Iterates replication groups, then node groups (shards), counting members per shard.
    :param serviceCode: The service code (elasticache)
    :param quotaCode: The quota code (L-7D6587E6)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxNodesPerShard = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    elasticache = boto3.client('elasticache', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Nodes per shard (Redis) quota: {serviceQuotaValue}")

        paginator = elasticache.get_paginator('describe_replication_groups')
        for page in paginator.paginate():
            for rg in page['ReplicationGroups']:
                rg_id = rg['ReplicationGroupId']
                for ng in rg.get('NodeGroups', []):
                    ng_id = ng['NodeGroupId']
                    node_count = len(ng.get('NodeGroupMembers', []))
                    resource_id = f"{rg_id}/{ng_id}"
                    logger.info(f"Replication Group {rg_id}, Shard {ng_id}: {node_count} nodes")

                    if maxNodesPerShard < node_count:
                        maxNodesPerShard = node_count
                        logger.info(f"Max value={maxNodesPerShard}")
                    if node_count / serviceQuotaValue > float(threshold) / 100:
                        sendQuotaThresholdEvent = True
                        data = {"resourceARN": resource_id, "usageValue": node_count}
                        resourceListCrossingThreshold.append(data)
                        logger.warning(f"Shard {resource_id} node count ({node_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxNodesPerShard), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking nodes per shard quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_3E7F7726(serviceCode, quotaCode, threshold, region):
    """
    Checks Subnet groups per Region (ElastiCache)
    :param serviceCode: The service code (elasticache)
    :param quotaCode: The quota code (L-3E7F7726)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    numSubnetGroups = 0
    sendQuotaThresholdEvent = False

    elasticache = boto3.client('elasticache', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Subnet groups per Region quota: {serviceQuotaValue}")

        paginator = elasticache.get_paginator('describe_cache_subnet_groups')
        for page in paginator.paginate():
            numSubnetGroups += len(page['CacheSubnetGroups'])

        logger.info(f"Total ElastiCache subnet groups in {region}: {numSubnetGroups}")

        if numSubnetGroups / serviceQuotaValue > float(threshold) / 100:
            sendQuotaThresholdEvent = True
            logger.warning(f"Subnet group count ({numSubnetGroups}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(numSubnetGroups), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking ElastiCache subnet groups quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_AF354865(serviceCode, quotaCode, threshold, region):
    """
    Checks Nodes per cluster per instance type (Redis cluster mode enabled)
    For cluster mode enabled replication groups, counts total nodes (across all shards).
    :param serviceCode: The service code (elasticache)
    :param quotaCode: The quota code (L-AF354865)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxNodesPerCluster = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    elasticache = boto3.client('elasticache', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Nodes per cluster per instance type (Redis cluster mode enabled) quota: {serviceQuotaValue}")

        paginator = elasticache.get_paginator('describe_replication_groups')
        for page in paginator.paginate():
            for rg in page['ReplicationGroups']:
                if rg.get('ClusterEnabled', False):
                    rg_id = rg['ReplicationGroupId']
                    total_nodes = 0
                    for ng in rg.get('NodeGroups', []):
                        total_nodes += len(ng.get('NodeGroupMembers', []))
                    logger.info(f"Replication Group {rg_id} (cluster mode enabled): {total_nodes} total nodes")

                    if maxNodesPerCluster < total_nodes:
                        maxNodesPerCluster = total_nodes
                        logger.info(f"Max value={maxNodesPerCluster}")
                    if total_nodes / serviceQuotaValue > float(threshold) / 100:
                        sendQuotaThresholdEvent = True
                        data = {"resourceARN": rg_id, "usageValue": total_nodes}
                        resourceListCrossingThreshold.append(data)
                        logger.warning(f"Replication Group {rg_id} node count ({total_nodes}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxNodesPerCluster), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking nodes per cluster (cluster mode enabled) quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_3F15A733(serviceCode, quotaCode, threshold, region):
    """
    Checks Parameter groups per Region (ElastiCache)
    :param serviceCode: The service code (elasticache)
    :param quotaCode: The quota code (L-3F15A733)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    numParameterGroups = 0
    sendQuotaThresholdEvent = False

    elasticache = boto3.client('elasticache', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Parameter groups per Region quota: {serviceQuotaValue}")

        paginator = elasticache.get_paginator('describe_cache_parameter_groups')
        for page in paginator.paginate():
            numParameterGroups += len(page['CacheParameterGroups'])

        logger.info(f"Total ElastiCache parameter groups in {region}: {numParameterGroups}")

        if numParameterGroups / serviceQuotaValue > float(threshold) / 100:
            sendQuotaThresholdEvent = True
            logger.warning(f"Parameter group count ({numParameterGroups}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(numParameterGroups), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking ElastiCache parameter groups quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_DFE45DF3(serviceCode, quotaCode, threshold, region):
    """
    Checks Nodes per Region (ElastiCache)
    Counts all cache cluster nodes in the region.
    :param serviceCode: The service code (elasticache)
    :param quotaCode: The quota code (L-DFE45DF3)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    numNodesPerRegion = 0
    sendQuotaThresholdEvent = False

    elasticache = boto3.client('elasticache', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Nodes per Region quota: {serviceQuotaValue}")

        paginator = elasticache.get_paginator('describe_cache_clusters')
        for page in paginator.paginate():
            numNodesPerRegion += len(page['CacheClusters'])

        logger.info(f"Total ElastiCache nodes in {region}: {numNodesPerRegion}")

        if numNodesPerRegion / serviceQuotaValue > float(threshold) / 100:
            sendQuotaThresholdEvent = True
            logger.warning(f"Node count ({numNodesPerRegion}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(numNodesPerRegion), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking ElastiCache nodes per region quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_A87EE522(serviceCode, quotaCode, threshold, region):
    """
    Checks Subnets per subnet group (ElastiCache)
    :param serviceCode: The service code (elasticache)
    :param quotaCode: The quota code (L-A87EE522)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxSubnetsPerGroup = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    elasticache = boto3.client('elasticache', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Subnets per subnet group quota: {serviceQuotaValue}")

        paginator = elasticache.get_paginator('describe_cache_subnet_groups')
        for page in paginator.paginate():
            for sg in page['CacheSubnetGroups']:
                sg_name = sg['CacheSubnetGroupName']
                subnet_count = len(sg.get('Subnets', []))
                logger.info(f"Subnet Group {sg_name}: {subnet_count} subnets")

                if maxSubnetsPerGroup < subnet_count:
                    maxSubnetsPerGroup = subnet_count
                    logger.info(f"Max value={maxSubnetsPerGroup}")
                if subnet_count / serviceQuotaValue > float(threshold) / 100:
                    sendQuotaThresholdEvent = True
                    data = {"resourceARN": sg_name, "usageValue": subnet_count}
                    resourceListCrossingThreshold.append(data)
                    logger.warning(f"Subnet Group {sg_name} subnet count ({subnet_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxSubnetsPerGroup), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking ElastiCache subnets per subnet group quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_8C334AD1(serviceCode, quotaCode, threshold, region):
    """
    Checks Nodes per cluster (Memcached)
    Counts nodes in each Memcached cache cluster.
    :param serviceCode: The service code (elasticache)
    :param quotaCode: The quota code (L-8C334AD1)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    maxNodesPerCluster = 0
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False

    elasticache = boto3.client('elasticache', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Nodes per cluster (Memcached) quota: {serviceQuotaValue}")

        paginator = elasticache.get_paginator('describe_cache_clusters')
        for page in paginator.paginate(ShowCacheNodeInfo=True):
            for cluster in page['CacheClusters']:
                if cluster.get('Engine', '').lower() == 'memcached':
                    cluster_id = cluster['CacheClusterId']
                    node_count = cluster.get('NumCacheNodes', 0)
                    logger.info(f"Memcached Cluster {cluster_id}: {node_count} nodes")

                    if maxNodesPerCluster < node_count:
                        maxNodesPerCluster = node_count
                        logger.info(f"Max value={maxNodesPerCluster}")
                    if node_count / serviceQuotaValue > float(threshold) / 100:
                        sendQuotaThresholdEvent = True
                        data = {"resourceARN": cluster_id, "usageValue": node_count}
                        resourceListCrossingThreshold.append(data)
                        logger.warning(f"Memcached Cluster {cluster_id} node count ({node_count}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxNodesPerCluster), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Memcached nodes per cluster quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_D2FEF667(serviceCode, quotaCode, threshold, region):
    """
    Checks Security groups per Region (ElastiCache)
    Note: This is the ElastiCache-specific security groups (not VPC security groups).
    Uses describe_cache_security_groups which applies to non-VPC (EC2-Classic) ElastiCache.
    For VPC-based ElastiCache, this may return 0 or only default groups.
    :param serviceCode: The service code (elasticache)
    :param quotaCode: The quota code (L-D2FEF667)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    numSecurityGroups = 0
    sendQuotaThresholdEvent = False

    elasticache = boto3.client('elasticache', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = serviceQuota['Quota']['Value']
        logger.info(f"Security groups per Region (ElastiCache) quota: {serviceQuotaValue}")

        try:
            paginator = elasticache.get_paginator('describe_cache_security_groups')
            for page in paginator.paginate():
                numSecurityGroups += len(page['CacheSecurityGroups'])
        except ClientError as e:
            # describe_cache_security_groups is not supported in VPC-only regions
            if 'InvalidParameterValue' in str(e) or 'Default' in str(e):
                logger.info(f"Cache security groups not applicable in this region (VPC-only): {e}")
                numSecurityGroups = 0
            else:
                raise

        logger.info(f"Total ElastiCache security groups in {region}: {numSecurityGroups}")

        if serviceQuotaValue > 0 and numSecurityGroups / serviceQuotaValue > float(threshold) / 100:
            sendQuotaThresholdEvent = True
            logger.warning(f"Security group count ({numSecurityGroups}) exceeds {float(threshold)}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(numSecurityGroups), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking ElastiCache security groups quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_36B04611(serviceCode, quotaCode, threshold, region):
    """
    Checks the VPC Security Groups per RDS DB instance quota usage
    :param serviceCode: The service code (rds)
    :param quotaCode: The quota code for Security groups (VPC)
    :param threshold: The threshold value (e.g., 80)
    :param region: The AWS region to check
    :return: None
    """
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False
    max_sg_count = 0

    rds_client = boto3.client('rds', region_name=region)
    sq_client = boto3.client('service-quotas', region_name=region)

    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()

    try:
        serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"RDS VPC Security Groups quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_db_instances.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                db_instances = json.load(test_file_content)
        else:
            paginator = rds_client.get_paginator('describe_db_instances')
            db_instances = []
            for page in paginator.paginate():
                db_instances.extend(page['DBInstances'])

        for db_instance in db_instances:
            db_id = db_instance['DBInstanceIdentifier']
            sg_count = len(db_instance.get('VpcSecurityGroups', []))

            usage_percentage = (sg_count / serviceQuotaValue) * 100
            logger.info(f"RDS Instance {db_id}: {sg_count} VPC security groups out of {serviceQuotaValue}")

            if max_sg_count < sg_count:
                max_sg_count = sg_count
                logger.info(f"Max value={max_sg_count}")

            if usage_percentage > float(threshold):
                data = {
                    "resourceARN": db_instance.get('DBInstanceArn', db_id),
                    "usageValue": sg_count
                }
                resourceListCrossingThreshold.append(data)
                sendQuotaThresholdEvent = True
                logger.warning(f"RDS Instance {db_id} SG count ({sg_count}) exceeds {threshold}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_sg_count), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking RDS VPC Security Groups quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_85E66A03(serviceCode, quotaCode, threshold, region):
    """
    Checks the Tags per RDS resource quota usage
    :param serviceCode: The service code (rds)
    :param quotaCode: The quota code for Tags per resource
    :param threshold: The threshold value (e.g., 80)
    :param region: The AWS region to check
    :return: None
    """
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False
    max_tag_count = 0

    rds_client = boto3.client('rds', region_name=region)
    sq_client = boto3.client('service-quotas', region_name=region)

    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()

    try:
        serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"RDS Tags per resource quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_db_instances.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                db_instances = json.load(test_file_content)
        else:
            paginator = rds_client.get_paginator('describe_db_instances')
            db_instances = []
            for page in paginator.paginate():
                db_instances.extend(page['DBInstances'])

        for db_instance in db_instances:
            db_id = db_instance['DBInstanceIdentifier']
            db_arn = db_instance.get('DBInstanceArn', db_id)

            if is_testing_enabled:
                tag_test_filename = f'tests/{inspect.stack()[0][3]}_list_tags.json'
                logger.info(f"Detected testing enabled. Using test payload from {tag_test_filename}")
                with open(tag_test_filename, 'r') as test_file_content:
                    tags_response = json.load(test_file_content)
            else:
                tags_response = rds_client.list_tags_for_resource(ResourceName=db_arn)

            tag_count = len(tags_response.get('TagList', []))

            usage_percentage = (tag_count / serviceQuotaValue) * 100
            logger.info(f"RDS Instance {db_id}: {tag_count} tags out of {serviceQuotaValue}")

            if max_tag_count < tag_count:
                max_tag_count = tag_count
                logger.info(f"Max value={max_tag_count}")

            if usage_percentage > float(threshold):
                data = {
                    "resourceARN": db_arn,
                    "usageValue": tag_count
                }
                resourceListCrossingThreshold.append(data)
                sendQuotaThresholdEvent = True
                logger.warning(f"RDS Instance {db_id} tag count ({tag_count}) exceeds {threshold}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_tag_count), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking RDS Tags per resource quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_E9D71017(serviceCode, quotaCode, threshold, region):
    """
    Checks the Rules per security group quota usage for RDS-associated VPC security groups
    :param serviceCode: The service code (rds)
    :param quotaCode: The quota code for Rules per security group
    :param threshold: The threshold value (e.g., 80)
    :param region: The AWS region to check
    :return: None
    """
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False
    max_rule_count = 0

    rds_client = boto3.client('rds', region_name=region)
    ec2_client = boto3.client('ec2', region_name=region)
    sq_client = boto3.client('service-quotas', region_name=region)

    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()

    try:
        serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"RDS Rules per security group quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_db_instances.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                db_instances = json.load(test_file_content)
        else:
            paginator = rds_client.get_paginator('describe_db_instances')
            db_instances = []
            for page in paginator.paginate():
                db_instances.extend(page['DBInstances'])

        # Collect unique VPC security group IDs across all RDS instances
        sg_ids = set()
        for db_instance in db_instances:
            for sg in db_instance.get('VpcSecurityGroups', []):
                sg_ids.add(sg['VpcSecurityGroupId'])

        if not sg_ids:
            logger.info("No VPC security groups found for RDS instances")
            updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), "0", "[]", False)
            return

        if is_testing_enabled:
            sg_test_filename = f'tests/{inspect.stack()[0][3]}_describe_security_groups.json'
            logger.info(f"Detected testing enabled. Using test payload from {sg_test_filename}")
            with open(sg_test_filename, 'r') as test_file_content:
                security_groups = json.load(test_file_content)
        else:
            # Describe security groups in batches (API supports up to 200 IDs)
            security_groups = []
            sg_id_list = list(sg_ids)
            for i in range(0, len(sg_id_list), 200):
                batch = sg_id_list[i:i+200]
                response = ec2_client.describe_security_groups(GroupIds=batch)
                security_groups.extend(response['SecurityGroups'])

        for sg in security_groups:
            sg_id = sg['GroupId']
            rule_count = len(sg.get('IpPermissions', [])) + len(sg.get('IpPermissionsEgress', []))

            usage_percentage = (rule_count / serviceQuotaValue) * 100
            logger.info(f"Security Group {sg_id}: {rule_count} rules out of {serviceQuotaValue}")

            if max_rule_count < rule_count:
                max_rule_count = rule_count
                logger.info(f"Max value={max_rule_count}")

            if usage_percentage > float(threshold):
                data = {
                    "resourceARN": sg_id,
                    "usageValue": rule_count
                }
                resourceListCrossingThreshold.append(data)
                sendQuotaThresholdEvent = True
                logger.warning(f"Security Group {sg_id} rule count ({rule_count}) exceeds {threshold}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_rule_count), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking RDS Rules per security group quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_A399AC0B(serviceCode, quotaCode, threshold, region):
    """
    Checks the Custom engine versions quota usage for RDS
    :param serviceCode: The service code (rds)
    :param quotaCode: The quota code for Custom engine versions
    :param threshold: The threshold value (e.g., 80)
    :param region: The AWS region to check
    :return: None
    """
    sendQuotaThresholdEvent = False
    custom_engine_count = 0

    rds_client = boto3.client('rds', region_name=region)
    sq_client = boto3.client('service-quotas', region_name=region)

    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()

    try:
        serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"RDS Custom engine versions quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_db_engine_versions.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                engine_versions = json.load(test_file_content)
        else:
            paginator = rds_client.get_paginator('describe_db_engine_versions')
            engine_versions = []
            for page in paginator.paginate():
                for version in page['DBEngineVersions']:
                    if version.get('DatabaseInstallationFilesS3BucketName'):
                        engine_versions.append(version)

        custom_engine_count = len(engine_versions)
        logger.info(f"RDS Custom engine versions count: {custom_engine_count} out of {serviceQuotaValue}")

        usage_percentage = (custom_engine_count / serviceQuotaValue) * 100 if serviceQuotaValue > 0 else 0

        if usage_percentage > float(threshold):
            sendQuotaThresholdEvent = True
            logger.warning(f"RDS Custom engine versions ({custom_engine_count}) exceeds {threshold}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(custom_engine_count), "[]", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking RDS Custom engine versions quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


# ============================================================================
# S3 Quota Usage Functions
# ============================================================================

def L_FAABEEBA(serviceCode, quotaCode, threshold, region):
    """
    Checks S3 Access Points usage
    :param serviceCode: The service code (s3)
    :param quotaCode: The quota code for Access Points
    :param threshold: The threshold value (e.g., 80)
    :param region: The AWS region to check
    :return: None
    """
    sendQuotaThresholdEvent = False
    access_point_count = 0

    sq_client = boto3.client('service-quotas', region_name=region)
    sts_client = boto3.client('sts', region_name=region)
    s3control = boto3.client('s3control', region_name=region)

    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()

    try:
        try:
            serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.error(f"Error calling get_service_quota: {e}")
            serviceQuota = sq_client.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)

        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"S3 Access Points quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_list_access_points.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                response = json.load(test_file_content)
                access_point_count = len(response.get('AccessPointList', []))
        else:
            account_id = sts_client.get_caller_identity()['Account']
            next_token = None
            while True:
                kwargs = {'AccountId': account_id, 'MaxResults': 1000}
                if next_token:
                    kwargs['NextToken'] = next_token
                response = s3control.list_access_points(**kwargs)
                access_point_count += len(response.get('AccessPointList', []))
                next_token = response.get('NextToken')
                if not next_token:
                    break

        logger.info(f"S3 Access Points count: {access_point_count} out of {serviceQuotaValue}")

        usage_percentage = (access_point_count / serviceQuotaValue) * 100 if serviceQuotaValue > 0 else 0

        if usage_percentage >= float(threshold):
            sendQuotaThresholdEvent = True
            logger.warning(f"S3 Access Points ({access_point_count}) exceeds {threshold}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(access_point_count), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking S3 Access Points quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error checking S3 Access Points: {e}")


def L_881EA1F4(serviceCode, quotaCode, threshold, region):
    """
    Checks S3 Multi-Region Access Points usage
    :param serviceCode: The service code (s3)
    :param quotaCode: The quota code for Multi-Region Access Points
    :param threshold: The threshold value (e.g., 80)
    :param region: The AWS region to check
    :return: None
    """
    sendQuotaThresholdEvent = False
    mrap_count = 0

    sq_client = boto3.client('service-quotas', region_name=region)
    sts_client = boto3.client('sts', region_name=region)
    s3control = boto3.client('s3control', region_name=region)

    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()

    try:
        try:
            serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.error(f"Error calling get_service_quota: {e}")
            serviceQuota = sq_client.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)

        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"S3 Multi-Region Access Points quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_list_multi_region_access_points.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                response = json.load(test_file_content)
                mrap_count = len(response.get('AccessPoints', []))
        else:
            account_id = sts_client.get_caller_identity()['Account']
            next_token = None
            while True:
                kwargs = {'AccountId': account_id, 'MaxResults': 5}
                if next_token:
                    kwargs['NextToken'] = next_token
                response = s3control.list_multi_region_access_points(**kwargs)
                mrap_count += len(response.get('AccessPoints', []))
                next_token = response.get('NextToken')
                if not next_token:
                    break

        logger.info(f"S3 Multi-Region Access Points count: {mrap_count} out of {serviceQuotaValue}")

        usage_percentage = (mrap_count / serviceQuotaValue) * 100 if serviceQuotaValue > 0 else 0

        if usage_percentage >= float(threshold):
            sendQuotaThresholdEvent = True
            logger.warning(f"S3 Multi-Region Access Points ({mrap_count}) exceeds {threshold}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(mrap_count), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking S3 Multi-Region Access Points quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error checking S3 Multi-Region Access Points: {e}")



def L_B461D596(serviceCode, quotaCode, threshold, region):
    """
    Checks S3 Replication rules per bucket (max across all buckets)
    :param serviceCode: The service code (s3)
    :param quotaCode: The quota code for Replication rules
    :param threshold: The threshold value (e.g., 80)
    :param region: The AWS region to check
    :return: None
    """
    sendQuotaThresholdEvent = False
    max_replication_rules = 0
    resourceListCrossingThreshold = []

    sq_client = boto3.client('service-quotas', region_name=region)
    s3_client = boto3.client('s3', region_name=region)

    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()

    try:
        try:
            serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.error(f"Error calling get_service_quota: {e}")
            serviceQuota = sq_client.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)

        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"S3 Replication rules per bucket quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_list_buckets.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                buckets_response = json.load(test_file_content)
        else:
            buckets_response = s3_client.list_buckets()

        for bucket in buckets_response.get('Buckets', []):
            bucket_name = bucket['Name']
            try:
                if is_testing_enabled:
                    test_filename = f'tests/{inspect.stack()[0][3]}_get_bucket_replication.json'
                    with open(test_filename, 'r') as test_file_content:
                        replication = json.load(test_file_content)
                else:
                    replication = s3_client.get_bucket_replication(Bucket=bucket_name)
                rule_count = len(replication.get('ReplicationConfiguration', {}).get('Rules', []))
                if rule_count > max_replication_rules:
                    max_replication_rules = rule_count
                usage_pct = (rule_count / serviceQuotaValue) * 100 if serviceQuotaValue > 0 else 0
                if usage_pct >= float(threshold):
                    resourceListCrossingThreshold.append({
                        "resourceARN": f"arn:aws:s3:::{bucket_name}",
                        "usageValue": rule_count
                    })
            except ClientError as e:
                if e.response['Error']['Code'] == 'ReplicationConfigurationNotFoundError':
                    continue
                logger.debug(f"Skipping bucket {bucket_name}: {e}")

        logger.info(f"S3 max Replication rules on a bucket: {max_replication_rules} out of {serviceQuotaValue}")

        usage_percentage = (max_replication_rules / serviceQuotaValue) * 100 if serviceQuotaValue > 0 else 0
        if usage_percentage >= float(threshold):
            sendQuotaThresholdEvent = True

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_replication_rules),
                         json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking S3 Replication rules quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error checking S3 Replication rules: {e}")


def L_146D5F0C(serviceCode, quotaCode, threshold, region):
    """
    Checks S3 Lifecycle rules per bucket (max across all buckets)
    :param serviceCode: The service code (s3)
    :param quotaCode: The quota code for Lifecycle rules
    :param threshold: The threshold value (e.g., 80)
    :param region: The AWS region to check
    :return: None
    """
    sendQuotaThresholdEvent = False
    max_lifecycle_rules = 0
    resourceListCrossingThreshold = []

    sq_client = boto3.client('service-quotas', region_name=region)
    s3_client = boto3.client('s3', region_name=region)

    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()

    try:
        try:
            serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.error(f"Error calling get_service_quota: {e}")
            serviceQuota = sq_client.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)

        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"S3 Lifecycle rules per bucket quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_list_buckets.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                buckets_response = json.load(test_file_content)
        else:
            buckets_response = s3_client.list_buckets()

        for bucket in buckets_response.get('Buckets', []):
            bucket_name = bucket['Name']
            try:
                if is_testing_enabled:
                    test_filename = f'tests/{inspect.stack()[0][3]}_get_bucket_lifecycle.json'
                    with open(test_filename, 'r') as test_file_content:
                        lifecycle = json.load(test_file_content)
                else:
                    lifecycle = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
                rule_count = len(lifecycle.get('Rules', []))
                if rule_count > max_lifecycle_rules:
                    max_lifecycle_rules = rule_count
                usage_pct = (rule_count / serviceQuotaValue) * 100 if serviceQuotaValue > 0 else 0
                if usage_pct >= float(threshold):
                    resourceListCrossingThreshold.append({
                        "resourceARN": f"arn:aws:s3:::{bucket_name}",
                        "usageValue": rule_count
                    })
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
                    continue
                logger.debug(f"Skipping bucket {bucket_name}: {e}")

        logger.info(f"S3 max Lifecycle rules on a bucket: {max_lifecycle_rules} out of {serviceQuotaValue}")

        usage_percentage = (max_lifecycle_rules / serviceQuotaValue) * 100 if serviceQuotaValue > 0 else 0
        if usage_percentage >= float(threshold):
            sendQuotaThresholdEvent = True

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_lifecycle_rules),
                         json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking S3 Lifecycle rules quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error checking S3 Lifecycle rules: {e}")



def L_748707F3(serviceCode, quotaCode, threshold, region):
    """
    Checks S3 Bucket lifecycle configuration rules (max lifecycle rules across all buckets)
    :param serviceCode: The service code (s3)
    :param quotaCode: The quota code for Bucket lifecycle configuration rules (L-748707F3)
    :param threshold: The threshold value (e.g., 80)
    :param region: The AWS region to check
    :return: None
    """
    sendQuotaThresholdEvent = False
    max_lifecycle_rules = 0
    resourceListCrossingThreshold = []

    sq_client = boto3.client('service-quotas', region_name=region)
    s3_client = boto3.client('s3', region_name=region)

    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()

    try:
        try:
            serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.error(f"Error calling get_service_quota: {e}")
            serviceQuota = sq_client.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)

        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"S3 Bucket lifecycle configuration rules quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_list_buckets.json'
            with open(test_filename, 'r') as test_file_content:
                buckets_response = json.load(test_file_content)
        else:
            buckets_response = s3_client.list_buckets()

        for bucket in buckets_response.get('Buckets', []):
            bucket_name = bucket['Name']
            try:
                if is_testing_enabled:
                    test_filename = f'tests/{inspect.stack()[0][3]}_get_bucket_lifecycle_configuration.json'
                    with open(test_filename, 'r') as test_file_content:
                        lifecycle_response = json.load(test_file_content)
                else:
                    lifecycle_response = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
                rule_count = len(lifecycle_response.get('Rules', []))
                if rule_count > max_lifecycle_rules:
                    max_lifecycle_rules = rule_count
                usage_pct = (rule_count / serviceQuotaValue) * 100 if serviceQuotaValue > 0 else 0
                if usage_pct >= float(threshold):
                    resourceListCrossingThreshold.append({
                        "resourceARN": f"arn:aws:s3:::{bucket_name}",
                        "usageValue": rule_count
                    })
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
                    continue
                logger.debug(f"Skipping bucket {bucket_name}: {e}")

        logger.info(f"S3 max Bucket lifecycle rules: {max_lifecycle_rules} out of {serviceQuotaValue}")

        usage_percentage = (max_lifecycle_rules / serviceQuotaValue) * 100 if serviceQuotaValue > 0 else 0
        if usage_percentage >= float(threshold):
            sendQuotaThresholdEvent = True

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_lifecycle_rules),
                         json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking S3 Bucket lifecycle rules quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error checking S3 Bucket lifecycle rules: {e}")




def L_55BA2C6C(serviceCode, quotaCode, threshold, region):
    """
    Checks S3 Bucket tags usage (max tags per bucket across all buckets)
    :param serviceCode: The service code (s3)
    :param quotaCode: The quota code for Bucket tags
    :param threshold: The threshold value (e.g., 80)
    :param region: The AWS region to check
    :return: None
    """
    sendQuotaThresholdEvent = False
    max_tag_count = 0
    resourceListCrossingThreshold = []

    sq_client = boto3.client('service-quotas', region_name=region)
    s3_client = boto3.client('s3', region_name=region)

    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()

    try:
        try:
            serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.error(f"Error calling get_service_quota: {e}")
            serviceQuota = sq_client.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)

        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"S3 Bucket tags quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_list_buckets.json'
            with open(test_filename, 'r') as test_file_content:
                buckets_response = json.load(test_file_content)
        else:
            buckets_response = s3_client.list_buckets()

        for bucket in buckets_response.get('Buckets', []):
            bucket_name = bucket['Name']
            try:
                if is_testing_enabled:
                    test_filename = f'tests/{inspect.stack()[0][3]}_get_bucket_tagging.json'
                    with open(test_filename, 'r') as test_file_content:
                        tagging = json.load(test_file_content)
                else:
                    tagging = s3_client.get_bucket_tagging(Bucket=bucket_name)
                tag_count = len(tagging.get('TagSet', []))
                if tag_count > max_tag_count:
                    max_tag_count = tag_count
                usage_pct = (tag_count / serviceQuotaValue) * 100 if serviceQuotaValue > 0 else 0
                if usage_pct >= float(threshold):
                    resourceListCrossingThreshold.append({
                        "resourceARN": f"arn:aws:s3:::{bucket_name}",
                        "usageValue": tag_count
                    })
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchTagSet':
                    continue
                logger.debug(f"Skipping bucket {bucket_name}: {e}")

        logger.info(f"S3 max Bucket tags: {max_tag_count} out of {serviceQuotaValue}")

        usage_percentage = (max_tag_count / serviceQuotaValue) * 100 if serviceQuotaValue > 0 else 0
        if usage_percentage >= float(threshold):
            sendQuotaThresholdEvent = True

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_tag_count),
                         json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking S3 Bucket tags quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error checking S3 Bucket tags: {e}")


def L_3E24E5F9(serviceCode, quotaCode, threshold, region):
    """
    Checks S3 Event notifications per bucket (max across all buckets)
    :param serviceCode: The service code (s3)
    :param quotaCode: The quota code for Event notifications
    :param threshold: The threshold value (e.g., 80)
    :param region: The AWS region to check
    :return: None
    """
    sendQuotaThresholdEvent = False
    max_notification_count = 0
    resourceListCrossingThreshold = []

    sq_client = boto3.client('service-quotas', region_name=region)
    s3_client = boto3.client('s3', region_name=region)

    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()

    try:
        try:
            serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.error(f"Error calling get_service_quota: {e}")
            serviceQuota = sq_client.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)

        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"S3 Event notifications per bucket quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_list_buckets.json'
            with open(test_filename, 'r') as test_file_content:
                buckets_response = json.load(test_file_content)
        else:
            buckets_response = s3_client.list_buckets()

        for bucket in buckets_response.get('Buckets', []):
            bucket_name = bucket['Name']
            try:
                if is_testing_enabled:
                    test_filename = f'tests/{inspect.stack()[0][3]}_get_bucket_notification.json'
                    with open(test_filename, 'r') as test_file_content:
                        notification = json.load(test_file_content)
                else:
                    notification = s3_client.get_bucket_notification_configuration(Bucket=bucket_name)
                # Count all notification configurations
                notif_count = (
                    len(notification.get('TopicConfigurations', [])) +
                    len(notification.get('QueueConfigurations', [])) +
                    len(notification.get('LambdaFunctionConfigurations', [])) +
                    len(notification.get('EventBridgeConfiguration', {}).get('Events', []))
                )
                if notif_count > max_notification_count:
                    max_notification_count = notif_count
                usage_pct = (notif_count / serviceQuotaValue) * 100 if serviceQuotaValue > 0 else 0
                if usage_pct >= float(threshold):
                    resourceListCrossingThreshold.append({
                        "resourceARN": f"arn:aws:s3:::{bucket_name}",
                        "usageValue": notif_count
                    })
            except ClientError as e:
                logger.debug(f"Skipping bucket {bucket_name}: {e}")

        logger.info(f"S3 max Event notifications on a bucket: {max_notification_count} out of {serviceQuotaValue}")

        usage_percentage = (max_notification_count / serviceQuotaValue) * 100 if serviceQuotaValue > 0 else 0
        if usage_percentage >= float(threshold):
            sendQuotaThresholdEvent = True

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_notification_count),
                         json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking S3 Event notifications quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error checking S3 Event notifications: {e}")


def L_DEDCCF9D(serviceCode, quotaCode, threshold, region):
    """
    Checks S3 Glacier Provisioned capacity units usage
    :param serviceCode: The service code (s3)
    :param quotaCode: The quota code for Glacier provisioned capacity units
    :param threshold: The threshold value (e.g., 80)
    :param region: The AWS region to check
    :return: None
    """
    sendQuotaThresholdEvent = False
    provisioned_count = 0

    sq_client = boto3.client('service-quotas', region_name=region)
    glacier_client = boto3.client('glacier', region_name=region)

    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()

    try:
        try:
            serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.error(f"Error calling get_service_quota: {e}")
            serviceQuota = sq_client.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)

        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"S3 Glacier Provisioned capacity units quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_list_provisioned_capacity.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                response = json.load(test_file_content)
        else:
            response = glacier_client.list_provisioned_capacity()

        provisioned_count = len(response.get('ProvisionedCapacityList', []))
        logger.info(f"S3 Glacier Provisioned capacity units: {provisioned_count} out of {serviceQuotaValue}")

        usage_percentage = (provisioned_count / serviceQuotaValue) * 100 if serviceQuotaValue > 0 else 0

        if usage_percentage >= float(threshold):
            sendQuotaThresholdEvent = True
            logger.warning(f"S3 Glacier Provisioned capacity ({provisioned_count}) exceeds {threshold}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(provisioned_count), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking S3 Glacier Provisioned capacity quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error checking S3 Glacier Provisioned capacity: {e}")



def L_5F53652F(serviceCode, quotaCode, threshold, region):
    """
    Checks Elastic IP address quota per NAT gateway
    :param serviceCode: The service code (vpc)
    :param quotaCode: The quota code (L-5F53652F)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False
    max_eip_count = 0

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"Elastic IP addresses per NAT gateway quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_nat_gateways.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                nat_gateways = json.load(test_file_content)
        else:
            paginator = ec2.get_paginator('describe_nat_gateways')
            nat_gateways = []
            for page in paginator.paginate(Filters=[{'Name': 'state', 'Values': ['available']}]):
                nat_gateways.extend(page['NatGateways'])

        for ngw in nat_gateways:
            ngw_id = ngw['NatGatewayId']
            # Count EIPs associated with this NAT gateway
            eip_count = sum(1 for addr in ngw.get('NatGatewayAddresses', []) if addr.get('AllocationId'))
            logger.info(f"NAT Gateway {ngw_id}: {eip_count} Elastic IPs out of {serviceQuotaValue}")

            if max_eip_count < eip_count:
                max_eip_count = eip_count
                logger.info(f"Max value={max_eip_count}")

            usage_percentage = (eip_count / serviceQuotaValue) * 100
            if usage_percentage > float(threshold) * 100:
                sendQuotaThresholdEvent = True
                data = {"resourceARN": ngw_id, "usageValue": eip_count}
                resourceListCrossingThreshold.append(data)
                logger.warning(f"NAT Gateway {ngw_id} EIP count ({eip_count}) exceeds {float(threshold) * 100}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_eip_count), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Elastic IP per NAT gateway quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_085A6257(serviceCode, quotaCode, threshold, region):
    """
    Checks IPv6 CIDR blocks per VPC
    :param serviceCode: The service code (vpc)
    :param quotaCode: The quota code (L-085A6257)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False
    max_ipv6_cidrs = 0

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"IPv6 CIDR blocks per VPC quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_vpcs.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                vpcs_pages = json.load(test_file_content)
        else:
            paginator = ec2.get_paginator('describe_vpcs')
            vpcs_pages = paginator.paginate()

        for page in vpcs_pages:
            for vpc in page['Vpcs']:
                vpc_id = vpc['VpcId']
                # Count IPv6 CIDR block associations
                ipv6_cidrs = [assoc for assoc in vpc.get('Ipv6CidrBlockAssociationSet', [])
                              if assoc.get('Ipv6CidrBlockState', {}).get('State') == 'associated']
                ipv6_count = len(ipv6_cidrs)
                logger.info(f"VPC {vpc_id}: {ipv6_count} IPv6 CIDR blocks out of {serviceQuotaValue}")

                if max_ipv6_cidrs < ipv6_count:
                    max_ipv6_cidrs = ipv6_count
                    logger.info(f"Max value={max_ipv6_cidrs}")

                usage_percentage = (ipv6_count / serviceQuotaValue) * 100
                if usage_percentage > float(threshold) * 100:
                    sendQuotaThresholdEvent = True
                    data = {"resourceARN": vpc_id, "usageValue": ipv6_count}
                    resourceListCrossingThreshold.append(data)
                    logger.warning(f"VPC {vpc_id} IPv6 CIDR count ({ipv6_count}) exceeds {float(threshold) * 100}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_ipv6_cidrs), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking IPv6 CIDR blocks per VPC quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_3248932A(serviceCode, quotaCode, threshold, region):
    """
    Checks Characters per VPC endpoint policy
    :param serviceCode: The service code (vpc)
    :param quotaCode: The quota code (L-3248932A)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False
    max_policy_length = 0

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"Characters per VPC endpoint policy quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_vpc_endpoints.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                endpoints_pages = json.load(test_file_content)
        else:
            paginator = ec2.get_paginator('describe_vpc_endpoints')
            endpoints_pages = paginator.paginate()

        for page in endpoints_pages:
            for endpoint in page['VpcEndpoints']:
                endpoint_id = endpoint['VpcEndpointId']
                policy_doc = endpoint.get('PolicyDocument', '')
                policy_length = len(policy_doc)
                logger.info(f"VPC Endpoint {endpoint_id}: policy length {policy_length} chars out of {serviceQuotaValue}")

                if max_policy_length < policy_length:
                    max_policy_length = policy_length
                    logger.info(f"Max value={max_policy_length}")

                usage_percentage = (policy_length / serviceQuotaValue) * 100
                if usage_percentage > float(threshold) * 100:
                    sendQuotaThresholdEvent = True
                    data = {"resourceARN": endpoint_id, "usageValue": policy_length}
                    resourceListCrossingThreshold.append(data)
                    logger.warning(f"VPC Endpoint {endpoint_id} policy length ({policy_length}) exceeds {float(threshold) * 100}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_policy_length), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Characters per VPC endpoint policy quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_29B6F2EB(serviceCode, quotaCode, threshold, region):
    """
    Checks Interface VPC Endpoints per VPC
    :param serviceCode: The service code (vpc)
    :param quotaCode: The quota code (L-29B6F2EB)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False
    maxInterfaceEndpointsPerVPC = 0

    ec2 = boto3.client('ec2', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"Interface VPC Endpoints per VPC quota: {serviceQuotaValue}")

        # Build a map of VPC -> count of Interface endpoints
        vpc_endpoint_counts = defaultdict(int)

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_vpc_endpoints.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                endpoints_pages = json.load(test_file_content)
        else:
            paginator = ec2.get_paginator('describe_vpc_endpoints')
            endpoints_pages = paginator.paginate(
                Filters=[{'Name': 'vpc-endpoint-type', 'Values': ['Interface']}]
            )

        for page in endpoints_pages:
            for endpoint in page['VpcEndpoints']:
                vpc_id = endpoint['VpcId']
                vpc_endpoint_counts[vpc_id] += 1

        for vpc_id, count in vpc_endpoint_counts.items():
            logger.info(f"VPC {vpc_id}: {count} Interface endpoints out of {serviceQuotaValue}")

            if maxInterfaceEndpointsPerVPC < count:
                maxInterfaceEndpointsPerVPC = count
                logger.info(f"Max value={maxInterfaceEndpointsPerVPC}")

            usage_percentage = (count / serviceQuotaValue) * 100
            if usage_percentage > float(threshold) * 100:
                sendQuotaThresholdEvent = True
                data = {"resourceARN": vpc_id, "usageValue": count}
                resourceListCrossingThreshold.append(data)
                logger.warning(f"VPC {vpc_id} Interface endpoint count ({count}) exceeds {float(threshold) * 100}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxInterfaceEndpointsPerVPC), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Interface VPC Endpoints per VPC quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_6E386A05(serviceCode, quotaCode, threshold, region):
    """
    Checks AWS Transfer Family Servers per Account
    :param serviceCode: The service code (transfer)
    :param quotaCode: The quota code (L-6E386A05)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False
    server_count = 0

    transfer_client = boto3.client('transfer', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"Transfer Family Servers per Account quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_list_servers.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                servers_response = json.load(test_file_content)
                server_count = len(servers_response.get('Servers', []))
        else:
            paginator = transfer_client.get_paginator('list_servers')
            for page in paginator.paginate():
                server_count += len(page.get('Servers', []))

        logger.info(f"Transfer Family server count: {server_count} out of {serviceQuotaValue}")

        usage_percentage = (server_count / serviceQuotaValue) * 100
        if usage_percentage > float(threshold) * 100:
            sendQuotaThresholdEvent = True
            logger.warning(f"Transfer Family server count ({server_count}) exceeds {float(threshold) * 100}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(server_count), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Transfer Family Servers per Account quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_2146F1FD(serviceCode, quotaCode, threshold, region):
    """
    Checks DMS Endpoints per Instance (replication instance)
    :param serviceCode: The service code (dms)
    :param quotaCode: The quota code (L-2146F1FD)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    resourceListCrossingThreshold = []
    sendQuotaThresholdEvent = False
    max_endpoints_per_instance = 0

    dms_client = boto3.client('dms', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"DMS Endpoints per Instance quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename_instances = f'tests/{inspect.stack()[0][3]}_describe_replication_instances.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename_instances}")
            with open(test_filename_instances, 'r') as test_file_content:
                replication_instances = json.load(test_file_content)
            test_filename_endpoints = f'tests/{inspect.stack()[0][3]}_describe_connections.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename_endpoints}")
            with open(test_filename_endpoints, 'r') as test_file_content:
                connections = json.load(test_file_content)
        else:
            # Get all replication instances
            replication_instances = []
            paginator = dms_client.get_paginator('describe_replication_instances')
            for page in paginator.paginate():
                replication_instances.extend(page.get('ReplicationInstances', []))

            # Get all connections (maps endpoints to replication instances)
            connections = []
            conn_paginator = dms_client.get_paginator('describe_connections')
            for page in conn_paginator.paginate():
                connections.extend(page.get('Connections', []))

        # Count endpoints per replication instance
        instance_endpoint_counts = defaultdict(int)
        for conn in connections:
            instance_arn = conn.get('ReplicationInstanceArn', '')
            instance_endpoint_counts[instance_arn] += 1

        for instance in replication_instances:
            instance_arn = instance['ReplicationInstanceArn']
            instance_id = instance['ReplicationInstanceIdentifier']
            endpoint_count = instance_endpoint_counts.get(instance_arn, 0)
            logger.info(f"Replication Instance {instance_id}: {endpoint_count} endpoints out of {serviceQuotaValue}")

            if max_endpoints_per_instance < endpoint_count:
                max_endpoints_per_instance = endpoint_count
                logger.info(f"Max value={max_endpoints_per_instance}")

            usage_percentage = (endpoint_count / serviceQuotaValue) * 100
            if usage_percentage > float(threshold) * 100:
                sendQuotaThresholdEvent = True
                data = {"resourceARN": instance_arn, "usageValue": endpoint_count}
                resourceListCrossingThreshold.append(data)
                logger.warning(f"Replication Instance {instance_id} endpoint count ({endpoint_count}) exceeds {float(threshold) * 100}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_endpoints_per_instance), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking DMS Endpoints per Instance quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_6B80B8FA(serviceCode, quotaCode, threshold, region):
    """
    Checks Launch configurations per region
    :param serviceCode: The service code (autoscaling)
    :param quotaCode: The quota code (L-6B80B8FA)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False
    launch_config_count = 0

    autoscaling_client = boto3.client('autoscaling', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"Launch configurations per region quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_describe_launch_configurations.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                response = json.load(test_file_content)
                launch_config_count = len(response.get('LaunchConfigurations', []))
        else:
            paginator = autoscaling_client.get_paginator('describe_launch_configurations')
            for page in paginator.paginate():
                launch_config_count += len(page.get('LaunchConfigurations', []))

        logger.info(f"Launch configuration count: {launch_config_count} out of {serviceQuotaValue}")

        usage_percentage = (launch_config_count / serviceQuotaValue) * 100
        if usage_percentage > float(threshold) * 100:
            sendQuotaThresholdEvent = True
            logger.warning(f"Launch configuration count ({launch_config_count}) exceeds {float(threshold) * 100}% of the quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(launch_config_count), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking Launch configurations per region quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def L_349AD9CA(serviceCode, quotaCode, threshold, region):
    """
    Checks S3 Replication transfer rate.
    This quota is not directly measurable via API — it represents a bandwidth limit (Gbps).
    We report the configured quota value and log that usage must be monitored via CloudWatch
    S3 replication metrics (S3 ReplicationLatency / BytesPendingReplication).
    :param serviceCode: The service code (s3)
    :param quotaCode: The quota code (L-349AD9CA)
    :param threshold: The threshold value
    :param region: The AWS region to check
    :return: None
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False
    total_replication_rules = 0

    s3_client = boto3.client('s3', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"S3 Replication transfer rate quota: {serviceQuotaValue} Gbps")

        # Replication transfer rate is a bandwidth limit, not a countable resource.
        # We count the number of replication rules as a proxy indicator of replication activity.
        if is_testing_enabled:
            test_filename = f'tests/{inspect.stack()[0][3]}_list_buckets.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as test_file_content:
                response = json.load(test_file_content)
        else:
            response = s3_client.list_buckets()

        for bucket in response.get('Buckets', []):
            bucket_name = bucket['BucketName']
            try:
                if not is_testing_enabled:
                    # Check bucket region to only count buckets in the target region
                    bucket_location = s3_client.get_bucket_location(Bucket=bucket_name)
                    bucket_region = bucket_location.get('LocationConstraint') or 'us-east-1'
                    if bucket_region != region:
                        continue

                    replication_config = s3_client.get_bucket_replication(Bucket=bucket_name)
                    rules = replication_config.get('ReplicationConfiguration', {}).get('Rules', [])
                    total_replication_rules += len(rules)
                    logger.info(f"Bucket {bucket_name}: {len(rules)} replication rules")
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'ReplicationConfigurationNotFoundError':
                    continue
                elif error_code == 'AccessDenied':
                    logger.warning(f"Access denied for bucket {bucket_name}, skipping")
                    continue
                else:
                    logger.warning(f"Error checking replication for bucket {bucket_name}: {e}")
                    continue

        logger.info(f"Total S3 replication rules in region {region}: {total_replication_rules}. Quota is {serviceQuotaValue} Gbps (bandwidth limit - monitor via CloudWatch S3 replication metrics)")

        # Report the replication rule count as a proxy. Actual bandwidth usage requires CloudWatch metrics.
        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(total_replication_rules), "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking S3 Replication transfer rate quota: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


# ---------------------------------------------------------------------------
# Bedrock quota helpers
# ---------------------------------------------------------------------------

def _bedrock_rpm_quota(serviceCode, quotaCode, threshold, region, model_id, quota_name):
    """
    Generic helper for Bedrock InvokeModel requests-per-minute quotas.
    Uses CloudWatch AWS/Bedrock Invocations metric (Sum over 1 min) as usage.
    :param model_id: The Bedrock model identifier used in the CloudWatch dimension
    :param quota_name: Human-readable name for logging
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False
    max_rpm = 0
    resourceListCrossingThreshold = []

    cloudwatch = boto3.client('cloudwatch', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"{quota_name} quota: {serviceQuotaValue} RPM")

        if is_testing_enabled:
            test_filename = f'tests/{quotaCode}_cloudwatch.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as f:
                response = json.load(f)
        else:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=5)
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/Bedrock',
                MetricName='Invocations',
                Dimensions=[{'Name': 'ModelId', 'Value': model_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=60,
                Statistics=['Sum']
            )

        datapoints = response.get('Datapoints', [])
        if datapoints:
            latest = max(datapoints, key=lambda x: x['Timestamp'])
            max_rpm = float(latest['Sum'])
            logger.info(f"{quota_name}: {max_rpm} invocations/min (quota {serviceQuotaValue})")

            usage_pct = (max_rpm / serviceQuotaValue) * 100 if serviceQuotaValue > 0 else 0
            if usage_pct > float(threshold):
                resourceListCrossingThreshold.append({
                    "resourceARN": model_id,
                    "usageValue": max_rpm
                })
                sendQuotaThresholdEvent = True
                logger.warning(f"{quota_name} usage ({max_rpm}) exceeds {threshold}% of quota ({serviceQuotaValue})")
        else:
            logger.info(f"No CloudWatch data for {quota_name} in region {region}")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_rpm),
                         json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)
    except ClientError as e:
        logger.error(f"Error checking {quota_name}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error checking {quota_name}: {e}")


def _bedrock_tpm_quota(serviceCode, quotaCode, threshold, region, model_id, quota_name):
    """
    Generic helper for Bedrock InvokeModel tokens-per-minute quotas.
    Sums InputTokenCount + OutputTokenCount over 1-min periods from CloudWatch.
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False
    max_tpm = 0
    resourceListCrossingThreshold = []

    cloudwatch = boto3.client('cloudwatch', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"{quota_name} quota: {serviceQuotaValue} TPM")

        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=5)
        total_tokens = 0

        for metric_name in ['InputTokenCount', 'OutputTokenCount']:
            if is_testing_enabled:
                test_filename = f'tests/{quotaCode}_{metric_name}.json'
                logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
                with open(test_filename, 'r') as f:
                    response = json.load(f)
            else:
                response = cloudwatch.get_metric_statistics(
                    Namespace='AWS/Bedrock',
                    MetricName=metric_name,
                    Dimensions=[{'Name': 'ModelId', 'Value': model_id}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=60,
                    Statistics=['Sum']
                )

            datapoints = response.get('Datapoints', [])
            if datapoints:
                latest = max(datapoints, key=lambda x: x['Timestamp'])
                total_tokens += float(latest['Sum'])

        max_tpm = total_tokens
        logger.info(f"{quota_name}: {max_tpm} tokens/min (quota {serviceQuotaValue})")

        usage_pct = (max_tpm / serviceQuotaValue) * 100 if serviceQuotaValue > 0 else 0
        if usage_pct > float(threshold):
            resourceListCrossingThreshold.append({
                "resourceARN": model_id,
                "usageValue": max_tpm
            })
            sendQuotaThresholdEvent = True
            logger.warning(f"{quota_name} usage ({max_tpm}) exceeds {threshold}% of quota ({serviceQuotaValue})")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_tpm),
                         json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)
    except ClientError as e:
        logger.error(f"Error checking {quota_name}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error checking {quota_name}: {e}")


def _bedrock_guardrail_rps_quota(serviceCode, quotaCode, threshold, region, quota_name):
    """
    Generic helper for Bedrock ApplyGuardrail requests-per-second quotas.
    Uses CloudWatch AWS/Bedrock GuardrailInvocations metric (Sum over 60s / 60).
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False
    max_rps = 0
    resourceListCrossingThreshold = []

    cloudwatch = boto3.client('cloudwatch', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"{quota_name} quota: {serviceQuotaValue} RPS")

        if is_testing_enabled:
            test_filename = f'tests/{quotaCode}_cloudwatch.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as f:
                response = json.load(f)
        else:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=5)
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/Bedrock',
                MetricName='Invocations',
                StartTime=start_time,
                EndTime=end_time,
                Period=60,
                Statistics=['Sum']
            )

        datapoints = response.get('Datapoints', [])
        if datapoints:
            latest = max(datapoints, key=lambda x: x['Timestamp'])
            # Convert per-minute sum to per-second average
            max_rps = float(latest['Sum']) / 60.0
            logger.info(f"{quota_name}: ~{max_rps:.2f} RPS (quota {serviceQuotaValue})")

            usage_pct = (max_rps / serviceQuotaValue) * 100 if serviceQuotaValue > 0 else 0
            if usage_pct > float(threshold):
                resourceListCrossingThreshold.append({
                    "resourceARN": "bedrock-guardrail",
                    "usageValue": max_rps
                })
                sendQuotaThresholdEvent = True
                logger.warning(f"{quota_name} usage ({max_rps:.2f}) exceeds {threshold}% of quota ({serviceQuotaValue})")
        else:
            logger.info(f"No CloudWatch data for {quota_name} in region {region}")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(round(max_rps, 2)),
                         json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)
    except ClientError as e:
        logger.error(f"Error checking {quota_name}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error checking {quota_name}: {e}")


def _bedrock_guardrail_text_units_quota(serviceCode, quotaCode, threshold, region, quota_name):
    """
    Generic helper for Bedrock ApplyGuardrail text-units-per-second quotas
    (content filter, denied topic, sensitive info, word filter, contextual grounding).
    These are not directly measurable via a single CloudWatch metric.
    We report the quota value and use GuardrailInvocations as a proxy indicator.
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False
    usage_proxy = 0

    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"{quota_name} quota: {serviceQuotaValue} text units/sec. "
                     "Actual text-unit throughput is not directly available via CloudWatch; reporting quota value only.")

        # No direct metric for text units processed per second — report 0 usage
        # and let the quota value be tracked for awareness.
        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(usage_proxy),
                         "", sendQuotaThresholdEvent)

    except ClientError as e:
        logger.error(f"Error checking {quota_name}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error checking {quota_name}: {e}")


# ---------------------------------------------------------------------------
# Bedrock On-demand InvokeModel — Requests Per Minute
# ---------------------------------------------------------------------------

def L_254CACF4(serviceCode, quotaCode, threshold, region):
    """On-demand InvokeModel requests per minute for Anthropic Claude 3.5 Sonnet"""
    _bedrock_rpm_quota(serviceCode, quotaCode, threshold, region,
                       'anthropic.claude-3-5-sonnet-20240620-v1:0',
                       'On-demand InvokeModel RPM - Claude 3.5 Sonnet')


def L_79E773B3(serviceCode, quotaCode, threshold, region):
    """On-demand InvokeModel requests per minute for Anthropic Claude 3.5 Sonnet V2"""
    _bedrock_rpm_quota(serviceCode, quotaCode, threshold, region,
                       'anthropic.claude-3-5-sonnet-20241022-v2:0',
                       'On-demand InvokeModel RPM - Claude 3.5 Sonnet V2')


def L_2DC80978(serviceCode, quotaCode, threshold, region):
    """On-demand InvokeModel requests per minute for Anthropic Claude 3 Haiku"""
    _bedrock_rpm_quota(serviceCode, quotaCode, threshold, region,
                       'anthropic.claude-3-haiku-20240307-v1:0',
                       'On-demand InvokeModel RPM - Claude 3 Haiku')


# ---------------------------------------------------------------------------
# Bedrock On-demand InvokeModel — Tokens Per Minute
# ---------------------------------------------------------------------------

def L_A50569E5(serviceCode, quotaCode, threshold, region):
    """On-demand InvokeModel tokens per minute for Anthropic Claude 3.5 Sonnet"""
    _bedrock_tpm_quota(serviceCode, quotaCode, threshold, region,
                       'anthropic.claude-3-5-sonnet-20240620-v1:0',
                       'On-demand InvokeModel TPM - Claude 3.5 Sonnet')


def L_AD41C330(serviceCode, quotaCode, threshold, region):
    """On-demand InvokeModel tokens per minute for Anthropic Claude 3.5 Sonnet V2"""
    _bedrock_tpm_quota(serviceCode, quotaCode, threshold, region,
                       'anthropic.claude-3-5-sonnet-20241022-v2:0',
                       'On-demand InvokeModel TPM - Claude 3.5 Sonnet V2')


def L_8CE99163(serviceCode, quotaCode, threshold, region):
    """On-demand InvokeModel tokens per minute for Anthropic Claude 3 Haiku"""
    _bedrock_tpm_quota(serviceCode, quotaCode, threshold, region,
                       'anthropic.claude-3-haiku-20240307-v1:0',
                       'On-demand InvokeModel TPM - Claude 3 Haiku')


# ---------------------------------------------------------------------------
# Bedrock Cross-Region InvokeModel — Requests Per Minute
# ---------------------------------------------------------------------------

def L_F457545D(serviceCode, quotaCode, threshold, region):
    """Cross-region InvokeModel requests per minute for Anthropic Claude 3.5 Sonnet"""
    _bedrock_rpm_quota(serviceCode, quotaCode, threshold, region,
                       'anthropic.claude-3-5-sonnet-20240620-v1:0',
                       'Cross-region InvokeModel RPM - Claude 3.5 Sonnet')


def L_1D3E59A3(serviceCode, quotaCode, threshold, region):
    """Cross-Region InvokeModel requests per minute for Anthropic Claude 3.5 Sonnet V2"""
    _bedrock_rpm_quota(serviceCode, quotaCode, threshold, region,
                       'anthropic.claude-3-5-sonnet-20241022-v2:0',
                       'Cross-region InvokeModel RPM - Claude 3.5 Sonnet V2')


# ---------------------------------------------------------------------------
# Bedrock Cross-Region InvokeModel — Tokens Per Minute
# ---------------------------------------------------------------------------

def L_FF8B4E28(serviceCode, quotaCode, threshold, region):
    """Cross-Region InvokeModel tokens per minute for Anthropic Claude 3.5 Sonnet V2"""
    _bedrock_tpm_quota(serviceCode, quotaCode, threshold, region,
                       'anthropic.claude-3-5-sonnet-20241022-v2:0',
                       'Cross-region InvokeModel TPM - Claude 3.5 Sonnet V2')


def L_479B647F(serviceCode, quotaCode, threshold, region):
    """Cross-region InvokeModel tokens per minute for Anthropic Claude 3.5 Sonnet"""
    _bedrock_tpm_quota(serviceCode, quotaCode, threshold, region,
                       'anthropic.claude-3-5-sonnet-20240620-v1:0',
                       'Cross-region InvokeModel TPM - Claude 3.5 Sonnet')


# ---------------------------------------------------------------------------
# Bedrock ApplyGuardrail — Requests Per Second
# ---------------------------------------------------------------------------

def L_9072D6F0(serviceCode, quotaCode, threshold, region):
    """On-demand ApplyGuardrail requests per second"""
    _bedrock_guardrail_rps_quota(serviceCode, quotaCode, threshold, region,
                                  'On-demand ApplyGuardrail RPS')


# ---------------------------------------------------------------------------
# Bedrock ApplyGuardrail — Text Units Per Second (policy-specific)
# ---------------------------------------------------------------------------

def L_01F3CD81(serviceCode, quotaCode, threshold, region):
    """On-demand ApplyGuardrail Content filter policy text units per second"""
    _bedrock_guardrail_text_units_quota(serviceCode, quotaCode, threshold, region,
                                         'ApplyGuardrail Content filter text units/sec')


def L_124DCF3D(serviceCode, quotaCode, threshold, region):
    """On-demand ApplyGuardrail Denied topic policy text units per second"""
    _bedrock_guardrail_text_units_quota(serviceCode, quotaCode, threshold, region,
                                         'ApplyGuardrail Denied topic text units/sec')


def L_CFCAAB0E(serviceCode, quotaCode, threshold, region):
    """On-demand ApplyGuardrail Sensitive information filter policy text units per second"""
    _bedrock_guardrail_text_units_quota(serviceCode, quotaCode, threshold, region,
                                         'ApplyGuardrail Sensitive info filter text units/sec')


def L_9F4DB459(serviceCode, quotaCode, threshold, region):
    """On-demand ApplyGuardrail Word filter policy text units per second"""
    _bedrock_guardrail_text_units_quota(serviceCode, quotaCode, threshold, region,
                                         'ApplyGuardrail Word filter text units/sec')


# ---------------------------------------------------------------------------
# Bedrock Contextual Grounding
# ---------------------------------------------------------------------------

def L_893F8BF9(serviceCode, quotaCode, threshold, region):
    """Contextual grounding source length in text units"""
    _bedrock_guardrail_text_units_quota(serviceCode, quotaCode, threshold, region,
                                         'Contextual grounding source length text units')

# ---------------------------------------------------------------------------
# Helper: API Rate Limit Quota (uses AWS/Usage CloudWatch namespace)
# ---------------------------------------------------------------------------

def _api_rate_limit_quota(serviceCode, quotaCode, threshold, region, cw_service, cw_resource, cw_type, cw_class, quota_name):
    """
    Generic helper for API rate-limit / throttle quotas.
    Uses CloudWatch AWS/Usage namespace with CallCount metric to approximate usage.
    :param cw_service: CloudWatch 'Service' dimension value (e.g. 'ElasticMapReduce')
    :param cw_resource: CloudWatch 'Resource' dimension value (e.g. 'DescribeCluster')
    :param cw_type: CloudWatch 'Type' dimension value (e.g. 'API')
    :param cw_class: CloudWatch 'Class' dimension value (e.g. 'None')
    :param quota_name: Human-readable name for logging
    """
    is_testing_enabled = 'IS_TESTING_ENABLED' in os.environ.keys()
    sendQuotaThresholdEvent = False
    max_usage = 0
    resourceListCrossingThreshold = []

    cloudwatch = boto3.client('cloudwatch', region_name=region)
    sq = boto3.client('service-quotas', region_name=region)

    try:
        try:
            serviceQuota = sq.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        except Exception as e:
            logger.info(f"Error calling get_service_quota: {e}. Fallback to default")
            serviceQuota = sq.get_aws_default_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
        serviceQuotaValue = float(serviceQuota['Quota']['Value'])
        logger.info(f"{quota_name} quota: {serviceQuotaValue}")

        if is_testing_enabled:
            test_filename = f'tests/{quotaCode}_cloudwatch.json'
            logger.info(f"Detected testing enabled. Using test payload from {test_filename}")
            with open(test_filename, 'r') as f:
                response = json.load(f)
        else:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=5)
            dimensions = [
                {'Name': 'Service', 'Value': cw_service},
                {'Name': 'Resource', 'Value': cw_resource},
                {'Name': 'Type', 'Value': cw_type},
                {'Name': 'Class', 'Value': cw_class},
            ]
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/Usage',
                MetricName='CallCount',
                Dimensions=dimensions,
                StartTime=start_time,
                EndTime=end_time,
                Period=60,
                Statistics=['Sum']
            )

        datapoints = response.get('Datapoints', [])
        if datapoints:
            latest = max(datapoints, key=lambda x: x['Timestamp'])
            max_usage = float(latest['Sum'])
            logger.info(f"{quota_name}: {max_usage} calls/min (quota {serviceQuotaValue})")

            usage_pct = (max_usage / serviceQuotaValue) * 100 if serviceQuotaValue > 0 else 0
            if usage_pct > float(threshold):
                resourceListCrossingThreshold.append({
                    "resourceARN": f"{cw_service}/{cw_resource}",
                    "usageValue": max_usage
                })
                sendQuotaThresholdEvent = True
                logger.warning(f"{quota_name} usage ({max_usage}) exceeds {threshold}% of quota ({serviceQuotaValue})")
        else:
            logger.info(f"No CloudWatch usage data for {quota_name} in region {region}")

        updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(max_usage),
                         json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)
    except ClientError as e:
        logger.error(f"Error checking {quota_name}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error checking {quota_name}: {e}")


# ---------------------------------------------------------------------------
# EMR (elasticmapreduce) API Rate Limit Quotas
# ---------------------------------------------------------------------------

def L_D74118B4(serviceCode, quotaCode, threshold, region):
    """Replenishment rate of DescribeCluster calls"""
    _api_rate_limit_quota(serviceCode, quotaCode, threshold, region,
                          'ElasticMapReduce', 'DescribeCluster', 'API', 'None',
                          'EMR Replenishment rate of DescribeCluster calls')

def L_283CCA2A(serviceCode, quotaCode, threshold, region):
    """The maximum number of API requests that you can make per second"""
    _api_rate_limit_quota(serviceCode, quotaCode, threshold, region,
                          'ElasticMapReduce', 'None', 'API', 'None',
                          'EMR Max API requests per second')

def L_81AF5123(serviceCode, quotaCode, threshold, region):
    """The maximum number of DescribeCluster API requests that you can make per second"""
    _api_rate_limit_quota(serviceCode, quotaCode, threshold, region,
                          'ElasticMapReduce', 'DescribeCluster', 'API', 'None',
                          'EMR Max DescribeCluster requests per second')

def L_432FAB44(serviceCode, quotaCode, threshold, region):
    """The maximum rate at which your bucket replenishes for all EMR operations"""
    _api_rate_limit_quota(serviceCode, quotaCode, threshold, region,
                          'ElasticMapReduce', 'None', 'API', 'None',
                          'EMR Max bucket replenishment rate for all operations')

def L_72BCD5B1(serviceCode, quotaCode, threshold, region):
    """Replenishment rate of DescribeStep calls"""
    _api_rate_limit_quota(serviceCode, quotaCode, threshold, region,
                          'ElasticMapReduce', 'DescribeStep', 'API', 'None',
                          'EMR Replenishment rate of DescribeStep calls')

def L_B810434D(serviceCode, quotaCode, threshold, region):
    """The maximum number of DescribeStep API requests that you can make per second"""
    _api_rate_limit_quota(serviceCode, quotaCode, threshold, region,
                          'ElasticMapReduce', 'DescribeStep', 'API', 'None',
                          'EMR Max DescribeStep requests per second')


# ---------------------------------------------------------------------------
# EventBridge (events) API Rate Limit Quotas
# ---------------------------------------------------------------------------

def L_5540C5E3(serviceCode, quotaCode, threshold, region):
    """Invocations throttle limit in transactions per second"""
    _api_rate_limit_quota(serviceCode, quotaCode, threshold, region,
                          'EventBridge', 'Invocations', 'API', 'None',
                          'EventBridge Invocations throttle limit TPS')

def L_9B653E91(serviceCode, quotaCode, threshold, region):
    """PutEvents throttle limit in transactions per second"""
    _api_rate_limit_quota(serviceCode, quotaCode, threshold, region,
                          'EventBridge', 'PutEvents', 'API', 'None',
                          'EventBridge PutEvents throttle limit TPS')


# ---------------------------------------------------------------------------
# CloudWatch Monitoring API Rate Limit Quotas
# ---------------------------------------------------------------------------

def L_05D334F0(serviceCode, quotaCode, threshold, region):
    """Rate of ListMetrics requests"""
    _api_rate_limit_quota(serviceCode, quotaCode, threshold, region,
                          'CloudWatch', 'ListMetrics', 'API', 'None',
                          'CloudWatch Rate of ListMetrics requests')

def L_5E141212(serviceCode, quotaCode, threshold, region):
    """Rate of GetMetricData requests"""
    _api_rate_limit_quota(serviceCode, quotaCode, threshold, region,
                          'CloudWatch', 'GetMetricData', 'API', 'None',
                          'CloudWatch Rate of GetMetricData requests')

def L_EE839489(serviceCode, quotaCode, threshold, region):
    """Rate of GetMetricStatistics requests"""
    _api_rate_limit_quota(serviceCode, quotaCode, threshold, region,
                          'CloudWatch', 'GetMetricStatistics', 'API', 'None',
                          'CloudWatch Rate of GetMetricStatistics requests')


# ============================================================================
# IAM Quota Functions
# ============================================================================

def L_F55AF5E4(serviceCode, quotaCode, region, threshold):
    """
    Checks Users per account
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    user_count = 0
    paginator = iam_client.get_paginator('list_users').paginate()
    for page in paginator:
        user_count += len(page['Users'])

    logger.info(f"Total IAM users: {user_count}")
    if user_count / serviceQuotaValue > float(threshold) / 100:
        sendQuotaThresholdEvent = True

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(user_count), "", sendQuotaThresholdEvent)


def L_FC9EC213(serviceCode, quotaCode, region, threshold):
    """
    Checks Tags per user (max tags across all users)
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    maxTags = 0
    resourceListCrossingThreshold = []
    paginator = iam_client.get_paginator('list_users').paginate()
    for page in paginator:
        for user in page['Users']:
            tags_response = iam_client.list_user_tags(UserName=user['UserName'])
            tag_count = len(tags_response['Tags'])
            if tag_count / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                resourceListCrossingThreshold.append({
                    "resourceARN": user['Arn'],
                    "usageValue": tag_count
                })
            if tag_count > maxTags:
                maxTags = tag_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxTags), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_C07B4B0D(serviceCode, quotaCode, region, threshold):
    """
    Checks Role trust policy length (max across all roles)
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    maxPolicyLen = 0
    resourceListCrossingThreshold = []
    paginator = iam_client.get_paginator('list_roles').paginate()
    for page in paginator:
        for role in page['Roles']:
            # AssumeRolePolicyDocument is already included in list_roles response
            policy_doc = json.dumps(role.get('AssumeRolePolicyDocument', {}))
            policy_len = len(policy_doc)
            if policy_len / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                resourceListCrossingThreshold.append({
                    "resourceARN": role['Arn'],
                    "usageValue": policy_len
                })
            if policy_len > maxPolicyLen:
                maxPolicyLen = policy_len

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxPolicyLen), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)



def L_3AD47CAE(serviceCode, quotaCode, region, threshold):
    """
    Checks Identity providers per IAM SAML provider object
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    maxIdpCount = 0
    resourceListCrossingThreshold = []
    saml_providers = iam_client.list_saml_providers()['SAMLProviderList']
    for provider in saml_providers:
        provider_arn = provider['Arn']
        metadata = iam_client.get_saml_provider(SAMLProviderArn=provider_arn)
        # Parse the SAML metadata XML to count IDPSSODescriptor elements
        saml_metadata = metadata.get('SAMLMetadataDocument', '')
        idp_count = saml_metadata.count('<IDPSSODescriptor') + saml_metadata.count('<md:IDPSSODescriptor')
        if idp_count == 0:
            idp_count = 1  # At least 1 identity provider per SAML provider object
        if idp_count / serviceQuotaValue > float(threshold) / 100:
            sendQuotaThresholdEvent = True
            resourceListCrossingThreshold.append({
                "resourceARN": provider_arn,
                "usageValue": idp_count
            })
        if idp_count > maxIdpCount:
            maxIdpCount = idp_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxIdpCount), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_F4A5425F(serviceCode, quotaCode, region, threshold):
    """
    Checks Groups per account
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    group_count = 0
    paginator = iam_client.get_paginator('list_groups').paginate()
    for page in paginator:
        group_count += len(page['Groups'])

    logger.info(f"Total IAM groups: {group_count}")
    if group_count / serviceQuotaValue > float(threshold) / 100:
        sendQuotaThresholdEvent = True

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(group_count), "", sendQuotaThresholdEvent)


def L_8E23FFD8(serviceCode, quotaCode, region, threshold):
    """
    Checks Versions per managed policy (max across all customer managed policies)
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    maxVersions = 0
    resourceListCrossingThreshold = []
    paginator = iam_client.get_paginator('list_policies').paginate(Scope='Local')
    for page in paginator:
        for policy in page['Policies']:
            version_count = 0
            versions = iam_client.list_policy_versions(PolicyArn=policy['Arn'])['Versions']
            version_count = len(versions)
            if version_count / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                resourceListCrossingThreshold.append({
                    "resourceARN": policy['Arn'],
                    "usageValue": version_count
                })
            if version_count > maxVersions:
                maxVersions = version_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxVersions), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_ED111B8C(serviceCode, quotaCode, region, threshold):
    """
    Checks Managed policy length (max policy document size across all customer managed policies)
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    maxPolicyLen = 0
    resourceListCrossingThreshold = []
    paginator = iam_client.get_paginator('list_policies').paginate(Scope='Local')
    for page in paginator:
        for policy in page['Policies']:
            policy_version = iam_client.get_policy_version(
                PolicyArn=policy['Arn'],
                VersionId=policy['DefaultVersionId']
            )
            import urllib.parse
            policy_doc = urllib.parse.unquote(json.dumps(policy_version['PolicyVersion']['Document']))
            policy_len = len(policy_doc)
            if policy_len / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                resourceListCrossingThreshold.append({
                    "resourceARN": policy['Arn'],
                    "usageValue": policy_len
                })
            if policy_len > maxPolicyLen:
                maxPolicyLen = policy_len

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxPolicyLen), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)



def L_6E65F664(serviceCode, quotaCode, region, threshold):
    """
    Checks Instance profiles per account
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    profile_count = 0
    paginator = iam_client.get_paginator('list_instance_profiles').paginate()
    for page in paginator:
        profile_count += len(page['InstanceProfiles'])

    logger.info(f"Total instance profiles: {profile_count}")
    if profile_count / serviceQuotaValue > float(threshold) / 100:
        sendQuotaThresholdEvent = True

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(profile_count), "", sendQuotaThresholdEvent)


def L_4019AD8B(serviceCode, quotaCode, region, threshold):
    """
    Checks Managed policies per user (max across all users)
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    maxPolicies = 0
    resourceListCrossingThreshold = []
    paginator = iam_client.get_paginator('list_users').paginate()
    for page in paginator:
        for user in page['Users']:
            attached = iam_client.list_attached_user_policies(UserName=user['UserName'])['AttachedPolicies']
            policy_count = len(attached)
            if policy_count / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                resourceListCrossingThreshold.append({
                    "resourceARN": user['Arn'],
                    "usageValue": policy_count
                })
            if policy_count > maxPolicies:
                maxPolicies = policy_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxPolicies), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_B39FB15B(serviceCode, quotaCode, region, threshold):
    """
    Checks Tags per role (max tags across all roles)
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    maxTags = 0
    resourceListCrossingThreshold = []
    paginator = iam_client.get_paginator('list_roles').paginate()
    for page in paginator:
        for role in page['Roles']:
            tags_response = iam_client.list_role_tags(RoleName=role['RoleName'])
            tag_count = len(tags_response['Tags'])
            if tag_count / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                resourceListCrossingThreshold.append({
                    "resourceARN": role['Arn'],
                    "usageValue": tag_count
                })
            if tag_count > maxTags:
                maxTags = tag_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxTags), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_FE177D64(serviceCode, quotaCode, region, threshold):
    """
    Checks Roles per account
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    role_count = 0
    paginator = iam_client.get_paginator('list_roles').paginate()
    for page in paginator:
        role_count += len(page['Roles'])

    logger.info(f"Total IAM roles: {role_count}")
    if role_count / serviceQuotaValue > float(threshold) / 100:
        sendQuotaThresholdEvent = True

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(role_count), "", sendQuotaThresholdEvent)



def L_F1176D35(serviceCode, quotaCode, region, threshold):
    """
    Checks SSH Public keys per user (max across all users)
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    maxKeys = 0
    resourceListCrossingThreshold = []
    paginator = iam_client.get_paginator('list_users').paginate()
    for page in paginator:
        for user in page['Users']:
            ssh_keys = iam_client.list_ssh_public_keys(UserName=user['UserName'])['SSHPublicKeys']
            key_count = len(ssh_keys)
            if key_count / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                resourceListCrossingThreshold.append({
                    "resourceARN": user['Arn'],
                    "usageValue": key_count
                })
            if key_count > maxKeys:
                maxKeys = key_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxKeys), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_C4DF001E(serviceCode, quotaCode, region, threshold):
    """
    Checks Keys per SAML provider (max across all SAML providers)
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    maxKeys = 0
    resourceListCrossingThreshold = []
    saml_providers = iam_client.list_saml_providers()['SAMLProviderList']
    for provider in saml_providers:
        provider_arn = provider['Arn']
        metadata = iam_client.get_saml_provider(SAMLProviderArn=provider_arn)
        saml_metadata = metadata.get('SAMLMetadataDocument', '')
        # Count signing keys (KeyDescriptor elements) in the SAML metadata
        key_count = saml_metadata.count('<KeyDescriptor') + saml_metadata.count('<md:KeyDescriptor')
        if key_count == 0 and saml_metadata:
            key_count = 1  # At least 1 key if metadata exists
        if key_count / serviceQuotaValue > float(threshold) / 100:
            sendQuotaThresholdEvent = True
            resourceListCrossingThreshold.append({
                "resourceARN": provider_arn,
                "usageValue": key_count
            })
        if key_count > maxKeys:
            maxKeys = key_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxKeys), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_E95E4862(serviceCode, quotaCode, region, threshold):
    """
    Checks Customer managed policies per account
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    policy_count = 0
    paginator = iam_client.get_paginator('list_policies').paginate(Scope='Local')
    for page in paginator:
        policy_count += len(page['Policies'])

    logger.info(f"Total customer managed policies: {policy_count}")
    if policy_count / serviceQuotaValue > float(threshold) / 100:
        sendQuotaThresholdEvent = True

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(policy_count), "", sendQuotaThresholdEvent)


def L_DB618D39(serviceCode, quotaCode, region, threshold):
    """
    Checks SAML providers per account
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    saml_providers = iam_client.list_saml_providers()['SAMLProviderList']
    provider_count = len(saml_providers)

    logger.info(f"Total SAML providers: {provider_count}")
    if provider_count / serviceQuotaValue > float(threshold) / 100:
        sendQuotaThresholdEvent = True

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(provider_count), "", sendQuotaThresholdEvent)



def L_384571C4(serviceCode, quotaCode, region, threshold):
    """
    Checks Managed policies per group (max across all groups)
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    maxPolicies = 0
    resourceListCrossingThreshold = []
    paginator = iam_client.get_paginator('list_groups').paginate()
    for page in paginator:
        for group in page['Groups']:
            attached = iam_client.list_attached_group_policies(GroupName=group['GroupName'])['AttachedPolicies']
            policy_count = len(attached)
            if policy_count / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                resourceListCrossingThreshold.append({
                    "resourceARN": group['Arn'],
                    "usageValue": policy_count
                })
            if policy_count > maxPolicies:
                maxPolicies = policy_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxPolicies), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_8758042E(serviceCode, quotaCode, region, threshold):
    """
    Checks Access keys per user (max across all users)
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    maxKeys = 0
    resourceListCrossingThreshold = []
    paginator = iam_client.get_paginator('list_users').paginate()
    for page in paginator:
        for user in page['Users']:
            access_keys = iam_client.list_access_keys(UserName=user['UserName'])['AccessKeyMetadata']
            key_count = len(access_keys)
            if key_count / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                resourceListCrossingThreshold.append({
                    "resourceARN": user['Arn'],
                    "usageValue": key_count
                })
            if key_count > maxKeys:
                maxKeys = key_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxKeys), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_7A1621EC(serviceCode, quotaCode, region, threshold):
    """
    Checks IAM groups per user (max across all users)
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    maxGroups = 0
    resourceListCrossingThreshold = []
    paginator = iam_client.get_paginator('list_users').paginate()
    for page in paginator:
        for user in page['Users']:
            groups = iam_client.list_groups_for_user(UserName=user['UserName'])['Groups']
            group_count = len(groups)
            if group_count / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                resourceListCrossingThreshold.append({
                    "resourceARN": user['Arn'],
                    "usageValue": group_count
                })
            if group_count > maxGroups:
                maxGroups = group_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxGroups), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_858F3967(serviceCode, quotaCode, region, threshold):
    """
    Checks OpenId connect providers per account
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    oidc_providers = iam_client.list_open_id_connect_providers()['OpenIDConnectProviderList']
    provider_count = len(oidc_providers)

    logger.info(f"Total OIDC providers: {provider_count}")
    if provider_count / serviceQuotaValue > float(threshold) / 100:
        sendQuotaThresholdEvent = True

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(provider_count), "", sendQuotaThresholdEvent)


def L_19F2CF71(serviceCode, quotaCode, region, threshold):
    """
    Checks MFA devices per user (max across all users)
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    maxMfa = 0
    resourceListCrossingThreshold = []
    paginator = iam_client.get_paginator('list_users').paginate()
    for page in paginator:
        for user in page['Users']:
            mfa_devices = iam_client.list_mfa_devices(UserName=user['UserName'])['MFADevices']
            mfa_count = len(mfa_devices)
            if mfa_count / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                resourceListCrossingThreshold.append({
                    "resourceARN": user['Arn'],
                    "usageValue": mfa_count
                })
            if mfa_count > maxMfa:
                maxMfa = mfa_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxMfa), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_76C48054(serviceCode, quotaCode, region, threshold):
    """
    Checks Signing certificates per user (max across all users)
    """
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas')
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    maxCerts = 0
    resourceListCrossingThreshold = []
    paginator = iam_client.get_paginator('list_users').paginate()
    for page in paginator:
        for user in page['Users']:
            certs = iam_client.list_signing_certificates(UserName=user['UserName'])['Certificates']
            cert_count = len(certs)
            if cert_count / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                resourceListCrossingThreshold.append({
                    "resourceARN": user['Arn'],
                    "usageValue": cert_count
                })
            if cert_count > maxCerts:
                maxCerts = cert_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxCerts), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)
