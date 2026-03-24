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
    Generic helper for Bedrock ApplyGuardrail text-units-per-second quotas.
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
# NOTE (BUG): These functions use incorrect parameter order (serviceCode, quotaCode, region, threshold)
# instead of the standard (serviceCode, quotaCode, threshold, region).
# Also: iam_client and sq_client are created without region_name (IAM is global but
# service-quotas requires a region). Both issues to be fixed in a future bug-fix pass.
# ============================================================================

def L_F55AF5E4(serviceCode, quotaCode, region, threshold):
    """Checks Users per account"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
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
    """Checks Tags per user (max tags across all users)"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
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
                resourceListCrossingThreshold.append({"resourceARN": user['Arn'], "usageValue": tag_count})
            if tag_count > maxTags:
                maxTags = tag_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxTags), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_C07B4B0D(serviceCode, quotaCode, region, threshold):
    """Checks Role trust policy length (max across all roles)"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    maxPolicyLen = 0
    resourceListCrossingThreshold = []
    paginator = iam_client.get_paginator('list_roles').paginate()
    for page in paginator:
        for role in page['Roles']:
            policy_doc = json.dumps(role.get('AssumeRolePolicyDocument', {}))
            policy_len = len(policy_doc)
            if policy_len / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                resourceListCrossingThreshold.append({"resourceARN": role['Arn'], "usageValue": policy_len})
            if policy_len > maxPolicyLen:
                maxPolicyLen = policy_len

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxPolicyLen), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_3AD47CAE(serviceCode, quotaCode, region, threshold):
    """Checks Identity providers per IAM SAML provider object"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    maxIdpCount = 0
    resourceListCrossingThreshold = []
    saml_providers = iam_client.list_saml_providers()['SAMLProviderList']
    for provider in saml_providers:
        provider_arn = provider['Arn']
        metadata = iam_client.get_saml_provider(SAMLProviderArn=provider_arn)
        saml_metadata = metadata.get('SAMLMetadataDocument', '')
        idp_count = saml_metadata.count('<IDPSSODescriptor') + saml_metadata.count('<md:IDPSSODescriptor')
        if idp_count == 0:
            idp_count = 1
        if idp_count / serviceQuotaValue > float(threshold) / 100:
            sendQuotaThresholdEvent = True
            resourceListCrossingThreshold.append({"resourceARN": provider_arn, "usageValue": idp_count})
        if idp_count > maxIdpCount:
            maxIdpCount = idp_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxIdpCount), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_F4A5425F(serviceCode, quotaCode, region, threshold):
    """Checks Groups per account"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
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
    """Checks Versions per managed policy (max across all customer managed policies)"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    maxVersions = 0
    resourceListCrossingThreshold = []
    paginator = iam_client.get_paginator('list_policies').paginate(Scope='Local')
    for page in paginator:
        for policy in page['Policies']:
            versions = iam_client.list_policy_versions(PolicyArn=policy['Arn'])['Versions']
            version_count = len(versions)
            if version_count / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                resourceListCrossingThreshold.append({"resourceARN": policy['Arn'], "usageValue": version_count})
            if version_count > maxVersions:
                maxVersions = version_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxVersions), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_ED111B8C(serviceCode, quotaCode, region, threshold):
    """Checks Managed policy length (max policy document size across all customer managed policies)"""
    import urllib.parse
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
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
            policy_doc = urllib.parse.unquote(json.dumps(policy_version['PolicyVersion']['Document']))
            policy_len = len(policy_doc)
            if policy_len / serviceQuotaValue > float(threshold) / 100:
                sendQuotaThresholdEvent = True
                resourceListCrossingThreshold.append({"resourceARN": policy['Arn'], "usageValue": policy_len})
            if policy_len > maxPolicyLen:
                maxPolicyLen = policy_len

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxPolicyLen), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_6E65F664(serviceCode, quotaCode, region, threshold):
    """Checks Instance profiles per account"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
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
    """Checks Managed policies per user (max across all users)"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
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
                resourceListCrossingThreshold.append({"resourceARN": user['Arn'], "usageValue": policy_count})
            if policy_count > maxPolicies:
                maxPolicies = policy_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxPolicies), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_B39FB15B(serviceCode, quotaCode, region, threshold):
    """Checks Tags per role (max tags across all roles)"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
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
                resourceListCrossingThreshold.append({"resourceARN": role['Arn'], "usageValue": tag_count})
            if tag_count > maxTags:
                maxTags = tag_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxTags), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_FE177D64(serviceCode, quotaCode, region, threshold):
    """Checks Roles per account"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
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
    """Checks SSH Public keys per user (max across all users)"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
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
                resourceListCrossingThreshold.append({"resourceARN": user['Arn'], "usageValue": key_count})
            if key_count > maxKeys:
                maxKeys = key_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxKeys), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_C4DF001E(serviceCode, quotaCode, region, threshold):
    """Checks Keys per SAML provider (max across all SAML providers)"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    maxKeys = 0
    resourceListCrossingThreshold = []
    saml_providers = iam_client.list_saml_providers()['SAMLProviderList']
    for provider in saml_providers:
        provider_arn = provider['Arn']
        metadata = iam_client.get_saml_provider(SAMLProviderArn=provider_arn)
        saml_metadata = metadata.get('SAMLMetadataDocument', '')
        key_count = saml_metadata.count('<KeyDescriptor') + saml_metadata.count('<md:KeyDescriptor')
        if key_count == 0 and saml_metadata:
            key_count = 1
        if key_count / serviceQuotaValue > float(threshold) / 100:
            sendQuotaThresholdEvent = True
            resourceListCrossingThreshold.append({"resourceARN": provider_arn, "usageValue": key_count})
        if key_count > maxKeys:
            maxKeys = key_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxKeys), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_E95E4862(serviceCode, quotaCode, region, threshold):
    """Checks Customer managed policies per account"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
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
    """Checks SAML providers per account"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    saml_providers = iam_client.list_saml_providers()['SAMLProviderList']
    provider_count = len(saml_providers)

    logger.info(f"Total SAML providers: {provider_count}")
    if provider_count / serviceQuotaValue > float(threshold) / 100:
        sendQuotaThresholdEvent = True

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(provider_count), "", sendQuotaThresholdEvent)


def L_384571C4(serviceCode, quotaCode, region, threshold):
    """Checks Managed policies per group (max across all groups)"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
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
                resourceListCrossingThreshold.append({"resourceARN": group['Arn'], "usageValue": policy_count})
            if policy_count > maxPolicies:
                maxPolicies = policy_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxPolicies), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_8758042E(serviceCode, quotaCode, region, threshold):
    """Checks Access keys per user (max across all users)"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
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
                resourceListCrossingThreshold.append({"resourceARN": user['Arn'], "usageValue": key_count})
            if key_count > maxKeys:
                maxKeys = key_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxKeys), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_7A1621EC(serviceCode, quotaCode, region, threshold):
    """Checks IAM groups per user (max across all users)"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
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
                resourceListCrossingThreshold.append({"resourceARN": user['Arn'], "usageValue": group_count})
            if group_count > maxGroups:
                maxGroups = group_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxGroups), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_858F3967(serviceCode, quotaCode, region, threshold):
    """Checks OpenId connect providers per account"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
    serviceQuota = sq_client.get_service_quota(ServiceCode=serviceCode, QuotaCode=quotaCode)
    serviceQuotaValue = serviceQuota['Quota']['Value']

    oidc_providers = iam_client.list_open_id_connect_providers()['OpenIDConnectProviderList']
    provider_count = len(oidc_providers)

    logger.info(f"Total OIDC providers: {provider_count}")
    if provider_count / serviceQuotaValue > float(threshold) / 100:
        sendQuotaThresholdEvent = True

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(provider_count), "", sendQuotaThresholdEvent)


def L_19F2CF71(serviceCode, quotaCode, region, threshold):
    """Checks MFA devices per user (max across all users)"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
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
                resourceListCrossingThreshold.append({"resourceARN": user['Arn'], "usageValue": mfa_count})
            if mfa_count > maxMfa:
                maxMfa = mfa_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxMfa), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)


def L_76C48054(serviceCode, quotaCode, region, threshold):
    """Checks Signing certificates per user (max across all users)"""
    sendQuotaThresholdEvent = False
    iam_client = boto3.client('iam')
    sq_client = boto3.client('service-quotas', region_name=region)
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
                resourceListCrossingThreshold.append({"resourceARN": user['Arn'], "usageValue": cert_count})
            if cert_count > maxCerts:
                maxCerts = cert_count

    updateQuotaUsage(region, quotaCode, serviceCode, str(serviceQuotaValue), str(maxCerts), json.dumps(resourceListCrossingThreshold), sendQuotaThresholdEvent)
