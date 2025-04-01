# Guidance for Managing AWS Quotas Through Automation

## Table of Contents 

1. [Overview](#overview-required)
    - [Cost](#cost)
2. [Prerequisites](#prerequisites-required)
    - [Operating System](#operating-system-required)
    - [AWS account requirements](#aws-account-requirements)
3. [Deployment Steps](#deployment-steps-required)
4. [Deployment Validation](#deployment-validation-required)
5. [Running the Guidance](#running-the-guidance-required)
6. [Next Steps](#next-steps-required)
7. [Cleanup](#cleanup-required)

## Overview 

Managing AWS service quotas effectively is crucial for maintaining operational continuity and preventing unexpected disruptions to business-critical applications. While AWS provides native solutions like Service Quotas and Trusted Advisor for limit monitoring, organizations often face challenges in implementing comprehensive quota management strategies, particularly for resources not covered by these services. AWS provides a [Quota Monitor solution](https://aws.amazon.com/solutions/implementations/quota-monitor/) which allows customers to manage several AWS service Quotas. However there are quotas which are not currently exposed through the AWS Service Quota service and cannot be managed by these solutions.

This solution in this repository provides an automation to manage quotas which are not captured by above solutions. It uses a flexible, pull-based model that allows customers to monitor Any AWS service quota, if they are not covered by AWS Trusted Advisor or Service Quotas. This solution empowers organizations to:
 
* Create custom quota monitoring templates for any AWS service
* Define and track service-specific limits using AWS API calls
* Implement automated usage monitoring and threshold alerts
* Maintain centralized visibility of all service quotas across their multi-account AWS environment
* Provide information on which resources are crossing the Service Quota threshold boundary

Whether you're managing a growing cloud infrastructure or maintaining large-scale AWS deployments, this solution provides the tools necessary for proactive quota management and operational excellence.

The solutions can be deployed in a single account or accross multiple accounts within an organization:

### Single Account Deployment

![SingleAccountDeployment](./images/QuotaGuard-single_account-ver02.png)

The Single Account deployment model monitors service quotas within one AWS account. The solutions works as follows,

1. **Scheduled  Monitoring**: An **EventBridge  rule** triggers the Lambda function (QuotaGuardLambda) every 10  minutes. The  Lambda function reads the configuration file (QuotaList.json) from the  specified S3 bucket to identify the quotas to monitor and their  thresholds.
2. **Quota  Data Retrieval**:  The  Lambda function queries AWS Service Quotas API to  fetch current quota usage for the specified services and regions.
3. **Threshold  Evaluation**: The  Lambda function compares the retrieved quota usage against the thresholds and if any quota exceeds its threshold, the Lambda function generates a custom  event (quota-threshold-event).
4. **Alert  Generation**: The  custom event is sent to EventBridge, which matches it against a  notification rule (QuotaGuardEventNotificationRule). The  matched event is routed to an SNS topic (QuotaThresholdSnsTopic).
5. **Administrator  Notification**: The  SNS topic sends an email notification to the administrator's email  address provided during deployment. The  email contains details about the breached quota, including service name,  region, and usage percentage.
6. **Data  Storage**: The  Lambda function stores quota usage data in a DynamoDB table (QuotaGuardDDBTable)  for tracking and analysis.

### Multi-Account Deployment

![MultiAccountDeployment](./images/QuotaGuard-multi_account-ver02.png)

The Multi-Account model uses a hub-and-spoke architecture to monitor quotas across multiple AWS accounts in an organization. The solutions works as follows, 

**Spoke (or member) Account Workflow**

1. **Local  Quota Monitoring**: In  each spoke account, an EventBridge rule triggers a Lambda function (QuotaGuardLambda)  every 10 minutes. The  Lambda function reads QuotaList.json from S3 to identify quotas and  thresholds for monitoring.
2. **Quota  Data Retrieval**: The  Lambda function queries AWS Service Quotas API to  fetch current quota usage for local resources.
3. **Threshold  Evaluation**: The  Lambda function compares current usage and If any quota exceeds its threshold, it generates a custom event (quota-threshold-event).
4. **Event  Forwarding to Hub**: Using  a cross-account IAM role, the custom event is sent to the central  EventBus in the hub account (or management account) via EventBridge.
5. **Data  Storage**: Quota  usage data is stored locally in a DynamoDB table (QuotaGuardDDBTable) for  tracking purposes.

**Hub Account Workflow**

6. **Centralized  Event Aggregation**: The  hub account's EventBridge receives quota-threshold-event events from all  spoke accounts. A  policy on the EventBus ensures only events from accounts within the same  AWS Organization are accepted.
7. **Notification  Rule Matching**: Events  are matched against a notification rule (QuotaGuardEventRule) that routes  them to an SNS topic (QuotaThresholdSnsTopic).
8. **Administrator  Notification**: The  SNS topic sends notifications to administrators with details  about breached quotas across all accounts. Notifications  include information such as account ID, service name, region, and usage  percentage.


### Cost 

_You are responsible for the cost of the AWS services used while running this Guidance. As of <month> <year>, the cost for running this Guidance with the default settings in the <Default AWS Region (Most likely will be US East (N. Virginia)) > is approximately $<n.nn> per month for processing ( <nnnnn> records )._

_We recommend creating a [Budget](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html) through [AWS Cost Explorer](https://aws.amazon.com/aws-cost-management/aws-cost-explorer/) to help manage costs. Prices are subject to change. For full details, refer to the pricing webpage for each AWS service used in this Guidance._

The cost of running this guidance per region per account is $6.45 per month

### Sample Cost Table

The following table provides a sample cost breakdown for deploying this Guidance with the default parameters in the US East (N. Virginia) Region for one month per each account

| AWS service  | Dimensions | Cost [USD] |
| ----------- | ------------ | ------------ |
| Amazon EventBridge | 259,200  | $ 0.26 |
| AWS Lambda | 259,200 invokations | $ 2.75 |
| Amazon DynamoDB | 5 million writes / 1 million reads | $ 3.44 |

## Prerequisites

### Operating System

These deployment instructions are optimized to best work on macOS, Linux or Windows based operating systems with a bash shell and the [aws cli](https://aws.amazon.com/cli/). Deployment in another OS may require additional steps.

### AWS account requirements

1. A S3 bucket to store the guidance artifacts (Lambda function code, configuration and deployment files)
2. Multi-account deployments require the following resource policy applied to the S3 bucket where files will be stored. Replace ORG_ID for your organization identifier:
  ```{```  
```    "Version": "2012-10-17",```  
```    "Statement": [{```  
```        "Sid": "AllowGetObject",```  
```        "Principal": {```  
```            "AWS": "*"```  
```        },```  
```        "Effect": "Allow",```  
```        "Action": "s3:GetObject",```  
```        "Resource": "arn:aws:s3:::amzn-s3-demo-bucket/*",```  
```        "Condition": {```  
```            "StringEquals": {```  
```                "aws:PrincipalOrgID": ["ORG_ID"]```  
```            }```  
```        }```  
```    }]```  
```}``` 

## Deployment Steps

### Single Account Deployment

1. Clone / Copy github Repo (add repo link)
2. Create a S3 bucket for the solution resources and create a folder named "qg-templates" in the bucket
3. Create  your SSO profile as specified in the document “[Configuring IAM Identity Center authentication with the AWS CLI”](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html) upload the resources to this S3 bucket using the below command
4. Use the below command to upload resources to the bucket and deploy the stack

  ```./deploy.sh -h```

  ```Usage: $0 [OPTIONS]```
 
  ```Deploy CloudFormation stack for Quota Guard```

  ```Required Parameters:```

  ```  -p, --profile     AWS CLI profile name```

  ```  -b, --bucket      S3 bucket name for deployment```

  ```  -t, --type        Account type (single or multi)```

  ```  -e, --email       Email address for notifications```

  ```  Example:```

  ```   $0 --profile myprofile --bucket my-bucket-name --type multi --email user@example.com```

  ```   $0 -p myprofile -b my-bucket-name -t single -e user@example.com```


5. Use this CloudFormation template ***quota-guard-single-account.yaml*** from the S3 bucket to deploy the solution. CloudFormation stacks are deployed using the console as explained in the documentation through [console](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-console-create-stack.html) or [CLI.](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-cli-creating-stack.html)

6. Provide  the required parameters -

    * Configfile - JSON Config file name for the configuration.
    * DeploymentBucket - The name of the S3 bucket containing the lambda package and templates.
    * DeploymentBucketPrefix - (qg-templates) - S3 prefix for Lambda package.
    * QuotaThresholdEventNotificationEmail - Email Address of an Admin who will receive notifications of Quota Threshold Exceeded Events.
    * RegionList - List of AWS Regions to monitor quota of resources.
    * ExecutionTimeInCron - Cron Expression to specify the schedule for pulling usage data and performing threshold checks. 

7. Deploy  the stack


### Multi-Account Deployment

Make sure to have followed the [AWS account requirements](#aws-account-requirements) before continuing with these steps.

1. Clone / Copy github Repo (add repo link)
2. Create a S3 bucket for the solution resources and create a folder named "qg-templates" in the bucket
3. Create  your SSO profile as specified in the document “[Configuring IAM Identity Center authentication with the AWS CLI”](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html) upload the resources to this S3 bucket using the below command
4. Use the below command to upload resources to the bucket and deploy the stack

  ```./deploy.sh -h```

  ```Usage: $0 [OPTIONS]```
 
  ```Deploy CloudFormation stack for Quota Guard```

  ```Required Parameters:```

  ```  -p, --profile     AWS CLI profile name```

  ```  -b, --bucket      S3 bucket name for deployment```

  ```  -t, --type        Account type (single or multi)```

  ```  -e, --email       Email address for notifications```

  ```  Example:```

  ```   $0 --profile myprofile --bucket my-bucket-name --type multi --email user@example.com```

  ```   $0 -p myprofile -b my-bucket-name -t single -e user@example.com```

5. Deploy  the Hub Stack in the central account: 
    
    5.1. Use this CloudFormation template ***quota-guard-hub.yaml*** from the S3 bucket to deploy the solution. CloudFormation stacks are deployed using the console as explained in the documentation through [console](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-console-create-stack.html) or [CLI.](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-cli-creating-stack.html)
    
    5.2. Provide  the required parameters - 

   * AWSOrganizationId - Organization Id for your AWS Organizations.
   * ConfigFile - JSON Config file name for the configuration.
   * DeploymentBucket - S3 bucket containing the Lambda package and templates.
   * DeploymentBucketPrefix - The prefix of the S3 bucket containing the Lambda package and templates.
   * OrganizationalUnits - List of OUs for which you want to monitor Quotas.
   * QuotaThresholdEventNotificationEmail - Email Address of an Admin who will receive notifications of Quota Threshold Exceeded Events.
   * RegionList - List of AWS Regions to monitor quota of resources.
   * ExecutionTimeInCron - Cron Expression to specify the schedule for pulling usage data and performing threshold checks.

7. The  Spoke Stack will be automatically deployed to member accounts via  StackSets in provided OrganizationalUnits
 
**Example:**

1. Clone the repo using command ```git clone xxxxxxxxxx```
2. cd to the repo folder ```cd <repo-name>```
3. Install packages in requirements using command ```pip install requirement.txt```
4. Edit content of **file-name** and replace **s3-bucket** with the bucket name in your account.
5. Run this command to deploy the stack ```cdk deploy``` 
6. Capture the domain name created by running this CLI command ```aws apigateway ............```



## Deployment Validation 

### Single account deployment 
* Open CloudFormation console and verify the status of the template with the name starting with quota-guard-single-account.

### Multi-account deployment 
* Open CloudFormation console and verify the status of the template with the name starting with quota-guard-hub.
* In the CloudFormation console, select StackSets and verify the status of the templates in the stack set with the name starting with QuotaGuardSpokeStackSet.

## Running the Guidance 

There is no action needed once the stacks are deployed. The solution will run a lambda function periodically, per account, to check quotas specified in the configuration file


## Next Steps

You can tailor QuotaGuard solution to your needs by: 

* Updating  the QuotaList.json file with additional services or custom thresholds for service limits you want to monitor.
* Modifying  Lambda function code for custom logic or additional integrations for service limits that you want to monitor.
* Adjusting  CloudFormation templates to add resources or change configurations (e.g.,  notification protocols).



## Cleanup 

### Single account

1. Delete the stack from the cloudformation console

### Milti-account account

1. Delete the stacksets from the cloudformation console
2. Delete the stack from the cloudformation console

## Notices 

*Customers are responsible for making their own independent assessment of the information in this Guidance. This Guidance: (a) is for informational purposes only, (b) represents AWS current product offerings and practices, which are subject to change without notice, and (c) does not create any commitments or assurances from AWS and its affiliates, suppliers or licensors. AWS products or services are provided “as is” without warranties, representations, or conditions of any kind, whether express or implied. AWS responsibilities and liabilities to its customers are controlled by AWS agreements, and this Guidance is not part of, nor does it modify, any agreement between AWS and its customers.*


## Authors

* Anandprasanna Gaitonde - anandprg@amazon.com
* Preetam Rebello - preetreb@amazon.com
* Raj Bagwe - rbagwe@amazon.com
* Varun Mehta - varunmra@amazon.com
* Santiago Flores Kanter - sfkanter@amazon.com

