# Product Overview

QuotaGuard is an AWS quota monitoring and alerting automation solution that tracks service quotas not covered by AWS Service Quotas or Trusted Advisor.

## Purpose

Provides proactive monitoring of AWS service limits to prevent operational disruptions by:
- Monitoring custom AWS service quotas via API calls
- Tracking usage against configurable thresholds
- Sending automated alerts when thresholds are exceeded
- Maintaining centralized visibility across single or multi-account AWS environments

## Deployment Models

- Single Account: Monitors quotas within one AWS account
- Multi-Account: Hub-and-spoke architecture for AWS Organizations with centralized alerting

## Key Features

- Pull-based monitoring model with configurable schedules (default: every 10 minutes)
- JSON-based quota configuration (QuotaList.json)
- DynamoDB storage for quota usage tracking
- EventBridge-based alerting via SNS email notifications
- Supports both regional and global AWS service quotas
- Extensible architecture for adding new quota checks

## Monitored Services

Currently supports 11+ AWS service quotas including:
- VPC (network interfaces, endpoints, NAT gateways, subnets, peering connections)
- EC2 (Client VPN endpoints, Transit Gateway route tables)
- EBS (gp2 volume storage)
- ELB (registered instances per load balancer)
- S3 (bucket count)
- IAM (managed policies per role, server certificates)
- OpenSearch Service (domain count)
