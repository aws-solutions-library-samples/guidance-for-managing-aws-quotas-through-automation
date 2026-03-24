# Technology Stack

## Runtime & Language

- Python 3.12 (Lambda and local execution)
- Bash shell scripts for deployment automation

## AWS Services

- AWS Lambda: Serverless compute for quota monitoring logic
- Amazon EventBridge: Scheduled triggers (cron) and event routing
- Amazon DynamoDB: Quota usage data storage (PAY_PER_REQUEST billing)
- Amazon SNS: Email notifications for threshold alerts
- Amazon S3: Configuration and deployment artifact storage
- AWS CloudFormation: Infrastructure as Code (IaC) for deployment
- AWS IAM: Role-based access control

## Dependencies

- boto3: AWS SDK for Python (only external dependency)

## Build & Deployment

### Package Lambda Function
```bash
cd lambda-code
cp ../local/aws_quotas.py .
cp ../local/quota_update_dynamo.py .
zip ../packages/quota_guard_1.0.0.zip index.py aws_quotas.py quota_update_dynamo.py tests/*
rm aws_quotas.py quota_update_dynamo.py
cd ..
```

### Deploy Stack
```bash
./deploy.sh --profile <profile> --bucket <bucket-name> --type <single|multi> --email <email>
```

Required parameters:
- `-p, --profile`: AWS CLI profile name
- `-b, --bucket`: S3 bucket for deployment artifacts
- `-t, --type`: Account type (single or multi)
- `-e, --email`: Email for notifications

### Local Testing
```bash
cd local
python app.py
```

Environment variables for local execution:
- `AWS_REGION`: Target AWS region (default: us-east-1)
- `REGION_LIST`: Comma-separated list of regions to monitor
- `QUOTA_CSV_PATH`: Path for CSV output (default: quota_usage.csv)

## CloudFormation Templates

- `quota-guard-single-account.yaml`: Single account deployment
- `quota-guard-hub.yaml`: Multi-account hub (management account)
- `quota-guard-spoke.yaml`: Multi-account spoke (member accounts)

## Configuration

- `config/QuotaList.json`: Quota definitions with ServiceCode, QuotaCode, QuotaAppliedAtLevel (Regional/Global), and Threshold percentage
- Lambda environment variables: SERVICEQUOTA_BUCKET, DDB_TABLE, EVENT_BUS, REGION_LIST, QUOTALIST_FILE

## Testing

Test data located in `lambda-code/tests/` with JSON files for each quota code containing mock AWS API responses.
