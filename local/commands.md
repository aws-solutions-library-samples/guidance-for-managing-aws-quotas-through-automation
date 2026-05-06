# Local Development Guide

This directory contains scripts for running QuotaGuard locally against your AWS account. Use these tools to test quota checks, retrieve service quota baselines, and generate usage reports without deploying the full Lambda stack.

## Prerequisites

- Python 3.12+
- AWS CLI v2 configured with valid credentials
- `jq` and `bc` installed (used by shell scripts)

## Initial Setup

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

> **Note:** The only runtime dependency is `boto3`. Your AWS credentials must be configured via `aws configure`, environment variables, or an IAM instance profile.

---

## Running Quota Usage Checks

The main entry point is `app.py`, which reads `config/QuotaList.json` and executes each quota check function defined in `aws_quotas.py`. Results are written to a local CSV file.

### Basic Usage

```bash
python app.py --aws-region us-east-1
```

### Multi-Region

```bash
python app.py --aws-region us-east-1 --region-list us-east-1,us-west-2,eu-west-1
```

### Command-Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--aws-region` | Primary AWS region for global quota checks | `AWS_REGION` env var or `us-east-1` |
| `--region-list` | Comma-separated list of regions to check regional quotas | `REGION_LIST` env var or the value of `--aws-region` |

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_REGION` | Fallback region if `--aws-region` is not provided | `us-east-1` |
| `REGION_LIST` | Fallback region list if `--region-list` is not provided | _(empty — uses single region)_ |
| `QUOTA_CSV_PATH` | Output path for the quota usage CSV | `quota_usage.csv` |

### Output

Results are written to `quota_usage.csv` (or the path specified by `QUOTA_CSV_PATH`) with the following columns:

```
QuotaCode, ServiceCode, Region, LimitValue, UsageValue, ResourceList, Timestamp
```

### Example with Logging

```bash
python app.py --aws-region us-west-2 --region-list us-west-2,us-east-1 > qusage.log 2>&1
```

---

## Retrieving Service Quota Baselines

The `get_service_quotas.sh` script calls the AWS Service Quotas API to export your account's applied quota values and (where available) current usage via CloudWatch metrics.

### Usage

```bash
./get_service_quotas.sh "<comma-separated-services>" <region>
```

### Examples

```bash
# Single service
./get_service_quotas.sh "ec2" us-east-1

# Multiple services
./get_service_quotas.sh "ec2,vpc,s3,iam,ebs,elasticloadbalancing" us-west-2

# All commonly monitored services
./get_service_quotas.sh "autoscaling,bedrock,dynamodb,ebs,ec2,ecr,elasticache,elasticloadbalancing,elasticmapreduce,es,events,iam,kms,monitoring,rds,r53,s3,ses,sns,ssm,transfer,vpc" us-west-2
```

### Output

Generates a timestamped CSV file (e.g., `service_quotas_20260331_134208.csv`) with columns:

```
accountId, region, serviceCode, quotaCode, quotaName, quotaValue, defaultValue, adjustable, usageValue, usagePct
```

---

## Merging Quota Data

The `merge_quota_usage.sh` script combines the Service Quotas baseline data with the custom usage data produced by `app.py`. This gives you a single view of all quotas with their current usage percentages.

### Usage

```bash
./merge_quota_usage.sh <service_quotas_csv> [quota_usage_csv]
```

### Examples

```bash
# Merge with default quota_usage.csv
./merge_quota_usage.sh service_quotas_20260331_134208.csv

# Specify both files explicitly
./merge_quota_usage.sh service_quotas_20260331_134208.csv quota_usage.csv
```

### Output

Generates a timestamped merged CSV file (e.g., `merged_quotas_20260331_140401.csv`) that updates the `usageValue` and `usagePct` columns from the service quotas file with the custom-calculated values from `quota_usage.csv`.

---

## Typical Workflow

A complete local run looks like this:

```bash
# 1. Activate virtual environment
source .venv/bin/activate

# 2. Retrieve baseline quotas from AWS Service Quotas API
./get_service_quotas.sh "ec2,vpc,s3,iam,ebs,elasticloadbalancing,es" us-west-2

# 3. Run custom quota usage checks (for quotas not covered by Service Quotas)
python app.py --aws-region us-west-2 --region-list us-west-2,us-east-1

# 4. Merge the two datasets into a single report
./merge_quota_usage.sh service_quotas_20260331_134208.csv quota_usage.csv
```

The merged CSV provides a comprehensive view of your account's quota utilization, combining both AWS-native usage metrics and custom-calculated values for quotas that lack built-in tracking.

---

## File Reference

| File | Purpose |
|------|---------|
| `app.py` | Local entry point — reads config and runs quota checks |
| `aws_quotas.py` | Core quota checking logic (shared with Lambda) |
| `quota_update_csv.py` | Writes quota results to CSV (local-only output adapter) |
| `quota_update_dynamo.py` | Writes quota results to DynamoDB (used by Lambda) |
| `get_service_quotas.sh` | Fetches baseline quotas from AWS Service Quotas API |
| `merge_quota_usage.sh` | Merges baseline and custom usage data into one report |
| `requirements.txt` | Python dependencies (`boto3`) |

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `NoCredentialError` | Run `aws configure` or export `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` |
| `Quota not implemented: L_XXXXXXXX` | The quota code exists in `QuotaList.json` but has no matching function in `aws_quotas.py`. Add the implementation or remove the entry. |
| `get_service_quotas.sh` fails with date error | On macOS, the script uses `date -v-5M`. On Linux, it falls back to `date -d '5 minutes ago'`. Ensure you're running on a supported platform. |
| Empty `usageValue` in merged CSV | The quota either lacks a CloudWatch usage metric or the custom check isn't implemented yet. |
