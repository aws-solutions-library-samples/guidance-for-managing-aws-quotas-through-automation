#!/bin/bash

# Script to merge usage data from quota_usage.csv into service_quotas CSV file
# Usage: ./merge_quota_usage.sh <service_quotas_file> [quota_usage_file]
# Example: ./merge_quota_usage.sh service_quotas_20260304_151234.csv quota_usage.csv

# Check if service quotas file is provided
if [ -z "$1" ]; then
    echo "Error: Service quotas CSV file not provided"
    echo "Usage: $0 <service_quotas_file> [quota_usage_file]"
    echo "Example: $0 service_quotas_20260304_151234.csv quota_usage.csv"
    exit 1
fi

SERVICE_QUOTAS_FILE="$1"
QUOTA_USAGE_FILE="${2:-quota_usage.csv}"

# Check if files exist
if [ ! -f "$SERVICE_QUOTAS_FILE" ]; then
    echo "Error: Service quotas file not found: $SERVICE_QUOTAS_FILE"
    exit 1
fi

if [ ! -f "$QUOTA_USAGE_FILE" ]; then
    echo "Error: Quota usage file not found: $QUOTA_USAGE_FILE"
    exit 1
fi

# Output file
OUTPUT_FILE="merged_quotas_$(date +%Y%m%d_%H%M%S).csv"

echo "Merging quota usage data..."
echo "Service Quotas File: $SERVICE_QUOTAS_FILE"
echo "Quota Usage File: $QUOTA_USAGE_FILE"
echo "Output File: $OUTPUT_FILE"
echo ""

# Use Python for proper CSV parsing (handles commas inside quoted fields)
python3 - "$SERVICE_QUOTAS_FILE" "$QUOTA_USAGE_FILE" "$OUTPUT_FILE" << 'PYEOF'
import csv
import sys

service_quotas_file = sys.argv[1]
quota_usage_file = sys.argv[2]
output_file = sys.argv[3]

# Build lookup from quota_usage.csv
# Columns: QuotaCode,ServiceCode,Region,LimitValue,UsageValue,ResourceList,Timestamp
usage_map = {}
with open(quota_usage_file, newline='') as f:
    reader = csv.reader(f)
    header = next(reader)
    for row in reader:
        if len(row) >= 5:
            quota_code, service_code, region = row[0], row[1], row[2]
            usage_value = row[4]
            key = f"{quota_code}|{service_code}|{region}"
            usage_map[key] = usage_value

# Process service_quotas CSV and merge usage data
# Columns: accountId,region,serviceCode,quotaCode,quotaName,quotaValue,defaultValue,adjustable,usageValue,usagePct
updated_count = 0
total_rows = 0

with open(service_quotas_file, newline='') as infile, open(output_file, 'w', newline='') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)

    header = next(reader)
    writer.writerow(header)

    for row in reader:
        total_rows += 1
        if len(row) < 10:
            # Pad short rows
            row.extend([''] * (10 - len(row)))

        account_id, region, service_code, quota_code = row[0], row[1], row[2], row[3]
        quota_name, quota_value, default_value, adjustable = row[4], row[5], row[6], row[7]
        usage_value, usage_pct = row[8], row[9]

        key = f"{quota_code}|{service_code}|{region}"
        if key in usage_map:
            usage_value = usage_map[key]
            # Recalculate usage percentage
            try:
                qv = float(quota_value)
                if qv > 0:
                    usage_pct = f"{float(usage_value) / qv * 100:.2f}"
                else:
                    usage_pct = ""
            except (ValueError, ZeroDivisionError):
                usage_pct = ""
            updated_count += 1

        writer.writerow([account_id, region, service_code, quota_code, quota_name,
                         quota_value, default_value, adjustable, usage_value, usage_pct])

print(f"RESULT:{total_rows}:{updated_count}")
PYEOF

echo ""
echo "Merge complete!"
echo "Output saved to: $OUTPUT_FILE"
echo ""
echo "Summary:"
total_rows=$(($(wc -l < "$OUTPUT_FILE") - 1))
echo "  Total quotas processed: $total_rows"
