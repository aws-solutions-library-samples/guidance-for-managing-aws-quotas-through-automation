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

# Create a temporary associative array file for usage data
# quota_usage.csv columns: QuotaCode,ServiceCode,Region,LimitValue,UsageValue,ResourceList,Timestamp
TEMP_USAGE_MAP=$(mktemp)

tail -n +2 "$QUOTA_USAGE_FILE" | while IFS=',' read -r quota_code service_code region limit_value usage_value resource_list timestamp; do
    quota_code=$(echo "$quota_code" | tr -d '"')
    service_code=$(echo "$service_code" | tr -d '"')
    region=$(echo "$region" | tr -d '"')
    usage_value=$(echo "$usage_value" | tr -d '"')

    key="${quota_code}|${service_code}|${region}"
    echo "$key=$usage_value" >> "$TEMP_USAGE_MAP"
done

# Write header to output file
head -n 1 "$SERVICE_QUOTAS_FILE" > "$OUTPUT_FILE"

# service_quotas CSV columns:
# accountId,region,serviceCode,quotaCode,quotaName,quotaValue,defaultValue,adjustable,usageValue,usagePct
TEMP_UPDATED_COUNT=$(mktemp)
echo "0" > "$TEMP_UPDATED_COUNT"

tail -n +2 "$SERVICE_QUOTAS_FILE" | while IFS=',' read -r account_id region service_code quota_code quota_name quota_value default_value adjustable usage_value usage_pct; do
    account_id=$(echo "$account_id" | tr -d '"')
    region=$(echo "$region" | tr -d '"')
    service_code=$(echo "$service_code" | tr -d '"')
    quota_code=$(echo "$quota_code" | tr -d '"')
    quota_name=$(echo "$quota_name" | tr -d '"')
    quota_value=$(echo "$quota_value" | tr -d '"')
    default_value=$(echo "$default_value" | tr -d '"')
    adjustable=$(echo "$adjustable" | tr -d '"')
    usage_value=$(echo "$usage_value" | tr -d '"')
    usage_pct=$(echo "$usage_pct" | tr -d '"')

    key="${quota_code}|${service_code}|${region}"
    lookup=$(grep "^${key}=" "$TEMP_USAGE_MAP" 2>/dev/null | head -n 1 | cut -d'=' -f2)

    if [ -n "$lookup" ]; then
        usage_value="$lookup"
        # Recalculate usage percentage
        if [ -n "$quota_value" ] && [ "$quota_value" != "0" ] && [ "$quota_value" != "0.0" ]; then
            usage_pct=$(echo "scale=2; $usage_value / $quota_value * 100" | bc 2>/dev/null || echo "")
        else
            usage_pct=""
        fi
        count=$(cat "$TEMP_UPDATED_COUNT")
        echo "$((count + 1))" > "$TEMP_UPDATED_COUNT"
    fi

    echo "\"$account_id\",\"$region\",\"$service_code\",\"$quota_code\",\"$quota_name\",\"$quota_value\",\"$default_value\",\"$adjustable\",\"$usage_value\",\"$usage_pct\"" >> "$OUTPUT_FILE"
done

# Clean up
updated=$(cat "$TEMP_UPDATED_COUNT")
rm -f "$TEMP_USAGE_MAP"
rm -f "$TEMP_UPDATED_COUNT"

echo ""
echo "Merge complete!"
echo "Output saved to: $OUTPUT_FILE"
echo ""
echo "Summary:"
total_rows=$(($(wc -l < "$OUTPUT_FILE") - 1))
echo "  Total quotas processed: $total_rows"
echo "  Quotas updated with usage data: $updated"
