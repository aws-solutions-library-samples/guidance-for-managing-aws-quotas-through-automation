#!/bin/bash

# Script to fetch AWS Service Quotas and export to CSV
# Usage: ./get_service_quotas.sh [services] [region]
# Example: ./get_service_quotas.sh "ec2,vpc,s3" us-east-1

# Default values
DEFAULT_SERVICES="ec2"
DEFAULT_REGION="us-west-2"

# Parse arguments
SERVICES="${1:-$DEFAULT_SERVICES}"
REGION="${2:-$DEFAULT_REGION}"

# Convert comma-separated services to array
IFS=',' read -ra SERVICE_ARRAY <<< "$SERVICES"

# Output CSV file
OUTPUT_FILE="service_quotas_$(date +%Y%m%d_%H%M%S).csv"

# Get account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text 2>/dev/null)
if [ -z "$ACCOUNT_ID" ]; then
    echo "Error: Unable to retrieve AWS account ID. Check your credentials."
    exit 1
fi

# Create CSV header
echo "accountId,region,serviceCode,quotaCode,quotaName,quotaValue,defaultValue,adjustable,usageValue,usagePct" > "$OUTPUT_FILE"

echo "Fetching service quotas..."
echo "Account: $ACCOUNT_ID"
echo "Services: ${SERVICE_ARRAY[*]}"
echo "Region: $REGION"
echo "Output file: $OUTPUT_FILE"
echo ""

# Process each service
for service in "${SERVICE_ARRAY[@]}"; do
    echo "Processing service: $service"

    # Get all quotas for the service
    quotas=$(aws service-quotas list-service-quotas \
        --service-code "$service" \
        --region "$REGION" \
        --output json 2>&1)

    exit_code=$?

    if [ $exit_code -ne 0 ]; then
        echo "  ✗ Failed to fetch quotas for $service"
        echo "  Error: $quotas"
        continue
    fi

    # Check if quotas exist
    quota_count=$(echo "$quotas" | jq '.Quotas | length' 2>/dev/null)

    if [ -z "$quota_count" ] || [ "$quota_count" -eq 0 ]; then
        echo "  ✗ No quotas found for $service"
        continue
    fi

    echo "  Found $quota_count quotas"

    # Also fetch default quotas for comparison
    defaults=$(aws service-quotas list-aws-default-service-quotas \
        --service-code "$service" \
        --region "$REGION" \
        --output json 2>/dev/null)

    # Build default value lookup (fallback to empty object if defaults fetch failed)
    default_map=$(echo "$defaults" | jq -r '
        [.Quotas // [] | .[] | {(.QuotaCode): .Value}] | add // {}
    ' 2>/dev/null)
    if [ -z "$default_map" ] || ! echo "$default_map" | jq empty 2>/dev/null; then
        default_map="{}"
    fi

    # Iterate quotas and fetch usage where available
    quota_codes=$(echo "$quotas" | jq -r '.Quotas[].QuotaCode')
    usage_fetched=0

    for qcode in $quota_codes; do
        # Extract quota details from already-fetched data
        row=$(echo "$quotas" | jq -r --arg qc "$qcode" --arg acct "$ACCOUNT_ID" \
            --arg region "$REGION" --argjson def_map "$default_map" '
            .Quotas[] | select(.QuotaCode == $qc) |
            {
                acct: $acct,
                region: $region,
                serviceCode: .ServiceCode,
                quotaCode: .QuotaCode,
                quotaName: .QuotaName,
                value: .Value,
                defaultValue: ($def_map[.QuotaCode] // .Value),
                adjustable: .Adjustable,
                hasUsageMetric: (.UsageMetric != null and .UsageMetric != {})
            }
        ')

        has_usage=$(echo "$row" | jq -r '.hasUsageMetric')
        usage_val=""
        usage_pct=""

        if [ "$has_usage" = "true" ]; then
            # Extract CloudWatch metric info from the quota's UsageMetric field
            metric_info=$(echo "$quotas" | jq -r --arg qc "$qcode" '
                .Quotas[] | select(.QuotaCode == $qc) | .UsageMetric // empty
            ')

            if [ -n "$metric_info" ]; then
                cw_namespace=$(echo "$metric_info" | jq -r '.MetricNamespace // empty')
                cw_metric=$(echo "$metric_info" | jq -r '.MetricName // empty')
                cw_stat=$(echo "$metric_info" | jq -r '.MetricStatisticRecommendation // "Maximum"')
                cw_dimensions=$(echo "$metric_info" | jq -r '
                    [.MetricDimensions | to_entries[] | {"Name": .key, "Value": .value}]
                ')

                if [ -n "$cw_namespace" ] && [ -n "$cw_metric" ]; then
                    end_time=$(date -u +%Y-%m-%dT%H:%M:%SZ)
                    start_time=$(date -u -v-5M +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '5 minutes ago' +%Y-%m-%dT%H:%M:%SZ)

                    cw_result=$(aws cloudwatch get-metric-statistics \
                        --namespace "$cw_namespace" \
                        --metric-name "$cw_metric" \
                        --dimensions "$cw_dimensions" \
                        --start-time "$start_time" \
                        --end-time "$end_time" \
                        --period 300 \
                        --statistics "$cw_stat" \
                        --region "$REGION" \
                        --output json 2>/dev/null)

                    if [ $? -eq 0 ]; then
                        usage_val=$(echo "$cw_result" | jq -r --arg stat "$cw_stat" '
                            .Datapoints | sort_by(.Timestamp) | last | .[$stat] // empty
                        ' 2>/dev/null)
                        if [ -n "$usage_val" ] && [ "$usage_val" != "null" ]; then
                            ((usage_fetched++)) || true
                        fi
                    fi
                fi
            fi
        fi

        # Calculate usage percentage
        if [ -n "$usage_val" ] && [ "$usage_val" != "null" ] && [ "$usage_val" != "" ]; then
            quota_value=$(echo "$row" | jq -r '.value')
            if [ -n "$quota_value" ] && [ "$quota_value" != "0" ]; then
                usage_pct=$(echo "scale=2; $usage_val / $quota_value * 100" | bc 2>/dev/null || echo "")
            fi
        fi

        # Write CSV row
        echo "$row" | jq -r --arg uv "${usage_val:-}" --arg up "${usage_pct:-}" '
            [
                .acct,
                .region,
                .serviceCode,
                .quotaCode,
                .quotaName,
                (.value | tostring),
                (.defaultValue | tostring),
                (if .adjustable then "true" else "false" end),
                $uv,
                $up
            ] | @csv
        ' >> "$OUTPUT_FILE"
    done

    echo "  ✓ Completed $service ($usage_fetched quotas with usage data)"
done

echo ""
echo "Export complete!"
echo "Output saved to: $OUTPUT_FILE"
echo ""
echo "Summary:"
total_rows=$(($(wc -l < "$OUTPUT_FILE") - 1))
echo "  Total quotas exported: $total_rows"
