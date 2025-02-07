#!/bin/bash
function show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Deploy CloudFormation stack for Quota Monitor Extensions"
    echo
    echo "Required Parameters:"
    echo "  -p, --profile     AWS CLI profile name"
    echo "  -b, --bucket      S3 bucket name for deployment"
    echo "  -t, --type        Account type (single or multi)"
    echo "  -e, --email       Email address for notifications"
    echo
    echo "Example:"
    echo "  $0 --profile myprofile --bucket my-bucket-name --type multi --email user@example.com"
    echo "  $0 -p myprofile -b my-bucket-name -t single -e user@example.com"
    exit 1
}
# Check if no arguments provided
if [ $# -eq 0 ]; then
    echo "Error: No arguments provided"
    show_usage
fi

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -p|--profile)
        PROFILE="$2"
        shift
        shift
        ;;
        -b|--bucket)
        BUCKET_NAME="$2"
        shift
        shift
        ;;
        -t|--type)
        ACCOUNT_TYPE="$2"
        shift
        shift
        ;;
        -e|--email)
        EMAIL_ADDRESS="$2"
        shift
        shift
        ;;
        -h|--help)
        show_usage
        ;;
        *)
        echo "Unknown option: $1"
        show_usage
        ;;
    esac
done

# Validate required parameters
if [ -z "$PROFILE" ] || [ -z "$BUCKET_NAME" ] || [ -z "$ACCOUNT_TYPE" ] || [ -z "$EMAIL_ADDRESS" ]; then
    echo "Error: Missing required parameters"
    show_usage
fi

[ -f "packages/quota_extension_1.0.0.zip" ] && rm "packages/quota_extension_1.0.0.zip"
# Create packages directory if it doesn't exist
mkdir -p packages
cd lambda-code
zip ../packages/quota_guard_1.0.0.zip index.py tests/*
cd ..



#Copying files to bucket
aws s3 cp packages/quota_guard_1.0.0.zip s3://$BUCKET_NAME/qg-templates/ --profile $PROFILE
aws s3 cp templates/quota-guard-hub.yaml s3://$BUCKET_NAME/qg-templates/quota-guard-hub.yaml --profile $PROFILE
aws s3 cp templates/quota-guard-single-account.yaml s3://$BUCKET_NAME/qg-templates/ --profile $PROFILE
aws s3 cp templates/quota-guard-spoke.yaml s3://$BUCKET_NAME/qg-templates/ --profile $PROFILE

aws s3 cp config/QuotaList.json s3://$BUCKET_NAME/ --profile $PROFILE



sleep 5
# Select template based on account type
if [ "$ACCOUNT_TYPE" = "multi" ]; then
    TEMPLATE_NAME="quota-guard-hub.yaml"
elif [ "$ACCOUNT_TYPE" = "single" ]; then
    TEMPLATE_NAME="quota-guard-single-account.yaml"
else
    echo "Error: Account type must be 'single' or 'multi'"
    exit 1
fi

# Create CloudFormation stack with selected template
aws cloudformation create-stack \
    --stack-name QMExtensionsStack \
    --template-url https://$BUCKET_NAME.s3.amazonaws.com/qg-templates/$TEMPLATE_NAME \
    --capabilities CAPABILITY_IAM \
    --parameters \
        ParameterKey=LambdaBucket,ParameterValue=$BUCKET_NAME \
        ParameterKey=ConfigBucket,ParameterValue=$BUCKET_NAME \
        ParameterKey=QuotaThresholdEventNotificationEmail,ParameterValue=$EMAIL_ADDRESS \
    --profile $PROFILE
