rm packages/quota_guard_1.0.0.zip
cd lambda-code
zip ../packages/quota_guard_1.0.0.zip index.py
cd ..
aws s3 cp packages/quota_guard_1.0.0.zip s3://vod-anand78/qg-templates/ --profile $1
aws s3 cp templates/quota-guard-hub.yaml s3://vod-anand78/qg-templates/ --profile $1
aws s3 cp templates/quota-guard-single-account.yaml s3://vod-anand78/qg-templates/ --profile $1
aws s3 cp templates/quota-guard-spoke.yaml s3://vod-anand78/qg-templates/ --profile $1

aws s3 cp config/QuotaList.json s3://vod-anand78/ --profile $1
sleep 5
#aws cloudformation create-stack --stack-name QMExtensionsStack --template-url https://vod-anand78.s3.amazonaws.com/qg-templates/quota-management-guard.yaml    --capabilities CAPABILITY_IAM --profile $2
