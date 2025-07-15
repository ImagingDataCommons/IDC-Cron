mkdir ./json
mkdir ./txt

# AppEngine-module specific app.yaml
gsutil cp "gs://${DEPLOYMENT_BUCKET}/${CRON_APP_YAML}" ./app.yaml

# Print module and version of app.yaml
grep '^service:' ./app.yaml

# Configuration file (env vars not used in cron)
gsutil cp "gs://${DEPLOYMENT_BUCKET}/${IDC_CRON_CONFIG}" ./config.txt

# Production only - buckets to skip
#if [[ ${TIER} == "PROD" ]]; then
#    gsutil cp "${ACL_SKIP_BUCKETS_JSON_FILE_GCS_PATH}" ./
#fi

# Pack staged files for caching
echo "Packing JSON and text files for caching into deployment..."
cp --verbose *.json ./json
cp --verbose *.txt ./txt
