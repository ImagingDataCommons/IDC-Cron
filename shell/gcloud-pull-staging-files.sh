mkdir ./json
mkdir ./txt

# These are project-level YAMLs which are loaded from the production bucket
gsutil cp "gs://${DEPLOYMENT_BUCKET}/${PROJECT_CRON_YAML}" ./cron.yaml

# AppEngine-module specific app.yaml
gsutil cp "gs://${DEPLOYMENT_BUCKET}/${CRON_APP_YAML}" ./app.yaml

# Print module and version of app.yaml
grep '^service:' ./app.yaml

# Cron runtime key
gsutil cp "gs://${DEPLOYMENT_BUCKET}/${CRON_RUNTIME_SA_KEY}" ./privatekey.json

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
