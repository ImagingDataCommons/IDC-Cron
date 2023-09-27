#!/usr/bin/env bash

USER_AND_MACHINE="your_name@machine"
ZONE="your_machine_zone"
PROJECT="your_project"

if [ -f "${HOME}/.bash_profile" ]; then
    source ${HOME}/.bash_profile
fi

# Private, local values for above variables set here:

ENV_FILE="./upload_for_test-SetEnv.sh"

if [ -f "${ENV_FILE}" ]; then
    source "${ENV_FILE}"
fi

cd ../tasks
gcloud compute scp bucket_access_to_bq.py "${USER_AND_MACHINE}:" --zone "${ZONE}"  --project "${PROJECT}"
gcloud compute scp proxy_usage_processing.py "${USER_AND_MACHINE}:" --zone "${ZONE}"  --project ""${PROJECT}""
