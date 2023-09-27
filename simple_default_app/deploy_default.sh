#!/usr/bin/env bash


if [ "$#" -ne 1 ]; then
    echo "usage: ./deploy_default.sh project_id"
    exit
fi

PROJECT=$1

gcloud app deploy --verbosity=debug ./app.yaml --quiet --project=${PROJECT}