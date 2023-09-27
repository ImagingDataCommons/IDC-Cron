#!/usr/bin/env bash

#
# Copyright 2020, Institute for Systems Biology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Script to set up storage logging on a project
#

if [[ $# -ne 2 ]]; then
    echo "Usage: setup-storage-logging.sh [super-user] [project]"
    exit
fi

USE_ACCOUNT=$1
PROJECT=$2
ACTIVE=$(gcloud auth list 2>/dev/null | grep "^*" | sed 's/\*[ ]*//')
echo "Curently logged in as: " ${ACTIVE}

gsutil ls -b gs://${PROJECT}-storage-logs
if [[ $? -ne 0 ]]; then
  echo "Destination bucket does not exist. Create and add cloud-storage-analytics@google.com with write permissions"
  exit
fi

gcloud auth login ${USE_ACCOUNT}

ALL_BUCKS=$(gsutil ls -p $2)

for BUCK in ${ALL_BUCKS}; do
  AS_NAME=`echo ${BUCK} | grep "appspot.com"`
  ST_NAME=`echo ${BUCK} | grep "web-static-files"`
  if [ -z "${AS_NAME}" ] && [ -z "${ST_NAME}" ]; then
    gsutil logging set on -b gs://${PROJECT}-storage-logs ${BUCK}
    if [ $? -ne 0 ]; then
      exit
    fi
  fi
done

for BUCK in ${ALL_BUCKS}; do
  gsutil logging get ${BUCK}
done

gcloud auth login ${ACTIVE}
gcloud auth revoke ${USE_ACCOUNT}