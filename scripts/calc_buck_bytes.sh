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

BUCKET_LIST="where to store list of buckets"
REQ_PAYS_LIST="where to store requester-pays list"

if [ -f "${HOME}/.bash_profile" ]; then
    source ${HOME}/.bash_profile
fi

# Private, local values for above variables set here:

ENV_FILE="./calc_buck_bytes-SetEnv.sh"

if [ -f "${ENV_FILE}" ]; then
    source "${ENV_FILE}"
fi

#
# Generate lists of the usage and storage files in the bucket:
#

gsutil ls -p canceridc-data > ${BUCKET_LIST}
cat ${BUCKET_LIST}

#
# Get the column header line into the top of the file:
#

echo "Public bucket" > ${REQ_PAYS_LIST}
while IFS= read -r LINE
do
  echo ${LINE}
  FULL_OUT=`gsutil ls -L -b ${LINE}`
  echo ${FULL_OUT}
  REQ_PAYS=`echo ${FULL_OUT} | sed 's/.* Requester Pays enabled: //' | sed 's//" grep "True"`
  if [ ! -z "${REQ_PAYS}" ]; then
      echo "${LINE}" >> ${REQ_PAYS_LIST}
      echo ${LINE} "is public"
  fi
done < ${BUCKET_LIST}

  #gsutil cp ${COMBINED_USAGE} ${USAGE_BQ_BUCK}
  #bq load --source_format=CSV --field_delimiter=',' --skip_leading_rows=1 ${USAGE_BQ_TABLE} ${USAGE_BQ_BUCK}
  #rm -f ${COMBINED_USAGE}



