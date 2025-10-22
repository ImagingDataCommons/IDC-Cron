#!/usr/bin/env bash

#
# Copyright 2020-2024, Institute for Systems Biology
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

pull_a_set() {
  FUNC_LOG_LOC=${1}
  FUNC_FILE_CORE=${2}
  FUNC_HEADER_FIELD=${3}
  FUNC_MAX_FILES_PER_RUN=${4}
  FUNC_FILE_LIST=${5}
  FUNC_FLOW_NAME=${6}
  FUNC_COMBINED_PRESORT=${7}
  FUNC_COMBINED=${8}
  FUNC_LOG_LOC_PRE=${9}
  FUNC_LOG_LOC_POST=${10}
  FUNC_BQ_BUCK=${11}
  FUNC_BQ_TABLE=${12}
  FUNC_SCHEMA_FILE=${13}

  #
  # Generate lists of the records files in the bucket for this particular statistic:
  #

  gsutil ls -R ${FUNC_LOG_LOC}/* 2>/dev/null | grep "${FUNC_FILE_CORE}" | head -n ${FUNC_MAX_FILES_PER_RUN} > ${FUNC_FILE_LIST} || true

  #
  # Get the column header line into the top of the file:
  #

  if [ -s "${FUNC_FILE_LIST}" ]; then
    ONE_FILE=`head -n 1 ${FUNC_FILE_LIST}`
    gsutil cp ${ONE_FILE} ${FUNC_FILE_CORE}_header_file_${FUNC_FLOW_NAME}.txt
    head -n 1 ${FUNC_FILE_CORE}_header_file_${FUNC_FLOW_NAME}.txt > ${FUNC_COMBINED_PRESORT}
    rm ${FUNC_FILE_CORE}_header_file_${FUNC_FLOW_NAME}.txt

    #
    # Maybe get a backup date to assign a zero if we wind up with no data?
    #
    echo ${ONE_FILE}

    #
    # Haul in the files:
    #

    TOTAL=`cat ${FUNC_FILE_LIST} | wc -l`
    N=0
    while IFS= read -r LINE
    do
      gsutil cat ${LINE} | grep -v ${FUNC_HEADER_FIELD} > ${FUNC_FILE_CORE}_tmp_${FUNC_FLOW_NAME}.csv
      while IFS= read -r TLINE; do
        LINE_TIME=`echo ${TLINE} | cut -d, -f1 | sed -e 's/"//g'`
        LINE_CDR=`echo ${TLINE} | cut -d, -f2-`
        LINE_STAMP=`date -d"${LINE_TIME}" +"%Y-%m-%d %H:%M:%S"`
        echo "\"${LINE_STAMP}\",${LINE_CDR}" >> ${FUNC_COMBINED_PRESORT}
      done < ${FUNC_FILE_CORE}_tmp_${FUNC_FLOW_NAME}.csv
      rm -f ${FUNC_FILE_CORE}_tmp_${FUNC_FLOW_NAME}.csv
      #message: Could not parse '12/04/2024 13:56:00' as a timestamp.
      #Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD

      echo "${LINE}"
      TARG_LINE=`echo "${LINE}" | sed -e "s/${FUNC_LOG_LOC_PRE}/${FUNC_LOG_LOC_POST}/"`
      echo "${TARG_LINE}"
      gsutil mv ${LINE} ${TARG_LINE}
      N=$((N+1))
      echo "Finished ${N} of ${TOTAL}"
    done < ${FUNC_FILE_LIST}

    #
    # For laughs, let's sort and uniq the combined list
    #

    FILE_HEAD=`head -n 1 ${FUNC_COMBINED_PRESORT}`
    echo ${FILE_HEAD} > ${FUNC_COMBINED}
    grep -v "${FILE_HEAD}" ${FUNC_COMBINED_PRESORT} | sort | uniq >> ${FUNC_COMBINED}

    #
    # Up to the bucket and into BQ!
    #

    gsutil cp ${FUNC_COMBINED} ${FUNC_BQ_BUCK}
    LINECOUNT=`cat ${FUNC_COMBINED} | wc -l`
    if [ ${LINECOUNT} -eq 1 ]; then
      echo "There are no records so skipping BQ load"
    else
      echo ${FUNC_LOG_LOC}
      echo ${FUNC_FILE_CORE}
      echo ${FUNC_HEADER_FIELD}
      echo ${FUNC_MAX_FILES_PER_RUN}
      echo ${FUNC_FILE_LIST}
      echo ${FUNC_FLOW_NAME}
      echo ${FUNC_COMBINED_PRESORT}
      echo ${FUNC_COMBINED}
      echo ${FUNC_LOG_LOC_PRE}
      echo ${FUNC_LOG_LOC_POST}
      echo ${FUNC_BQ_BUCK}
      echo ${FUNC_BQ_TABLE}
      echo ${FUNC_SCHEMA_FILE}
      bq load --source_format=CSV --field_delimiter=',' --skip_leading_rows=1 "${FUNC_BQ_TABLE}" "${FUNC_BQ_BUCK}" "${FUNC_SCHEMA_FILE}"
    fi
    rm -f ${FUNC_COMBINED_PRESORT} ${FUNC_COMBINED}
  fi

  rm ${FUNC_FILE_LIST}

}

if [[ $# -ne 1 ]]; then
    echo "Usage: pdp_logs_to_bq.sh [flow-name]"
    exit
fi

FLOW_NAME=$1

source ${HOME}/scripts/setPDPEnvCombine-${FLOW_NAME}.sh

cd ${HOME}/pdpProcess
TODAY=`date +%Y-%m-%d-%H-%M`
FILE_LIST_SENT_BYTE=curr-byte-${FLOW_NAME}-${TODAY}.txt
FILE_LIST_LIST_OBJ=curr-list-${FLOW_NAME}-${TODAY}.txt
FILE_LIST_REQ_OBJ=curr-request-${FLOW_NAME}-${TODAY}.txt
COMBINED_BYTE_PRESORT=combined-byte-presort-${FLOW_NAME}-${TODAY}.csv
COMBINED_LIST_PRESORT=combined-list-presort-${FLOW_NAME}-${TODAY}.csv
COMBINED_REQ_PRESORT=combined-request-presort-${FLOW_NAME}-${TODAY}.csv
COMBINED_BYTE=combined-byte-${FLOW_NAME}-${TODAY}.csv
COMBINED_LIST=combined-list-${FLOW_NAME}-${TODAY}.csv
COMBINED_REQ=combined-request-${FLOW_NAME}-${TODAY}.csv
BYTE_HEADER_FIELD=read_object_sent_bytes_count
LIST_HEADER_FIELD=list_objects_request_count
READ_HEADER_FIELD=read_object_request_count
LIST_FILE_CORE=list_objects_request_count
READ_OBJECT_FILE_CORE=read_object_request_count
READ_BYTE_FILE_CORE=read_object_sent_byte_count
BYTE_BQ_BUCK=${BQ_BUCK}/${COMBINED_BYTE}
LIST_BQ_BUCK=${BQ_BUCK}/${COMBINED_LIST}
READ_BQ_BUCK=${BQ_BUCK}/${COMBINED_REQ}
LIST_SCHEMA_FILE="myListSchema-${FLOW_NAME}-${TODAY}.json"
BYTE_SCHEMA_FILE="myByteSchema-${FLOW_NAME}-${TODAY}.json"
READ_SCHEMA_FILE="myReqSchema-${FLOW_NAME}-${TODAY}.json"

cat << EOF1 > ${BYTE_SCHEMA_FILE}
[
  {
    "name": "timestamp",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "${BYTE_HEADER_FIELD}",
    "type": "INTEGER",
    "mode": "REQUIRED"
  }
]
EOF1

pull_a_set ${LOG_LOC} ${READ_BYTE_FILE_CORE} ${BYTE_HEADER_FIELD} ${MAX_FILES_PER_RUN} ${FILE_LIST_SENT_BYTE} \
           ${FLOW_NAME} ${COMBINED_BYTE_PRESORT} ${COMBINED_BYTE} ${LOG_LOC_PRE} ${LOG_LOC_POST} \
           ${BYTE_BQ_BUCK} ${BYTE_BQ_TABLE} ${BYTE_SCHEMA_FILE}

rm  ${BYTE_SCHEMA_FILE}

cat << EOF2 > ${LIST_SCHEMA_FILE}
[
  {
    "name": "timestamp",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "${LIST_HEADER_FIELD}",
    "type": "INTEGER",
    "mode": "REQUIRED"
  }
]
EOF2

pull_a_set ${LOG_LOC} ${LIST_FILE_CORE} ${LIST_HEADER_FIELD} ${MAX_FILES_PER_RUN} ${FILE_LIST_LIST_OBJ} \
           ${FLOW_NAME} ${COMBINED_LIST_PRESORT} ${COMBINED_LIST} ${LOG_LOC_PRE} ${LOG_LOC_POST} \
           ${LIST_BQ_BUCK} ${LIST_BQ_TABLE} ${LIST_SCHEMA_FILE}

rm  ${LIST_SCHEMA_FILE}

cat << EOF3 > ${READ_SCHEMA_FILE}
[
  {
    "name": "timestamp",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "${READ_HEADER_FIELD}",
    "type": "INTEGER",
    "mode": "REQUIRED"
  }
]
EOF3


pull_a_set ${LOG_LOC} ${READ_OBJECT_FILE_CORE} ${READ_HEADER_FIELD} ${MAX_FILES_PER_RUN} ${FILE_LIST_REQ_OBJ} \
           ${FLOW_NAME} ${COMBINED_REQ_PRESORT} ${COMBINED_REQ} ${LOG_LOC_PRE} ${LOG_LOC_POST} \
           ${READ_BQ_BUCK} ${READ_BQ_TABLE} ${READ_SCHEMA_FILE}

rm  ${READ_SCHEMA_FILE}



