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

# h/t Inian @  https://stackoverflow.com/questions/54802450/bash-display-nano-seconds-with-date
from_micro_to_readable() {
  withMicro="$(( $1 % 1000000 ))"
  withoutMicro="$(date -d@"$(( $1 / 1000000 ))" +"%Y-%m-%d %H:%M:%S")"
  printf '%s UTC\n' "$withoutMicro.$withMicro"
}

source ${HOME}/scripts/setEnvCombine.sh

cd ${HOME}
TODAY=`date +%Y-%m-%d-%H-%M`
FILE_LIST_USAGE=curr-usage-${TODAY}.txt
FILE_LIST_STORAGE=curr-store-${TODAY}.txt
COMBINED_USAGE=combined-usage-${TODAY}.csv
COMBINED_STORAGE=combined-storage-${TODAY}.csv
USAGE_HEADER_FIELD=cs_user_agent
STORAGE_HEADER_FIELD=storage_byte_hours
USAGE_BQ_BUCK=${BQ_BUCK}/${COMBINED_USAGE}
STORAGE_BQ_BUCK=${BQ_BUCK}/${COMBINED_STORAGE}

#
# Generate lists of the usage and storage files in the bucket:
#

gsutil ls ${LOG_LOC}/* | grep "_usage_20" > ${FILE_LIST_USAGE}

#
# Get the column header line into the top of the file:
#

if [ -s "${FILE_LIST_USAGE}" ]; then
  ONE_FILE=`head -n 1 ${FILE_LIST_USAGE}`
  gsutil cp ${ONE_FILE} usage_header_file.txt
  head -n 1 usage_header_file.txt | sed -e 's/time_micros/time/' > ${COMBINED_USAGE}
  rm usage_header_file.txt

  #
  # Do usage files (~1,500 files per hour)
  #

  TOTAL=`cat ${FILE_LIST_USAGE} | wc -l`
  N=0
  while IFS= read -r LINE
  do
    gsutil cat ${LINE} | grep -v ${USAGE_HEADER_FIELD} > usage_tmp.csv
    while IFS= read -r TLINE; do
      LINE_TIME=`echo ${TLINE} | cut -d, -f1 | sed -e 's/"//g'`
      LINE_CDR=`echo ${TLINE} | cut -d, -f2-`
      LINE_STAMP=`from_micro_to_readable $LINE_TIME`
      echo "\"${LINE_STAMP}\",${LINE_CDR}" >> ${COMBINED_USAGE}
    done < usage_tmp.csv
    echo "${LINE}"
    TARG_LINE=`echo "${LINE}" | sed -e "s/${LOG_LOC_PRE}/${LOG_LOC_POST}/"`
    echo "${TARG_LINE}"
    gsutil mv ${LINE} ${TARG_LINE}
    N=$((N+1))
    echo "Finished ${N} of ${TOTAL}"
  done < ${FILE_LIST_USAGE}
  rm -f usage_tmp.csv

  gsutil cp ${COMBINED_USAGE} ${USAGE_BQ_BUCK}
  bq load --source_format=CSV --field_delimiter=',' --skip_leading_rows=1 ${USAGE_BQ_TABLE} ${USAGE_BQ_BUCK}
  rm -f ${COMBINED_USAGE}
fi

#
# Do storage files. The date for the value in the file is in the file name. Yeah.
#

#
# If the directroy has NO objects, this is going to complain "CommandException: One or more URLs matched no objects" without all the
# extra routing. Have it return true if nothing is there
#
gsutil ls ${LOG_LOC}/* 2>/dev/null | grep "_storage_20" > ${FILE_LIST_STORAGE} || true

if [ -s "${FILE_LIST_STORAGE}" ]; then
  ONE_FILE=`head -n 1 ${FILE_LIST_STORAGE}`
  gsutil cp ${ONE_FILE} storage_header_file.txt
  EXIST_HEAD=`head -n 1 storage_header_file.txt`
  FULL_HEAD="\"time\",${EXIST_HEAD}"
  echo ${FULL_HEAD} > ${COMBINED_STORAGE}
  rm storage_header_file.txt

  TOTAL=`cat ${FILE_LIST_STORAGE} | wc -l`
  N=0
  while IFS= read -r LINE
  do
    DATE=`echo "${LINE}" | sed -e 's/.*_storage_2/2/'`
    # h/t palindrom @ https://stackoverflow.com/questions/918886/how-do-i-split-a-string-on-a-delimiter-in-bash
    DA=(${DATE//_/ })
    TIMESTAMP="${DA[0]}-${DA[1]}-${DA[2]} ${DA[3]}:${DA[4]}:${DA[5]} UTC"
    gsutil cat ${LINE} | grep -v ${STORAGE_HEADER_FIELD} > storage_tmp.csv
    while IFS= read -r TLINE; do
      echo "\"${TIMESTAMP}\",${TLINE}" >> ${COMBINED_STORAGE}
    done < storage_tmp.csv
    echo "${LINE}"
    TARG_LINE=`echo "${LINE}" | sed -e "s/${LOG_LOC_PRE}/${LOG_LOC_POST}/"`
    echo "${TARG_LINE}"
    gsutil mv ${LINE} ${TARG_LINE}
    N=$((N+1))
    echo "Finished ${N} of ${TOTAL}"
  done < ${FILE_LIST_STORAGE}
  rm -f storage_tmp.csv

  #
  # Copy combined files into cloud storage
  #

  gsutil cp ${COMBINED_STORAGE} ${STORAGE_BQ_BUCK}
  bq load --source_format=CSV --field_delimiter=',' --skip_leading_rows=1 ${STORAGE_BQ_TABLE} ${STORAGE_BQ_BUCK}
  rm -f ${COMBINED_STORAGE}
fi

#
# Remove local files
#

rm ${FILE_LIST_USAGE}
rm ${FILE_LIST_STORAGE}

