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
# Replace with local configuration settings:
#

LOG_LOC=gs://bucket-where-storage=log-files-dumped
LOG_LOC_PRE=storage-bucket-frag
LOG_LOC_POST=storage-archive-bucket-frag-replace
BQ_BUCK=gs://bucket-where-bq-files-are-uploaded
USAGE_BQ_TABLE=bq_dataset_destination.table_name_access_records
STORAGE_BQ_TABLE=bq_dataset_destination.table_name_storage_records
