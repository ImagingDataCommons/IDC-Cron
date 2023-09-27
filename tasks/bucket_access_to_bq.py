"""

Copyright 2020, Institute for Systems Biology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import re
import datetime
import time
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import numpy as np
from google.cloud.exceptions import NotFound
from config import settings
import logging


#
# Both the Google and the Pandas schemas for the usage table. Depends on whether we are using it to read in
# data or write it out:
#

def get_usage_schema(for_read):
    usage_schema_common = [

        bigquery.SchemaField("c_ip", "STRING", mode="REQUIRED",
                             description='The IP address from which the request was made. The "c" prefix indicates that this is information about the client.'),
        bigquery.SchemaField("c_ip_type", "INTEGER", mode="REQUIRED",
                             description='The type of IP in the c_ip field:  A value of 1 indicates an IPV4 address. '
                                         'A value of 2 indicates an IPV6 address.'),
        bigquery.SchemaField("c_ip_region", "STRING", mode="NULLABLE",
                             description='Reserved for future use.'),
        bigquery.SchemaField("cs_method", "STRING", mode="NULLABLE",
                             description='The HTTP method of this request. The "cs" prefix indicates that this information '
                                         'was sent from the client to the server name'),
        bigquery.SchemaField("cs_uri", "STRING", mode="REQUIRED",
                             description='The URI of the request.'),
        bigquery.SchemaField("sc_status", "INTEGER", mode="REQUIRED",
                             description='The HTTP status code the server sent in response. The "sc" prefix indicates '
                                         'that this information was sent from the server to the client.'),
        bigquery.SchemaField("cs_bytes", "INTEGER", mode="NULLABLE",
                             description='The number of bytes sent in the request.'),
        bigquery.SchemaField("sc_bytes", "INTEGER", mode="REQUIRED",
                             description='The number of bytes sent in the response.'),
        bigquery.SchemaField("time_taken_micros", "INTEGER", mode="NULLABLE",
                             description='The time it took to serve the request in microseconds, '
                                         'measured from when the first byte is received to when the '
                                         'response is sent. Note that for resumable uploads, the ending point '
                                         'is determined by the response to the final upload request that '
                                         'was part of the resumable upload.'),
        bigquery.SchemaField("cs_host", "STRING", mode="NULLABLE",
                             description='The host in the original request.'),
        bigquery.SchemaField("cs_referer", "STRING", mode="NULLABLE",
                             description='The HTTP referrer for the request.'),
        bigquery.SchemaField("cs_user_agent", "STRING", mode="NULLABLE",
                             description='The User-Agent of the request. The value is GCS '
                                         'Lifecycle Management for requests made by lifecycle management.'),
        bigquery.SchemaField("s_request_id", "STRING", mode="NULLABLE",
                             description='The request identifier.'),
        bigquery.SchemaField("cs_operation", "STRING", mode="NULLABLE",
                             description='The Cloud Storage operation e.g. GET_Object.'),
        bigquery.SchemaField("cs_bucket", "STRING", mode="NULLABLE",
                             description='The bucket specified in the request. If this is a list buckets request, this can be null.'),
        bigquery.SchemaField("cs_object", "STRING", mode="NULLABLE",
                             description='The object specified in this request. This can be null.'),
    ]

    time_for_read = bigquery.SchemaField("time_micros", "INTEGER", mode="REQUIRED",
                                         description='The time that the request was completed, in microseconds since the Unix epoch.')

    time_for_write = bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED",
                                          description='The time that the request was completed')

    pandas_schema_common = {
                           'c_ip': np.object,
                           'c_ip_type': np.int64,
                           'c_ip_region': np.object,
                           'cs_method': np.object,
                           'cs_uri': np.object,
                           'sc_status': np.int64,
                           'cs_bytes': np.int64,
                           'sc_bytes': np.int64,
                           'time_taken_micros': np.int64,
                           'cs_host': np.object,
                           'cs_referer': np.object,
                           'cs_user_agent': np.object,
                           's_request_id': np.object,
                           'cs_operation': np.object,
                           'cs_bucket': np.object,
                           'cs_object': np.object,
                           }

    pandas_schema = pandas_schema_common.copy()
    usage_schema = usage_schema_common.copy()

    if for_read:
        usage_schema.insert(0, time_for_read)
        pandas_schema['time_micros'] = np.int64
    else:
        usage_schema.insert(0, time_for_write)
        pandas_schema['time'] = np.datetime64

    return (usage_schema, pandas_schema)

#
# Both the Google and the Pandas schemas for the storage table. Depends on whether we are using it to read in
# data or write it out:
#

def get_storage_schema(for_read):
    storage_schema_common = [
        bigquery.SchemaField("bucket", "STRING", mode="REQUIRED",
                             description='The name of the bucket.'),
        bigquery.SchemaField("storage_byte_hours", "INTEGER", mode="REQUIRED",
                             description='Average size in byte-hours over a 24 hour period of the bucket. '
                                         'To get the total size of the bucket, divide byte-hours by 24.')
    ]

    time_for_write = bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED",
                                          description='Timestamp of report')

    pandas_schema_common = {
                           'bucket': np.object,
                           'storage_byte_hours': np.int64
                           }

    pandas_schema = pandas_schema_common.copy()
    storage_schema = storage_schema_common.copy()

    if not for_read:
        storage_schema.insert(0, time_for_write)
        pandas_schema['time'] = np.datetime64

    return (storage_schema, pandas_schema)

#
# Answer if BQ table exists
#

def bq_table_exists(client, target_dataset, dest_table):
    table_ref = client.dataset(target_dataset).table(dest_table)
    try:
        print(table_ref)
        client.get_table(table_ref)
        print("got it")
        return True
    except NotFound:
        return False

#
# Answer if BQ dataset exists
#

def bq_dataset_exists(client, target_dataset):
    try:
        client.get_dataset(target_dataset)
        return True
    except NotFound:
        return False

#
# Delete BQ table
#

def delete_table_bq(client, target_dataset, delete_table):
    table_ref = client.dataset(target_dataset).table(delete_table)
    try:
        client.delete_table(table_ref)
        print('Table {}:{} deleted'.format(target_dataset, delete_table))
    except NotFound as ex:
        print('Table {}:{} was not present'.format(target_dataset, delete_table))
    except Exception as ex:
        print(ex)
        return False

    return True

#
# Write the dataframe to bigQuery and archive the file if successful
#

def write_to_bucket(bq_client, df, source_bucket, archive_bucket, blob, job_config, location, full_table_name):

    write_job = bq_client.load_table_from_dataframe(df, full_table_name, location=location, job_config=job_config)

    query_job = bq_client.get_job(write_job.job_id, location=location)
    job_state = query_job.state
    while job_state != 'DONE':
        query_job = bq_client.get_job(write_job.job_id, location=location)
        job_state = query_job.state
        if job_state != 'DONE':
            time.sleep(5)

    write_job = bq_client.get_job(write_job.job_id, location=location)
    if write_job.error_result is not None:
        print('Error result!! {}'.format(write_job.error_result))
        return False
    else:
        source_bucket.copy_blob(blob, archive_bucket, blob.name)
        blob.delete()

    return True
#
# Argument-free timestamp conversion function:
#

def convert(timestamp):
    return pd.to_datetime(timestamp, unit='us', utc=True)


#
# Do the work for a project
#

def sink_from_bucket_to_table_for_project(project, tag, bq_client, storage_client, deploy_project):

    DATASET_BASE = settings['INGEST_STORAGE_LOGS_DATASET_BASE']
    USAGE_TABLE = settings['INGEST_STORAGE_LOGS_USAGE_TABLE']
    STORAGE_TABLE = settings['INGEST_STORAGE_LOGS_STORAGE_TABLE']
    SOURCE_BUCKET = settings['INGEST_STORAGE_LOGS_SOURCE_BUCKET']
    ARCHIVE_BUCKET = settings['INGEST_STORAGE_LOGS_ARCHIVE_BUCKET']
    LOCATION = settings['INGEST_STORAGE_LOGS_LOCATION']
    DO_DELETE_FIRST = (settings['INGEST_STORAGE_LOGS_DO_DELETE_FIRST'] == "True")
    LOG_FILES_PER_RUN = int(settings['INGEST_STORAGE_LOGS_FILES_PER_RUN'])

    #
    # If tables do not exist, create them. Can also delete them first:
    #

    full_dataset = "{}{}".format(DATASET_BASE, tag)
    proj_dataset = "{}.{}".format(deploy_project, full_dataset)
    if not bq_dataset_exists(bq_client, proj_dataset):
        dataset = bigquery.Dataset(proj_dataset)
        dataset.location = LOCATION
        bq_client.create_dataset(dataset)

    full_usage_table = "{}.{}.{}".format(deploy_project, full_dataset, USAGE_TABLE)

    if DO_DELETE_FIRST:
        if bq_table_exists(bq_client, full_dataset, USAGE_TABLE):
            delete_table_bq(bq_client, full_dataset, USAGE_TABLE)

    if not bq_table_exists(bq_client, full_dataset, USAGE_TABLE):
        table = bigquery.Table(full_usage_table, schema=get_usage_schema(False)[0])
        table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="time")
        bq_client.create_table(table)

    full_storage_table = "{}.{}.{}".format(deploy_project, full_dataset, STORAGE_TABLE)

    if DO_DELETE_FIRST:
        if bq_table_exists(bq_client, full_dataset, STORAGE_TABLE):
            delete_table_bq(bq_client, full_dataset, STORAGE_TABLE)

    if not bq_table_exists(bq_client, full_dataset, STORAGE_TABLE):
        table = bigquery.Table(full_storage_table, schema=get_storage_schema(False)[0])
        table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="time")
        bq_client.create_table(table)

    ##
    ## Get a listing of files. Then, loop through the files, read each into a dataframe, massage the timestamps,
    ## append them to BQ, and then move the file to the archive bucket:
    ##

    full_source_bucket = SOURCE_BUCKET.format(project)
    full_archive_bucket = ARCHIVE_BUCKET.format(project)

    source_bucket = storage_client.bucket(full_source_bucket)
    archive_bucket = storage_client.bucket(full_archive_bucket)
    blobs = storage_client.list_blobs(full_source_bucket)
    file_count = 0
    for blob in blobs:
        if file_count > LOG_FILES_PER_RUN:
            break
        file_count += 1
        if re.search("^.*_v0$", str(blob.name)) is None:
            raise Exception()
        url = "gs://{}/{}".format(full_source_bucket, blob.name)

        if "_usage_2" in blob.name:
            #
            # Process usage files, which have a microsecond unix timestamp which we convert to a datetime:
            #
            df = pd.read_csv(url, dtype=get_usage_schema(True)[1])
            #
            # Yeah, newlbie pandas adding a column, writing into it, and deleting the orginal:
            #
            df = df.reindex(columns=['time'] + df.columns.tolist())
            df['time']= pd.to_datetime(df['time'])
            print(df.info())
            df['time'] = df['time_micros'].apply(convert)
            df = df.drop('time_micros', axis=1)

            job_config = bigquery.LoadJobConfig(
                schema=get_usage_schema(False)[0],
                write_disposition="WRITE_APPEND"
            )

            if not write_to_bucket(bq_client, df, source_bucket, archive_bucket, blob, job_config, LOCATION, full_usage_table):
                raise Exception()

            df = None

        elif "_storage_2" in blob.name:
            #
            # Process storage files, which have the date of the file in the filename (!?!?!):
            parse_it = blob.name.split("_storage_2")
            parse_it = parse_it[1].split("_")
            year = int("2{}".format(parse_it[0]))
            month =  int(parse_it[1])
            day = int(parse_it[2])
            hour = int(parse_it[3])
            minute = int(parse_it[4])
            second = int(parse_it[5])
            time_o_day = datetime.datetime(year, month, day, hour, minute, second, tzinfo=datetime.timezone.utc)
            df = pd.read_csv(url, dtype=get_usage_schema(True)[1])
            #
            # Add a column with the datetime:
            #
            df = df.reindex(columns=['time'] + df.columns.tolist())
            df['time'] = pd.to_datetime(df['time'])
            df['time'] = time_o_day
            print(df.info())
            print(df)

            job_config = bigquery.LoadJobConfig(
                schema=get_storage_schema(False)[0],
                write_disposition="WRITE_APPEND"
            )

            if not write_to_bucket(bq_client, df, source_bucket, archive_bucket, blob, job_config, LOCATION, full_storage_table):
                raise Exception()

            df = None

#
# Main access point
#

def sink_from_bucket_to_table():

    USER_GCP_ACCESS_CREDENTIALS = settings['GOOGLE_APPLICATION_CREDENTIALS']
    DEPLOY_PROJECT_ID = settings['DEPLOY_PROJECT_ID']
    PROJECT_IDS=settings['INGEST_STORAGE_LOGS_PROJECT_IDS']
    PROJECT_TAGS=settings['INGEST_STORAGE_LOGS_PROJECT_TAGS']

    try:
        bq_client = bigquery.Client.from_service_account_json(USER_GCP_ACCESS_CREDENTIALS, project=DEPLOY_PROJECT_ID)
        storage_client = storage.Client.from_service_account_json(USER_GCP_ACCESS_CREDENTIALS, project=DEPLOY_PROJECT_ID)
    except Exception as e:
        logging.error("Exception while building SC2")
        logging.exception(e)
        raise e

    project_list = PROJECT_IDS.split(',')
    tag_list = PROJECT_TAGS.split(',')

    for project, tag in zip(project_list, tag_list):
        sink_from_bucket_to_table_for_project(project, tag, bq_client, storage_client, DEPLOY_PROJECT_ID)


if __name__ == '__main__':
    # This is used when running locally only during test:
    sink_from_bucket_to_table()
