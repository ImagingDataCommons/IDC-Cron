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

import time
from google.cloud import bigquery
from config import settings
import logging


def generic_bq_harness(client, sql, target_dataset, dest_table, do_batch, write_depo):
    """
    Handles all the boilerplate for running a BQ job
    """
    job_config = bigquery.QueryJobConfig()
    if do_batch:
        job_config.priority = bigquery.QueryPriority.BATCH
    if write_depo is not None:
        job_config.write_disposition = write_depo

    target_ref = client.dataset(target_dataset).table(dest_table)
    job_config.destination = target_ref
    print(target_ref)
    location = 'US'

    # API request - starts the query
    query_job = client.query(sql, location=location, job_config=job_config)

    # Query
    query_job = client.get_job(query_job.job_id, location=location)
    job_state = query_job.state

    while job_state != 'DONE':
        query_job = client.get_job(query_job.job_id, location=location)
        print('Job {} is currently in state {}'.format(query_job.job_id, query_job.state))
        job_state = query_job.state
        if job_state != 'DONE':
            time.sleep(5)
    print('Job {} is done'.format(query_job.job_id))

    query_job = client.get_job(query_job.job_id, location=location)
    if query_job.error_result is not None:
        print('Error result!! {}'.format(query_job.error_result))
        return False
    return True

'''
----------------------------------------------------------------------------------------------
Extract data from proxy logs
'''
def extract_log_fields(client, raw_table, target_dataset, dest_table, do_batch):

    sql = extract_log_fields_sql(raw_table)
    return generic_bq_harness(client, sql, target_dataset, dest_table, do_batch, bigquery.WriteDisposition.WRITE_TRUNCATE)

'''
----------------------------------------------------------------------------------------------
SQL for above
'''
def extract_log_fields_sql(table):

    return '''
        SELECT
            a.timeStamp,
            CASE WHEN REGEXP_CONTAINS(a.textPayload, "GLOBAL")
                 THEN "GLOBAL"
                 ELSE REGEXP_EXTRACT(a.textPayload, r'[A-Z]+ ON [0-9-]+ FOR IP ([0-9a-f:\.]+) is.*')
            END as ip_addr,
            CAST(REGEXP_EXTRACT(a.textPayload, r' is now ([0-9]+) bytes') AS INT64) as byte_count,
            CAST(REGEXP_EXTRACT(a.textPayload, r'USAGE ON ([0-9-]+)') AS TIMESTAMP) as quota_day,
        FROM `{0}` AS a
        WHERE (a.textPayload NOT LIKE "%exceed%")
        '''.format(table)

'''
----------------------------------------------------------------------------------------------
Build the maximum byte count per IP per day
'''
def daily_byte_max(client, byte_table, target_dataset, dest_table, do_batch):

    sql = daily_byte_max_sql(byte_table)
    return generic_bq_harness(client, sql, target_dataset, dest_table, do_batch, bigquery.WriteDisposition.WRITE_TRUNCATE)

'''
----------------------------------------------------------------------------------------------
SQL for above
'''
def daily_byte_max_sql(table):

    return '''
        SELECT
            a.quota_day,
            a.ip_addr,
            MAX(a.byte_count) as bytes
        FROM `{0}` AS a
        GROUP BY a.ip_addr, a.quota_day
        ORDER BY a.quota_day
        '''.format(table)

'''
----------------------------------------------------------------------------------------------
Build the maximum byte count per IP per day
'''
def daily_user_and_largest(client, max_table, target_dataset, dest_table, do_batch):

    sql = daily_user_and_largest_sql(max_table)
    return generic_bq_harness(client, sql, target_dataset, dest_table, do_batch, bigquery.WriteDisposition.WRITE_TRUNCATE)

'''
----------------------------------------------------------------------------------------------
SQL for above
'''
def daily_user_and_largest_sql(table):

    return '''
        WITH a1 as (
          SELECT
              quota_day,
              MAX(bytes) as biggest_use,
              COUNT(*) as num_users
          FROM `{0}`
          WHERE ip_addr != "GLOBAL"
          GROUP BY quota_day),
              a2 as (
          SELECT
              quota_day,
              bytes as global_use
          FROM `{0}`
          WHERE ip_addr = "GLOBAL")

        SELECT
            a1.quota_day,
            a1.biggest_use,
            a1.num_users,
            a2.global_use
        FROM a1 JOIN a2 ON a1.quota_day = a2.quota_day
        ORDER BY a1.quota_day
        '''.format(table)


'''
----------------------------------------------------------------------------------------------
Do the work
'''
def process_logs():

    DEPLOY_PROJECT = settings['DEPLOY_PROJECT_ID']
    PROXY_PROJECT_IDS = settings['PROXY_PROJECT_IDS']
    PROXY_PROJECT_TAGS = settings['PROXY_PROJECT_TAGS']

    try:
        bqclient = bigquery.Client(project=DEPLOY_PROJECT)
    except Exception as e:
        logging.error("Exception while building SC2")
        logging.exception(e)
        raise e

    project_list = PROXY_PROJECT_IDS.split(',')
    tag_list = PROXY_PROJECT_TAGS.split(',')

    for project, tag in zip(project_list, tag_list):
        process_raw_logs_for_project(DEPLOY_PROJECT, project, tag, bqclient)

    return


'''
----------------------------------------------------------------------------------------------
Do the work
'''
def process_raw_logs_for_project(deploy_project, project, tag, bqclient):

    logging.info('Processing proxy logs for {}'.format(project))
    full_dataset_raw = "{}{}".format(settings["PROXY_RAW_DATASET_BASE"], tag)
    full_dataset_stats = "{}{}".format(settings["PROXY_STATS_DATASET_BASE"], tag)

    input_table = "{}.{}.{}".format(deploy_project, full_dataset_raw, settings["PROXY_RAW_TABLES"])
    output_table = settings["PROXY_PROCESSED_TABLE"]
    success = extract_log_fields(bqclient, input_table, full_dataset_stats, output_table, False)
    if not success:
        logging.error("{} extract_log_fields job failed".format(input_table))
        return False

    input_table = "{}.{}.{}".format(deploy_project, full_dataset_stats, settings["PROXY_PROCESSED_TABLE"])
    output_table = settings["PROXY_BYTES_TABLE"]
    success = daily_byte_max(bqclient, input_table, full_dataset_stats, output_table, False)
    if not success:
        logging.error("{} daily_byte_max job failed".format(input_table))
        return False

    input_table = "{}.{}.{}".format(deploy_project, full_dataset_stats, settings["PROXY_BYTES_TABLE"])
    output_table = settings["PROXY_MAX_TABLE"]

    success = daily_user_and_largest(bqclient, input_table, full_dataset_stats, output_table, False)
    if not success:
        logging.error("{} daily_user_and_largest job failed".format(input_table))
        return False

    logging.info('Finished processing proxy logs for {}'.format(project))
    return True

if __name__ == '__main__':
    # This is used when running locally only during test:
    process_logs()
