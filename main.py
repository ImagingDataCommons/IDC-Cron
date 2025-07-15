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

from tasks.log_buckets_and_members import logit
from tasks.bucket_access_to_bq import sink_from_bucket_to_table
from tasks.proxy_usage_processing import process_logs
from tasks.check_cron_health import check_for_cron
from flask import Flask
from google.cloud import logging as glog
from config import settings
import logging

# Get standard logger set up
gcp_id = settings['DEPLOY_PROJECT_ID']
client = glog.Client(project=gcp_id)
client.get_default_handler()
client.setup_logging()

app = Flask(__name__)

@app.route('/tasks/log_buckets_and_members')
def iam_cron_work():
    try:
        logit(client)
    except Exception as e:
        logging.exception(e)

    return ''

@app.route('/tasks/process_proxy_usage')
def proxy_cron_work():
    try:
        process_logs()
    except Exception as e:
        logging.exception(e)

    return ''

@app.route('/tasks/transfer_bucket_access_to_bq')
def access_cron_work():

    try:
        sink_from_bucket_to_table()
    except Exception as e:
        logging.exception(e)

    return ''

@app.route('/tasks/check_cron')
def check_cron_work():

    try:
        check_for_cron()
    except Exception as e:
        logging.exception(e)

    return ''

if __name__ == '__main__':
    # This is used when running locally only.
    app.run(host='127.0.0.1', port=8080, debug=True)
