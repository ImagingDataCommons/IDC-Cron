import functions_framework
from google.cloud import storage
from google.cloud import monitoring_v3
import datetime
import os

BUCKET_NAME = os.environ['BUCKET_NAME']
LOGGING_BUCKET_NAME = os.environ['LOGGING_BUCKET_NAME']
PROJECT_ID = os.environ['PROJECT_ID']

def csv_to_bucket(storage_client, logging_bucket_name, file_name, csv):
    bucket = storage_client.bucket(logging_bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(csv, content_type='text/csv')
    return

def generate_csv(project_id, monitoring_client, filter, interval, metric_name):
    results = monitoring_client.list_time_series(
        request={
            "name": f"projects/{project_id}",
            "filter": filter,
            "interval": interval,
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }
    )

    csv = '{0},{1}\n'.format('timestamp', metric_name)
    for result in results:
        for point in result.points:
            data_point = point.interval.start_time.strftime("%m/%d/%Y %H:%M:%S")
            csv += '{0},{1}\n'.format(data_point, point.value.int64_value)

    return csv


@functions_framework.http
def collect_stats(request):
    monitoring_client = monitoring_v3.MetricServiceClient()
    storage_client = storage.Client()
    #
    # I believe the interval is of range (startTime, endTime] (start time not included)
    # Get the stats for the last hour :01-:00
    #
    last_hour = datetime.datetime.utcnow().strftime("%d/%m/%Y %H:00:00")
    top_hour_stamp = int(datetime.datetime.strptime(last_hour, "%d/%m/%Y %H:%M:%S").timestamp())
    last_top_hour_stamp = top_hour_stamp - (60 * 60)
    interval = monitoring_v3.TimeInterval(
        {
            "end_time": {"seconds": top_hour_stamp},
            "start_time": {"seconds": last_top_hour_stamp},
        }
    )

    list_objects_request_count_filter = f'metric.type = "storage.googleapis.com/api/request_count" AND resource.type = "gcs_bucket" AND resource.label.bucket_name = "{BUCKET_NAME}" AND metric.label.method = "ListObjects"  metric.label.response_code = "OK"'
    read_object_request_count_filter = f'metric.type = "storage.googleapis.com/api/request_count" AND resource.type = "gcs_bucket" AND resource.label.bucket_name = "{BUCKET_NAME}" AND metric.label.method = "ReadObject"  metric.label.response_code = "OK"'
    read_object_sent_bytes_count_filter = f'metric.type = "storage.googleapis.com/network/sent_bytes_count" AND resource.type = "gcs_bucket" AND resource.label.bucket_name = "{BUCKET_NAME}" AND metric.label.method = "ReadObject"  metric.label.response_code = "OK"'

    list_objects_request_count_csv = generate_csv(PROJECT_ID, monitoring_client, list_objects_request_count_filter, interval, "list_objects_request_count")
    read_object_request_count_csv = generate_csv(PROJECT_ID, monitoring_client, read_object_request_count_filter, interval, "read_object_request_count")
    read_object_sent_byte_count_csv = generate_csv(PROJECT_ID, monitoring_client, read_object_sent_bytes_count_filter, interval, "read_object_sent_bytes_count")

    acquire_time = datetime.datetime.utcnow()
    acquire_date = acquire_time.strftime("%m-%d-%Y")
    acquire_hms = acquire_time.strftime("%H:%M:%S")

    # E.g. idc-legacy-data-usage-logs/public-datasets-idc/12-04-2024/list_objects_request_count_14:00:07
    csv_to_bucket(storage_client, LOGGING_BUCKET_NAME, f'{BUCKET_NAME}/{acquire_date}/list_objects_request_count_{acquire_hms}', list_objects_request_count_csv)
    csv_to_bucket(storage_client, LOGGING_BUCKET_NAME, f'{BUCKET_NAME}/{acquire_date}/read_object_request_count_{acquire_hms}', read_object_request_count_csv)
    csv_to_bucket(storage_client, LOGGING_BUCKET_NAME, f'{BUCKET_NAME}/{acquire_date}/read_object_sent_byte_count_{acquire_hms}', read_object_sent_byte_count_csv)

    return "Successful\n"

