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

from google.cloud import storage
from oauth2client.client import GoogleCredentials
from google_helpers.utils import execute_with_retries
from google_helpers.utils import build_with_retries
from config import settings
import logging

#
# Do the work:
#

def logit(client):

    MONITOR_PROJECT_IDS = settings['MONITOR_PROJECT_IDS']
    MONITOR_PROJECT_TAGS = settings['MONITOR_PROJECT_TAGS']

    project_list = MONITOR_PROJECT_IDS.split(',')
    tag_list = MONITOR_PROJECT_TAGS.split(',')

    for project, tag in zip(project_list, tag_list):
        logit_for_project(project, tag, client)

    return


#
# Do the work:
#

def logit_for_project(targ_proj, targ_tag, client):

    logging.info('Into logit()')

    bucket_acl_logger = client.logger(settings['BUCKET_ACL_LOG_NAME'].format(targ_tag))
    bucket_def_acl_logger = client.logger(settings['BUCKET_DEFAULT_ACL_LOG_NAME'].format(targ_tag))
    file_unique_acl_logger = client.logger(settings["FILE_UNIQUE_ACL_LOG_NAME"].format(targ_tag))
    bucket_iam_logger = client.logger(settings['BUCKET_IAM_LOG_NAME'].format(targ_tag))
    project_iam_logger = client.logger(settings['PROJECT_IAM_LOG_NAME'].format(targ_tag))

    credentials = GoogleCredentials.get_application_default()

    ##
    ## There is an alpha implementation of the Resource Manager in the V2 API, but it does
    ## not support hauling out the IAM policy of a project. So another case of falling back
    ## on the old API. New API would be:
    ## crm_client = resource_manager.Client.from_service_account_json(USER_GCP_ACCESS_CREDENTIALS)
    ## WJRL 5/25/20: Quick Google check seems to indicate that this is still the way to go.
    ##

    iam_array = []
    try:
        crm_client = build_with_retries('cloudresourcemanager', 'v1beta1', credentials, 2)

        body = {}
        req = crm_client.projects().getIamPolicy(resource=targ_proj, body=body)
        iam_policy = execute_with_retries(req, 'GET_IAM', 2)
        entities = set()

        object_owners = ("roles/cloudbuild.builds.builder", "roles/storage.admin", "roles/storage.objectAdmin",
                         "roles/storage.objectCreator", "roles/editor", "roles/owner")

        for bind in iam_policy["bindings"]:
            for member in bind['members']:
                entry = {
                  'project': targ_proj,
                  'role': bind["role"],
                  'member': member
                }
                iam_array.append(entry)
                if bind["role"] in object_owners:
                    if member.startswith("user:"):
                        entities.add((member.replace("user:", "user-", 1), "OWNER"))
                    elif member.startswith("serviceAccount:"):
                        entities.add((member.replace("serviceAccount:", "user-", 1), "OWNER"))

    except Exception as e:
        logging.error("Exception while getting PIAM")
        logging.exception(e)
        raise e

    #
    # IAM policy is not available in the V1 interface, and ACLs not available in the V2 interface??
    # So use both:
    #

    try:
        storage_client2 = storage.Client.from_service_account_json(USER_GCP_ACCESS_CREDENTIALS, project=targ_proj)
    except Exception as e:
        logging.error("Exception while building SC2")
        logging.exception(e)
        raise e

    buck_acl_check = []
    per_buck_entities = {}

    #
    # It is an error to try and access the acl when the bucket is using bucket-level IAM. So we need to do that
    # first and see who has an ACL:
    #

    buck_iam_array = []
    for a_buck in storage_client2.list_buckets():
        try:
            this_buck_entities = entities.copy()
            per_buck_entities[a_buck.name] = this_buck_entities
            # Bucket needs this property set to handle requester-pays:
            use_bucket = storage_client2.bucket(a_buck.name, user_project = targ_proj)
            buck_iam = use_bucket.get_iam_policy(requested_policy_version=3)
            #if not buck_iam.uniform_bucket_level_access_enabled:
            if not a_buck.iam_configuration['uniformBucketLevelAccess']['enabled']:
                buck_acl_check.append(a_buck.name)

            # Transform the IAM Policy object into a dictionary that we can serialize:

            for bind in buck_iam.to_api_repr()["bindings"]:
                for member in bind['members']:
                    entry = {
                        'project': targ_proj,
                        'bucket': use_bucket.name,
                        'role': bind["role"],
                        'member': member
                    }
                    if bind["role"] in object_owners:
                        if member.startswith("user:"):
                            this_buck_entities.add((member.replace("user:", "user-", 1), "OWNER"))
                        elif member.startswith("serviceAccount:"):
                            this_buck_entities.add((member.replace("serviceAccount:", "user-", 1), "OWNER"))

                    buck_iam_array.append(entry)

        except Exception as e:
            logging.error("Exception while getting BIAM")
            logging.exception(e)


    #
    # Note: Experimented with bucket ACLs. If I made a *folder* public, then the bucket was made public as well.
    # However, objects could be made public individually 5/25/20
    #

    acl_array = []
    def_acl_array = []
    object_acl_array = []


    for buck_name in buck_acl_check:
        try:
            this_buck_entities = per_buck_entities[buck_name]
            bucket = storage_client2.bucket(buck_name, user_project = targ_proj)
            for item in bucket.acl:
                entry = {
                    'project': targ_proj,
                    'bucket': buck_name,
                    'role': item["role"],
                    'entity': item["entity"]
                }
                this_buck_entities.add((item["entity"], item["role"]))
                acl_array.append(entry)

            for item in bucket.default_object_acl:
                entry = {
                    'project': targ_proj,
                    'bucket': buck_name,
                    'role': item["role"],
                    'entity': item["entity"]
                }
                def_acl_array.append(entry)

            #
            # We should be making all buckets in our projects have uniform bucket level access. But if
            # we don't, we want to know if there are any weird acl entries that we do not expect
            #

            for blob in bucket.list_blobs():
                for acl_entry in blob.acl:
                    acl_tup = (acl_entry["entity"], acl_entry["role"])
                    if acl_tup not in this_buck_entities:
                        entry = {
                            'project': targ_proj,
                            'bucket': buck_name,
                            'file_name': blob.name,
                            'role': acl_entry["role"],
                            'entity': acl_entry["entity"]
                        }
                        object_acl_array.append(entry)


        except Exception as e:
            logging.error("Exception while getting BAC")
            logging.exception(e)

    try:
        bucket_acl_logger.log_struct({'bucket_acls': acl_array})
    except Exception as e:
        logging.error("Exception while logging ACL.")
        logging.exception(e)

    try:
        bucket_def_acl_logger.log_struct({'bucket_def_acls': def_acl_array})
    except Exception as e:
        logging.error("Exception while logging default ACL.")
        logging.exception(e)

    try:
        file_unique_acl_logger.log_struct({'bucket_unique_obj_acls': object_acl_array})
    except Exception as e:
        logging.error("Exception while logging file uniqe ACL.")
        logging.exception(e)

    try:
        bucket_iam_logger.log_struct({'bucket_iam': buck_iam_array})
    except Exception as e:
        logging.error("Exception while logging bucket IAM.")
        logging.exception(e)

    try:
        project_iam_logger.log_struct({'project_iam': iam_array})
    except Exception as e:
        logging.error("Exception while logging project IAM.")
        logging.exception(e)

    return
