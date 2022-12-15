try:
    from google.cloud import bigquery_datatransfer, bigquery_datatransfer_v1
    from google.protobuf.timestamp_pb2 import Timestamp
    from time import time
except Exception as e:
    print("Error : {}".format(e))

transfer_client = bigquery_datatransfer.DataTransferServiceClient()

# PROJECT_ID = "projects/still-emissary-369616/locations/asia-southeast1"
# # PROJECT_ID = {"projects":"still-emissary-369616","locations":"asia-southeast1"}
# DATASET_ID = "still-emissary-369616.ETL_projects"
# SERVICE_ACCOUNT = "aws-500@still-emissary-369616.iam.gserviceaccount.com"
# SCHEDULE_NAME = 'sample'

project_id = "still-emissary-369616"
dataset_id = "ETL_projects"
service_account = "aws-500@still-emissary-369616.iam.gserviceaccount.com"
display_name = 'sample'
location = "asia-southeast1"

def client(project_id, location):
    parent = transfer_client.common_location_path(project_id, location)
    return parent

# 2 options:
    # 1. delete previous task created and always create a new one (risk: ??, additional cost for creating?)
    # 2. just create 1 task and invoke to run the same task again (risk: task may be deleted)

def create_data_transfer_task(parent, dataset_id, display_name):
    # creates the task
    transfer_config = bigquery_datatransfer.TransferConfig(
        destination_dataset_id=dataset_id,
        display_name=display_name,
        data_source_id="amazon_s3",
        params={
            "destination_table_name_template": "test_table",
            "data_path": "s3://s3-etl-lambobr/project/books/LOAD00000001.csv",
            "access_key_id": "AKIATDJ4AYU4WISDRJIO",
            "secret_access_key": "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",
            "file_format": "CSV",
            "write_disposition": "WRITE_TRUNCATE",
        },
        # schedule="once",
        schedule_options={
            "disable_auto_scheduling": True
        }
    )
    transfer_config = transfer_client.create_transfer_config(
            parent=parent,
            transfer_config=transfer_config,
    )

    return transfer_config


def start_data_transfer_task(transfer_config, project_id, location):
    # starts the transfer config task
    transfer_config_name = transfer_config.name
    transfer_config_id = transfer_config_name.rsplit('/', 1)[-1]
    start_time = Timestamp(seconds=int(time()+20))
    parent_path = f'projects/{project_id}/locations/{location}/transferConfigs/{transfer_config_id}'
    transfer_runs = transfer_client.start_manual_transfer_runs({"parent": parent_path, "requested_run_time": start_time})


if __name__ == '__main__':
    parent = client(project_id, location)
    transfer_config = create_data_transfer_task(parent, dataset_id, display_name)
    start_data_transfer_task(transfer_config, project_id, location)