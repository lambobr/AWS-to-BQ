try:
    import boto3
    import json
except Exception as e:
    print("Error : {}".format(e))

with open(r'/opt/airflow/python_scripts/dms_config.json') as file:
    config = json.load(file)
    SourceEndpointArn = config["SourceEndpointArn"]
    TargetEndpointArn = config["TargetEndpointArn"]
    ReplicationInstanceArn = config["ReplicationInstanceArn"]
    ReplicationTaskName = config["ReplicationTaskName"]
    S3BucketName = config["S3BucketName"]
    ID = config["aws_access_key_id"]
    Secret = config["aws_secret_access_key"]
    gcp_ID = config["gcp_ID"]
    gcp_Secret = config["gcp_Secret"]

def init():
    client_s3 = boto3.client('s3', region_name='ap-southeast-1', aws_access_key_id = ID, aws_secret_access_key= Secret)
    client_dms = boto3.client('dms', region_name='ap-southeast-1', aws_access_key_id = ID, aws_secret_access_key= Secret)
    return client_s3, client_dms

#create s3 bucket if not exists
def s3_bucket(client_s3,S3BucketName):
    try:
        client_s3.head_bucket(Bucket=S3BucketName)
    except (client_s3.exceptions.NoSuchBucket, client_s3.exceptions.ClientError):
        client_s3.create_bucket(Bucket=S3BucketName,CreateBucketConfiguration={'LocationConstraint': 'ap-southeast-1'})

#delete task if already exist before creating new one
#need to delete since MigrationType should be full-load not resume
def delete_existing_task(client_dms, ReplicationTaskName):
    try:
        response = client_dms.describe_replication_tasks(
            Filters=[
                {
                    'Name': 'replication-task-id',
                    'Values': [
                        ReplicationTaskName,
                    ]
                },
            ],
            MaxRecords=100,
            WithoutSettings=True
        )
        ExistingReplicationTaskArn = response['ReplicationTasks'][0]['ReplicationTaskArn']
        client_dms.delete_replication_task(ReplicationTaskArn=ExistingReplicationTaskArn)

        #wait for successful deletion before creating new task
        waiter = client_dms.get_waiter('replication_task_deleted')
        waiter.wait(
            Filters=[
                {
                    'Name': 'replication-task-arn',
                    'Values': [
                        ExistingReplicationTaskArn,
                    ]
                },
            ],
            MaxRecords=100,
            WaiterConfig={
                'Delay': 15,    #polls every 15 seconds
                'MaxAttempts': 60
            }
        )

    except client_dms.exceptions.ResourceNotFoundFault: #otherwise if task not exists
        pass

#create dms task
def create_task(client_dms, SourceEndpointArn, TargetEndpointArn, ReplicationInstanceArn, ReplicationTaskName):

    response = client_dms.create_replication_task(
        ReplicationTaskIdentifier=ReplicationTaskName,
        SourceEndpointArn= SourceEndpointArn,
        TargetEndpointArn= TargetEndpointArn,
        ReplicationInstanceArn= ReplicationInstanceArn,
        MigrationType='full-load',
        TableMappings= """{
                            "rules": [
                                {
                                    "rule-type": "selection",
                                    "rule-id": "1",
                                    "rule-name": "1",
                                    "object-locator": {
                                        "schema-name": "project",
                                        "table-name": "%"
                                    },
                                    "rule-action": "include"
                                }
                            ]
                        }""",
        )
    #returns replication task arn
    return response['ReplicationTask']['ReplicationTaskArn']

#start dms task
def start_task(client_dms, replicationTaskArn):
    # waits for task to be ready before starting
    waiter = client_dms.get_waiter('replication_task_ready')
    waiter.wait(
        Filters=[
            {
                'Name': 'replication-task-arn',
                'Values': [
                    replicationTaskArn,
                ]
            },
        ],
        MaxRecords=100,
        WaiterConfig={
            'Delay': 15,
            'MaxAttempts': 60
        }
    )
    #starts the task
    client_dms.start_replication_task(
        ReplicationTaskArn=replicationTaskArn,
        StartReplicationTaskType='start-replication'
    )

#delete task when finished
def delete_finished_task(client_dms, replicationTaskArn):
    #wait for the task to come to a running state, once it's in a running state, then it waits for the task to stop
    #else if go directly to replication stop, error occurs
    waiter = client_dms.get_waiter('replication_task_running')
    waiter.wait(
        Filters=[
            {
                'Name': 'replication-task-arn',
                'Values': [
                    replicationTaskArn,
                ]
            },
        ],
        WaiterConfig={
            'Delay': 15,
            'MaxAttempts': 60
        }
    )

    waiter = client_dms.get_waiter('replication_task_stopped')
    waiter.wait(
        Filters=[
            {
                'Name': 'replication-task-arn',
                'Values': [
                    replicationTaskArn,
                ]
            },
        ],
        WaiterConfig={
            'Delay': 15,
            'MaxAttempts': 100
        }
    )

    client_dms.delete_replication_task(ReplicationTaskArn=replicationTaskArn)


def transfer(client_s3):
    gcs_client = boto3.client(
        "s3",  # !just like that
        region_name='ap-southeast-1', aws_access_key_id = gcp_ID,   #create HMAC keys for a service account:https://cloud.google.com/storage/docs/authentication/managing-hmackeys#create
        aws_secret_access_key= gcp_Secret,
        endpoint_url="https://storage.googleapis.com"
    )

    client_s3.download_file(Bucket='s3-etl-lambobr', Key='project/books/LOAD00000001.csv',Filename='file.csv')
    gcs_client.upload_file(Filename='file.csv',Bucket='aws-etl',Key='aws.csv')


if __name__ == '__main__':
    client_s3, client_dms = init()  #initialize client
    s3_bucket(client_s3,S3BucketName)   #create s3 bucket (target)
    delete_existing_task(client_dms, ReplicationTaskName)   #delete old task if exists
    replicationTaskArn = create_task(client_dms, SourceEndpointArn, TargetEndpointArn, ReplicationInstanceArn, ReplicationTaskName) #create new task
    start_task(client_dms, replicationTaskArn) #start migration task
    delete_finished_task(client_dms, replicationTaskArn) #finally delete finished task
    transfer(client_s3)
