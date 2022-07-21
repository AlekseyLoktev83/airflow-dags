import os
from datetime import datetime

from airflow.models import DAG
from airflow.providers.microsoft.azure.operators.wasb_delete_blob import WasbDeleteBlobOperator
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator

PATH_TO_UPLOAD_FILE = '/etc/hosts'
DAG_ID = "azure_blob_test"

with DAG(
    DAG_ID,
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args={"container_name": "russiapetcarejupiter", "blob_name": "InterfaceTest"},
) as dag:
    upload = LocalFilesystemToWasbOperator(task_id="upload_file", file_path=PATH_TO_UPLOAD_FILE)

