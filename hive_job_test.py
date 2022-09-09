import os
import uuid
from datetime import datetime

from airflow import DAG
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreateHiveJobOperator,
    DataprocCreateMapReduceJobOperator,
    DataprocCreatePysparkJobOperator,
    DataprocCreateSparkJobOperator,
    DataprocDeleteClusterOperator,
)
AVAILABILITY_ZONE_ID = 'ru-central1-b'
S3_BUCKET_NAME_FOR_JOB_LOGS = 'jupiter-app-test-storage'



with DAG(
    'hive_job_test',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
) as dag:
    create_hive_query = DataprocCreateHiveJobOperator(
        task_id='create_hive_query',
        cluster_id='c9qc9m3jccl8v7vigq10',
        query='SELECT 1;',
    )

    create_hive_query_from_file = DataprocCreateHiveJobOperator(
        task_id='create_hive_query_from_file',
        cluster_id='c9qc9m3jccl8v7vigq10',
        query='insert into monitoring_output select * from  monitoring_output',
#         script_variables={
#             'CITIES_URI': 's3a://data-proc-public/jobs/sources/hive-001/cities/',
#             'COUNTRY_CODE': 'RU',
#         },
    )

    
