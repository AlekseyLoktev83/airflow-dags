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

# should be filled with appropriate ids

# Name of the datacenter where Dataproc cluster will be created
AVAILABILITY_ZONE_ID = 'ru-central1-b'

# Dataproc cluster jobs will produce logs in specified s3 bucket
S3_BUCKET_NAME_FOR_JOB_LOGS = 'jupiter-app-test-storage'


with DAG(
    'dataproc_test',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
) as dag:
    create_pyspark_job = DataprocCreatePysparkJobOperator(
        task_id='create_pyspark_job',
        cluster_id='c9qc9m3jccl8v7vigq10',
#        main_python_file_uri='hdfs:///user/hive/warehouse/src/JUPITER_ROLLING_VOLUMES_HDFS_FDM.py',
        main_python_file_uri='s3a://jupiter-app-test-storage/src/dataproc/JUPITER/MAIN.py',
#         python_file_uris=[
#             's3a://jupiter-app-test-storage/src/dataproc/SHARED/EXTRACT_SETTING2.py',
#         ],
        file_uris=[
            's3a://data-proc-public/jobs/sources/data/config.json',
        ],
#         archive_uris=[
#             's3a://data-proc-public/jobs/sources/data/country-codes.csv.zip',
#         ],
        args=[
            's3a://data-proc-public/jobs/sources/data/cities500.txt.bz2',
            f's3a://{S3_BUCKET_NAME_FOR_JOB_LOGS}/dataproc/job/results/${{JOB_ID}}',
        ],
#         jar_file_uris=[
#             's3a://data-proc-public/jobs/sources/java/dataproc-examples-1.0.jar',
#             's3a://data-proc-public/jobs/sources/java/icu4j-61.1.jar',
#             's3a://data-proc-public/jobs/sources/java/commons-lang-2.6.jar',
#         ],
        properties={
            'spark.submit.deployMode': 'cluster',
        },
        packages=['org.slf4j:slf4j-simple:1.7.30'],
        repositories=['https://repo1.maven.org/maven2'],
        exclude_packages=['com.amazonaws:amazon-kinesis-client'],
    )


    create_pyspark_job 
