import datetime
import pendulum

from airflow import DAG, XComArg
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
# from airflow.providers.yandex.operators.yandexcloud_dataproc import  DataprocCreatePysparkJobOperator
from cloud_scripts.custom_dataproc import  DataprocCreatePysparkJobOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
from airflow.providers.hashicorp.hooks.vault import VaultHook

import uuid
from io import StringIO
import urllib.parse
import subprocess

import cloud_scripts.mssql_scripts as mssql_scripts
import json
import pandas as pd
import glob
import os


MSSQL_CONNECTION_NAME = 'odbc_default'
HDFS_CONNECTION_NAME = 'webhdfs_default'
VAULT_CONNECTION_NAME = 'vault_default'
AVAILABILITY_ZONE_ID = 'ru-central1-b'
S3_BUCKET_NAME_FOR_JOB_LOGS = 'jupiter-app-test-storage'
BCP_SEPARATOR = '0x01'
CSV_SEPARATOR = '\u0001'

MONITORING_DETAIL_DIR_PREFIX = 'MONITORING_DETAIL.CSV'
EXTRACT_ENTITIES_AUTO_FILE = 'EXTRACT_ENTITIES_AUTO.csv'
MONITORING_FILE = 'MONITORING.csv'
PARAMETERS_FILE = 'PARAMETERS.csv'

STATUS_FAILURE='FAILURE'
STATUS_COMPLETE='COMPLETE'
STATUS_PROCESS='PROCESS'

DAYS_TO_KEEP_OLD_FILES = 2

def separator_convert_hex_to_string(sep):
    sep_map = {'0x01':'\x01'}
    return sep_map.get(sep, sep)

@task(multiple_outputs=True)
def get_parameters(**kwargs):
    ti = kwargs['ti']
    ds = kwargs['ds']
    dag_run = kwargs['dag_run']
    process_date = ds
    execution_date = kwargs['execution_date'].strftime("%Y/%m/%d")
    parent_run_id = dag_run.conf.get('parent_run_id')
    run_id = parent_run_id if parent_run_id else urllib.parse.quote_plus(kwargs['run_id'])
    upload_date = kwargs['logical_date'].strftime("%Y-%m-%d %H:%M:%S")

    raw_path = Variable.get("RawPath")
    process_path = Variable.get("ProcessPath")
    output_path = Variable.get("OutputPath")
    white_list = Variable.get("WhiteList",default_var=None)
    black_list = Variable.get("BlackList",default_var=None)
    upload_path = f'{raw_path}/{execution_date}/'
    system_name = Variable.get("SystemName")
    last_upload_date = Variable.get("LastUploadDate")
    
    db_conn = BaseHook.get_connection(MSSQL_CONNECTION_NAME)
    bcp_parameters = '-S {} -d {} -U {} -P {}'.format(db_conn.host, db_conn.schema, db_conn.login, db_conn.password)

    parameters = {"RawPath": raw_path,
                  "ProcessPath": process_path,
                  "OutputPath": output_path,
                  "WhiteList": white_list,
                  "BlackList": black_list,
                  "MaintenancePathPrefix":"{}{}{}_{}_".format(raw_path,"/#MAINTENANCE/",ds,run_id),
                  "BcpParameters": bcp_parameters,
                  "UploadPath": upload_path,
                  "RunId":run_id,
                  "SystemName":system_name,
                  "LastUploadDate":last_upload_date,
                  "CurrentUploadDate":upload_date,
                  "ProcessDate":process_date,
                  "MaintenancePath":"{}{}".format(raw_path,"/#MAINTENANCE/"),
                  }
    print(parameters)
    return parameters

@task(task_id='save_parameters')
def save_parameters(parameters:dict):
    parameters_file_path=f'{parameters["MaintenancePathPrefix"]}{PARAMETERS_FILE}'

    temp_file_path =f'/tmp/{PARAMETERS_FILE}'
    df = pd.DataFrame(parameters.items(),columns=['Key', 'Value'])
    df.to_csv(temp_file_path, index=False, sep=CSV_SEPARATOR)
    
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()
    conn.upload(parameters_file_path,temp_file_path,overwrite=True)
    
    
    args = json.dumps({"MaintenancePathPrefix":parameters["MaintenancePathPrefix"],"ProcessDate":parameters["ProcessDate"]})
                                                                            
                                                                                            
    return [args]


with DAG(
    dag_id='jupiter_raw_qc',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["jupiter", "dev"],
    render_template_as_native_obj=True,
) as dag:
# Get dag parameters from vault    
    parameters = get_parameters()
    save_params = save_parameters(parameters)
    pyspark_job_qc = DataprocCreatePysparkJobOperator(
        task_id='create_pyspark_job',
        cluster_id='c9qc9m3jccl8v7vigq10',
#        main_python_file_uri='hdfs:///user/hive/warehouse/src/JUPITER_ROLLING_VOLUMES_HDFS_FDM.py',
        main_python_file_uri='s3a://jupiter-app-test-storage/src/dataproc/JUPITER/MAIN.py',
        python_file_uris=[
            's3a://jupiter-app-test-storage/src/dataproc/SHARED/EXTRACT_SETTING.py',
        ],
        file_uris=[
            's3a://data-proc-public/jobs/sources/data/config.json',
        ],
#         archive_uris=[
#             's3a://data-proc-public/jobs/sources/data/country-codes.csv.zip',
#         ],
        args=save_params,
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
    
#     save_params >> pyspark_job_qc

