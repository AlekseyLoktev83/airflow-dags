import datetime
import pendulum

from airflow import DAG, XComArg
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from cloud_scripts.custom_dataproc import  DataprocCreatePysparkJobOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
from airflow.providers.hashicorp.hooks.vault import VaultHook
from airflow.providers.ssh.hooks.ssh import SSHHook

import uuid
from io import StringIO
import urllib.parse
import subprocess

import cloud_scripts.mssql_scripts as mssql_scripts
import json
import pandas as pd
import glob
import os
from pathlib import Path
import glob
import base64


MSSQL_CONNECTION_NAME = 'odbc_jupiter'
HDFS_CONNECTION_NAME = 'webhdfs_default'
VAULT_CONNECTION_NAME = 'vault_default'
SSH_CONNECTION_NAME = 'ssh_jupiter'

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

TAGS=["jupiter", "main", "dev"]

def separator_convert_hex_to_string(sep):
    sep_map = {'0x01':'\x01'}
    return sep_map.get(sep, sep)

@task(multiple_outputs=True)
def get_parameters(**kwargs):
    ti = kwargs['ti']
    ds = kwargs['ds']
    dag_run = kwargs['dag_run']
    parent_process_date = dag_run.conf.get('parent_process_date')
    process_date = parent_process_date if parent_process_date else ds
    execution_date = kwargs['execution_date'].strftime("%Y/%m/%d")
    parent_run_id = dag_run.conf.get('parent_run_id')
    run_id = urllib.parse.quote_plus(parent_run_id) if parent_run_id else urllib.parse.quote_plus(kwargs['run_id'])
    
    schema = dag_run.conf.get('schema')
    upload_date = kwargs['logical_date'].strftime("%Y-%m-%d %H:%M:%S")

    raw_path = Variable.get("RawPath")
    process_path = Variable.get("ProcessPath")
    output_path = Variable.get("OutputPath")
    white_list = Variable.get("WhiteList",default_var=None)
    black_list = Variable.get("BlackList",default_var=None)
    upload_path = f'{raw_path}/{execution_date}/'
    system_name = Variable.get("SystemName")
    last_upload_date = Variable.get("LastUploadDate")
    
    parent_handler_id = dag_run.conf.get('parent_handler_id')
    handler_id = parent_handler_id if parent_handler_id else str(uuid.uuid4())
    
    logs_sftp_path = Variable.get("LogsSftpPath")
    logs_process_path = f'{process_path}/Logs/{handler_id}.csv'
    
    db_conn = BaseHook.get_connection(MSSQL_CONNECTION_NAME)
    bcp_parameters =  base64.b64encode(('-S {} -d {} -U {} -P {}'.format(db_conn.host, db_conn.schema, db_conn.login,db_conn.password)).encode()).decode()

    parameters = {"RawPath": raw_path,
                  "ProcessPath": process_path,
                  "OutputPath": output_path,
                  "WhiteList": white_list,
                  "BlackList": black_list,
                  "MaintenancePathPrefix":"{}{}{}_{}_".format(raw_path,"/#MAINTENANCE/",process_date,run_id),
                  "BcpParameters": bcp_parameters,
                  "UploadPath": upload_path,
                  "RunId":run_id,
                  "SystemName":system_name,
                  "LastUploadDate":last_upload_date,
                  "CurrentUploadDate":upload_date,
                  "ProcessDate":process_date,
                  "MaintenancePath":"{}{}".format(raw_path,"/#MAINTENANCE/"),
                  "Schema":schema,
                  "LogsSftpPath":logs_sftp_path,
                  "LogsProcessPath":logs_process_path,
                  "HandlerId":handler_id,
                  }
    print(parameters)
    return parameters

@task
def copy_hdfs_to_sftp(parameters:dict):
    logs_sftp_path=parameters["LogsSftpPath"]
    logs_process_path=parameters["LogsProcessPath"]
    file_name=f'{parameters["HandlerId"]}.txt'
    
    tmp_path=f'/tmp/'
    sftp_path=f'{logs_sftp_path}/{file_name}'
    dst_path = f'{logs_process_path}'
    
    ssh_hook=SSHHook(SSH_CONNECTION_NAME)
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    
    conn = hdfs_hook.get_conn()
    conn.download(dst_path, tmp_path, overwrite=True)
    
    files=glob.glob(f'{tmp_path}*.csv/*.csv')
    files = [f for f in files if os.path.isfile(f)]
    src_file_path = files[0]
    print(src_file_path)
    
    with ssh_hook.get_conn() as ssh_client:
     sftp_client = ssh_client.open_sftp()
     sftp_client.put(src_file_path,sftp_path)
    
    return True


with DAG(
    dag_id='jupiter_move_logs_to_sftp',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=TAGS,
    render_template_as_native_obj=True,
) as dag:
# Get dag parameters from vault    
    parameters = get_parameters()
    move_logs_to_sftp=copy_hdfs_to_sftp(parameters)
