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
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from cloud_scripts.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
from airflow.providers.hashicorp.hooks.vault import VaultHook
from airflow.providers.http.operators.http import SimpleHttpOperator
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
import base64

import struct
from contextlib import closing


MSSQL_CONNECTION_NAME = 'odbc_jupiter'
HDFS_CONNECTION_NAME = 'webhdfs_default'
VAULT_CONNECTION_NAME = 'vault_default'
SSH_CONNECTION_NAME = 'ssh_jupiter'
REMOTE_HDFS_CONNECTION_NAME = 'webhdfs_atlas'

AVAILABILITY_ZONE_ID = 'ru-central1-a'
S3_BUCKET_NAME_FOR_JOB_LOGS = 'jupiter-app-test-storage'
BCP_SEPARATOR = '0x01'
CSV_SEPARATOR = '\u0001'
TAGS=["jupiter", "interface", "dev"]

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
    create_date = dag_run.conf.get('CreateDate')

    raw_path = Variable.get("RawPath")
    process_path = Variable.get("ProcessPath")
    output_path = Variable.get("OutputPath")
    white_list = Variable.get("WhiteList",default_var=None)
    black_list = Variable.get("BlackList",default_var=None)
    upload_path = f'{raw_path}/{execution_date}/'
    system_name = Variable.get("SystemName")
    last_upload_date = Variable.get("LastUploadDate")
    
    db_conn = BaseHook.get_connection(MSSQL_CONNECTION_NAME)
    remote_hdfs_conn = BaseHook.get_connection(REMOTE_HDFS_CONNECTION_NAME)
    print(remote_hdfs_conn)
    remote_hdfs_url = remote_hdfs_conn.get_uri()
    dst_dir = f'{raw_path}/SOURCES/BASELINE_ANAPLAN/'
    
    bcp_parameters =  base64.b64encode(('-S {} -d {} -U {} -P {}'.format(db_conn.host, db_conn.schema, db_conn.login,db_conn.password)).encode()).decode()
    parent_handler_id = dag_run.conf.get('parent_handler_id')
    handler_id = parent_handler_id if parent_handler_id else str(uuid.uuid4())
    
    interface_dst_path = f'{raw_path}/SOURCES/BASELINE/'
    interface_raw_path = f'{raw_path}/SOURCES/BASELINE_ANAPLAN/'
       
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
                  "ParentRunId":parent_run_id,
                  "CreateDate":create_date,
                  "HandlerId":handler_id,
                  "InterfaceDstPath":interface_dst_path,
                  "InterfaceRawPath":interface_raw_path,
                   "RemoteHdfsUrl":remote_hdfs_url,
                  "DstDir":dst_dir,
                   }
    print(parameters)
    return parameters

@task
def create_child_dag_config(parameters:dict):
    conf={"parent_run_id":parameters["ParentRunId"],"parent_process_date":parameters["ProcessDate"],"schema":parameters["Schema"]}
    return conf

@task
def generate_distcp_script(parameters:dict, entity):
    src_path = entity['SrcPath']
    dst_path = entity['DstPath']
    remote_hdfs_url = parameters['RemoteHdfsUrl']
    
    script = f'hdfs dfs -rm -r {dst_path};hadoop distcp -pbc {remote_hdfs_url}{src_path} hdfs://$(hdfs getconf -namenodes){dst_path} '
    return script

@task
def generate_entity_list(parameters:dict):
    raw_path=parameters['RawPath']
    dst_dir=parameters['DstDir'] 
    entities = [
              {'SrcPath':'/FILES/BASELINE_ANAPLAN/','DstPath':dst_dir},
             ]
    return entities

@task
def get_intermediate_file_metadata(parameters:dict):
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()
    src_path=parameters["InterfaceRawPath"]
    files = conn.list(src_path)
    
    entity = None
    filtered = [x for x in files if ".csv" in x]
    for file in filtered:
       entity = {'File':file,'SrcPath':f'{src_path}{file}'}
    
    return entity

@task(multiple_outputs=True)
def create_file_info(parameters:dict, entity):
    interface_dst_path=parameters["InterfaceDstPath"]
    src_path=entity["SrcPath"]
    current_process_date = pendulum.now()
    file_name=os.path.splitext(entity["File"])[0] + current_process_date.strftime("_%Y%m%d_%H%M%S.dat")
    dst_path=f'{interface_dst_path}{file_name}'
    copy_command = f'hadoop dfs -cp -f {src_path} {dst_path}'
    
    
    return {"File":entity["File"].replace(".csv",".dat"), "ProcessDate":current_process_date.isoformat(), "CopyCommand":copy_command}

@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def add_filebuffer_sp(parameters:dict, entity):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    result = odbc_hook.run(sql=f"""exec [Jupiter].[AddFileBuffer] @FileName = ? ,@ProcessDate = ?,  @HandlerId = ? """, parameters=(entity["File"],entity["ProcessDate"], parameters["HandlerId"]))
    print(result)

    return result

with DAG(
    dag_id='jupiter_incoming_file_collect',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=TAGS,
    render_template_as_native_obj=True,
) as dag:
# Get dag parameters from vault    
    parameters = get_parameters()
    copy_remote_to_intermediate = BashOperator.partial(task_id="copy_remote_to_intermediate",
                                       do_xcom_push=True,
                                      ).expand(bash_command=generate_distcp_script.partial(parameters=parameters).expand(entity=generate_entity_list(parameters)),
                                              )
    
    get_intermediate_file_metadata = get_intermediate_file_metadata(parameters)
    file_info = create_file_info(parameters=parameters, entity=get_intermediate_file_metadata)
    copy_file_to_target_folder = BashOperator(task_id="copy_file_to_target_folder",
                                       do_xcom_push=True,
                                      bash_command=file_info["CopyCommand"],
                                              )
    add_filebuffer_sp = add_filebuffer_sp(parameters=parameters, entity=file_info)
    
    copy_remote_to_intermediate >> get_intermediate_file_metadata >> file_info >> copy_file_to_target_folder >> add_filebuffer_sp

