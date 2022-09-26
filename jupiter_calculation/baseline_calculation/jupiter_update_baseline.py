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
AVAILABILITY_ZONE_ID = 'ru-central1-b'
S3_BUCKET_NAME_FOR_JOB_LOGS = 'jupiter-app-test-storage'
BCP_SEPARATOR = '0x01'
CSV_SEPARATOR = '\u0001'
TAGS=["jupiter", "baseline", "dev"]
BASELINE_ENTITY_NAME='BaseLine'
BASELINE_OUTPUT_DIR='BaseLine.CSV/*.csv'
NEW_BASELINE_OUTPUT_DIR='NewBaseLine.CSV/*.csv'

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
    file_name = dag_run.conf.get('FileName')
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
    bcp_parameters =  base64.b64encode(('-S {} -d {} -U {} -P {}'.format(db_conn.host, db_conn.schema, db_conn.login,db_conn.password)).encode()).decode()
    bcp_import_parameters =  base64.b64encode((f'DRIVER=ODBC Driver 18 for SQL Server;SERVER={db_conn.host};DATABASE={db_conn.schema};UID={db_conn.login};PWD={db_conn.password};Encrypt=no;').encode()).decode()
    baseline_output_path=f'{output_path}/BASELINE/{execution_date}/'
    
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
                  "FileName":file_name,
                  "CreateDate":create_date,
                  "BcpImportParameters":bcp_import_parameters,
                  "BaseLineOutputPath":baseline_output_path,
                  }
    print(parameters)
    return parameters

@task
def create_child_dag_config(parameters:dict):
    conf={"parent_run_id":parameters["ParentRunId"],"parent_process_date":parameters["ProcessDate"],"schema":parameters["Schema"]}
    return conf

@task
def disable_baseline(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    result = odbc_hook.run(sql=f"""exec [{schema}].[DisableBaseLine]""")
    print(result)

    return result

@task
def update_baseline(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    result = odbc_hook.run(sql=f"""exec [{schema}].[UpdateBaseline]""")
    print(result)

    return result

@task
def add_new_baseline(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    result = odbc_hook.run(sql=f"""exec [{schema}].[AddNewBaseline]""")
    print(result)

    return result

@task
def enable_baseline(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    result = odbc_hook.run(sql=f"""exec [{schema}].[EnableBaseLine]""")
    print(result)

    return result

@task
def truncate_temp_baseline(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    result = odbc_hook.run(sql=f"""truncate table [{schema}].[TEMP_BASELINE]""")
    print(result)

    return result

with DAG(
    dag_id='jupiter_update_baseline',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=TAGS,
    render_template_as_native_obj=True,
) as dag:
# Get dag parameters from vault    
    parameters = get_parameters()
    dis_baseline = disable_baseline(parameters)
    truncate_temp_baseline1 = truncate_temp_baseline(parameters)
    upload_baseline = BashOperator(task_id="upload_baseline",
                                 do_xcom_push=True,
                                 bash_command='/utils/bcp_import.sh {{ti.xcom_pull(task_ids="get_parameters",key="BaseLineOutputPath")}}{{params.OUT_DIR}} {{ti.xcom_pull(task_ids="get_parameters",key="BcpImportParameters")}} \"{{ti.xcom_pull(task_ids="get_parameters",key="Schema")}}.TEMP_BASELINE\" "1" ',
                                 params={'OUT_DIR':BASELINE_OUTPUT_DIR},  
                                )
    up_baseline=update_baseline(parameters)
    truncate_temp_baseline2 = truncate_temp_baseline(parameters)
    upload_new_baseline = BashOperator(task_id="upload_new_baseline",
                                 do_xcom_push=True,
                                 bash_command='/utils/bcp_import.sh {{ti.xcom_pull(task_ids="get_parameters",key="BaseLineOutputPath")}}{{params.OUT_DIR}} {{ti.xcom_pull(task_ids="get_parameters",key="BcpImportParameters")}} \"{{ti.xcom_pull(task_ids="get_parameters",key="Schema")}}.TEMP_BASELINE\" "1" ',
                                 params={'OUT_DIR':NEW_BASELINE_OUTPUT_DIR},  
                                )
    
    add_new_bs=add_new_baseline(parameters)
    enable_baseline = enable_baseline(parameters)
        
    dis_baseline >> truncate_temp_baseline1 >> upload_baseline >> up_baseline >> truncate_temp_baseline2 >> upload_new_baseline >> add_new_bs >> enable_baseline
