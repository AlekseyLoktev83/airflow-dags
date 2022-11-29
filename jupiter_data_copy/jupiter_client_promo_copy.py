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
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
from airflow.providers.hashicorp.hooks.vault import VaultHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.email import EmailOperator

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
AVAILABILITY_ZONE_ID = 'ru-central1-a'
S3_BUCKET_NAME_FOR_JOB_LOGS = 'jupiter-app-test-storage'
BCP_SEPARATOR = '0x01'
CSV_SEPARATOR = '\u0001'
TAGS=["jupiter", "promo", "dev"]
PARAMETERS_FILE = 'PARAMETERS.csv'
COPY_MODE_DATABASE='Database'

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
    
    extract_schema = dag_run.conf.get('extract_schema')
    client_prefix = dag_run.conf.get('client_prefix') 
    client_name = dag_run.conf.get('client_name')
    drop_files_if_errors = dag_run.conf.get('drop_files_if_errors')    
    copy_mode = dag_run.conf.get('copy_mode')
    source_path = dag_run.conf.get('source_path')
    emails = dag_run.conf.get('emails')
    id = dag_run.conf.get('Id')
    
    parent_handler_id = dag_run.conf.get('parent_handler_id')   
    handler_id = parent_handler_id if parent_handler_id else str(uuid.uuid4())
    timestamp_field = pendulum.now().strftime("%Y%m%d%H%M%S")
    
    client_promo_dir = Variable.get("ClientPromoDir")
    upload_path = f'{raw_path}/{ client_promo_dir}/{client_prefix}_{timestamp_field}'   

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
                  "UploadPath": upload_path,
                  "ParentRunId":parent_run_id,
                  "ExtractSchema":extract_schema,
                  "ClientPrefix":client_prefix,
                  "ClientName":client_name,
                  "DropFilesIfErrors":drop_files_if_errors,
                  "CopyMode":copy_mode,
                  "HandlerId":handler_id,
                  "TimestampField":timestamp_field,
                  "SourcePath": source_path,
                  "Emails":emails,
				  "Id":id,
                  }
    print(parameters)
    return parameters

@task
def create_child_dag_config(parameters:dict):
    conf={"parent_run_id":parameters["ParentRunId"],
          "parent_process_date":parameters["ProcessDate"],
          "schema":parameters["Schema"],
          "client_prefix":parameters["ClientPrefix"],
          "client_name":parameters["ClientName"],
          "source_path":parameters["SourcePath"],
         }
    return conf

@task  
def set_client_upload_processing_flag_up(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    result = odbc_hook.run(sql=f"""exec [{schema}].[DLSetClientUploadFlag] @Prefix = ? ,@Name = ?,  @Flag = ? """, parameters=(parameters["ClientPrefix"],parameters["ClientName"], 1))
    print(result)

    return result 

@task
def create_client_upload_wait_handler(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    result = odbc_hook.run(sql=f"""exec [{schema}].[DLCreateClientUploadWaitHandler] @Prefix = ? ,@HandlerId = ? """, parameters=(parameters["ClientName"],parameters["HandlerId"]))
    print(result)

    return result

def _if_load_from_database(**kwargs):
    return ['trigger_jupiter_client_promo_copy_table'] if kwargs['input'] == COPY_MODE_DATABASE else ['copy_from_existing']

@task(trigger_rule=TriggerRule.NONE_FAILED)  
def set_client_upload_processing_flag_complete(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    result = odbc_hook.run(sql=f"""exec [{schema}].[DLSetClientUploadFlag] @Prefix = ? ,@Name = ?,  @Flag = ? """, parameters=(parameters["ClientPrefix"],parameters["ClientName"], 0))
    print(result)

    return result

@task(trigger_rule=TriggerRule.ONE_FAILED)  
def set_client_upload_processing_flag_error(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    result = odbc_hook.run(sql=f"""exec [{schema}].[DLSetClientUploadFlag] @Prefix = ? ,@Name = ?,  @Flag = ? """, parameters=(parameters["ClientPrefix"],parameters["ClientName"], 2))
    print(result)

    return result 
	
@task(trigger_rule=TriggerRule.NONE_FAILED)  
def set_copy_task_status_complete(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    result = odbc_hook.run(sql=f"""exec [{schema}].[UpdateScenarioCopyTaskStatus] @Success = ? , @TaskId = ? """, parameters=(1,parameters["Id"]))
    print(result)

    return result
	
@task(trigger_rule=TriggerRule.ONE_FAILED)  
def set_copy_task_status_error(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    result = odbc_hook.run(sql=f"""exec [{schema}].[UpdateScenarioCopyTaskStatus] @Success = ? , @TaskId = ? """, parameters=(0,parameters["Id"]))
    print(result)

    return result	

def _check_drop_data_if_failure(**kwargs):
    return kwargs['drop_files_if_errors']


with DAG(
    dag_id='jupiter_client_promo_copy',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=TAGS,
    render_template_as_native_obj=True,
    default_args={'retries': 2},	
) as dag:
# Get dag parameters from vault    
    parameters = get_parameters()
    
    child_dag_config = create_child_dag_config(parameters)
    
    set_client_upload_processing_flag_up = set_client_upload_processing_flag_up(parameters)
    create_client_upload_wait_handler = create_client_upload_wait_handler(parameters)
    
    if_load_from_database = BranchPythonOperator(
        task_id='if_load_from_database',
        python_callable=_if_load_from_database,
        op_kwargs={'input': parameters['CopyMode']},
    )
    
    trigger_jupiter_client_promo_copy_table = TriggerDagRunOperator(
        task_id="trigger_jupiter_client_promo_copy_table",
        trigger_dag_id="jupiter_client_promo_copy_table",  
        conf='{{ti.xcom_pull(task_ids="create_child_dag_config")}}',
        wait_for_completion = True,
    )
    
    copy_from_existing = BashOperator(
        task_id='copy_from_existing',
        bash_command='hadoop dfs -cp {{ti.xcom_pull(task_ids="get_parameters",key="SourcePath")}} {{ti.xcom_pull(task_ids="get_parameters",key="UploadPath")}} ',
    )
    
    # jupiter_send_copy_successful_notification = EmailOperator( 
          # task_id='jupiter_send_copy_successful_notification', 
          # to='{{ti.xcom_pull(task_ids="get_parameters")["Emails"]}}', 
          # subject='Copy promo for client {{ti.xcom_pull(task_ids="get_parameters")["ClientName"]}}', 
          # html_content='Promo for client {{ti.xcom_pull(task_ids="get_parameters")["ClientName"]}} copied successful.',
          # trigger_rule=TriggerRule.NONE_FAILED,
    # )
    
    set_client_upload_processing_flag_complete = set_client_upload_processing_flag_complete(parameters)
    set_client_upload_processing_flag_error = set_client_upload_processing_flag_error(parameters)
	
    set_copy_task_status_complete = set_copy_task_status_complete(parameters)
    set_copy_task_status_error = set_copy_task_status_error(parameters)	
    
    check_drop_data_if_failure = ShortCircuitOperator(
        task_id='check_drop_data_if_failure',
        python_callable=_check_drop_data_if_failure,
        op_kwargs={'drop_files_if_errors': parameters["DropFilesIfErrors"]},
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    
    drop_uploaded_data = BashOperator(
        task_id='drop_uploaded_data',
        bash_command='hadoop dfs -rm -r {{ti.xcom_pull(task_ids="get_parameters",key="UploadPath")}} ',
    )
    
    # jupiter_send_copy_error_notification = EmailOperator( 
          # task_id='jupiter_send_copy_error_notification', 
          # to='{{ti.xcom_pull(task_ids="get_parameters")["Emails"]}}', 
          # subject='Error copy promo for client {{ti.xcom_pull(task_ids="get_parameters")["ClientName"]}}', 
          # html_content='Promo for client {{ti.xcom_pull(task_ids="get_parameters")["ClientName"]}} copied with errors. Please contact administrator.',
          # trigger_rule=TriggerRule.ONE_FAILED,
    # )
    
    child_dag_config >> set_client_upload_processing_flag_up >> create_client_upload_wait_handler >> if_load_from_database >> trigger_jupiter_client_promo_copy_table >> [set_client_upload_processing_flag_complete,set_client_upload_processing_flag_error,set_copy_task_status_complete,set_copy_task_status_error]
    child_dag_config >> set_client_upload_processing_flag_up >> create_client_upload_wait_handler >> if_load_from_database >> trigger_jupiter_client_promo_copy_table >> check_drop_data_if_failure >> drop_uploaded_data
    child_dag_config >> set_client_upload_processing_flag_up >> create_client_upload_wait_handler >> if_load_from_database >> copy_from_existing >> [set_client_upload_processing_flag_complete,set_client_upload_processing_flag_error,set_copy_task_status_complete,set_copy_task_status_error]
    child_dag_config >> set_client_upload_processing_flag_up >> create_client_upload_wait_handler >> if_load_from_database >> copy_from_existing >> check_drop_data_if_failure >> drop_uploaded_data
