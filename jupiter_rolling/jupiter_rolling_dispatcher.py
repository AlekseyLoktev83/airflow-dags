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
TAGS=["jupiter", "rolling", "dev"]
PARAMETERS_FILE = 'PARAMETERS.csv'

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
    rolling_day = Variable.get("RollingDay")
    
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
                  "ParentRunId":parent_run_id,
                  "FileName":file_name,
                  "CreateDate":create_date,
                  "RollingDay":rolling_day,
                  }
    print(parameters)
    return parameters

def _is_rolling_day(**kwargs):
    return int(kwargs['rolling_day']) == pendulum.today().day_of_week
with DAG(
    dag_id='jupiter_rolling_dispatcher',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=TAGS,
    render_template_as_native_obj=True,
) as dag:
# Get dag parameters from vault    
    parameters = get_parameters()
    
   
    trigger_jupiter_orders_delivery_fdm = TriggerDagRunOperator(
        task_id="trigger_jupiter_orders_delivery_fdm",
        trigger_dag_id="jupiter_orders_delivery_fdm",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":"{{dag_run.conf.get('schema')}}"},
        wait_for_completion = True,
    )
    
    trigger_jupiter_rolling_volumes_fdm = TriggerDagRunOperator(
        task_id="trigger_jupiter_rolling_volumes_fdm",
        trigger_dag_id="jupiter_rolling_volumes_fdm",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":"{{dag_run.conf.get('schema')}}"},
        wait_for_completion = True,
    )
        
    trigger_jupiter_update_rolling = TriggerDagRunOperator(
        task_id="trigger_jupiter_update_rolling",
        trigger_dag_id="jupiter_update_rolling",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":"{{dag_run.conf.get('schema')}}"},
        wait_for_completion = True,
    )
    
    trigger_jupiter_rolling_success_notify = TriggerDagRunOperator(
        task_id="trigger_jupiter_rolling_success_notify",
        trigger_dag_id="jupiter_rolling_notify",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":"{{dag_run.conf.get('schema')}}","message":"Orders/delivery and rolling building has been completed successfully."},
        wait_for_completion = True,
    )
    
    trigger_jupiter_rolling_failure_notify = TriggerDagRunOperator(
        task_id="trigger_jupiter_rolling_failure_notify",
        trigger_dag_id="jupiter_rolling_notify",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":"{{dag_run.conf.get('schema')}}","message":"Orders/delivery and rolling building failed"},
        wait_for_completion = True,
        trigger_rule=TriggerRule.ONE_FAILED,
    )     
    
    trigger_jupiter_orders_failure_notify = TriggerDagRunOperator(
        task_id="trigger_jupiter_orders_failure_notify",
        trigger_dag_id="jupiter_rolling_notify",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":"{{dag_run.conf.get('schema')}}","message":"Orders/delivery building failed"},
        wait_for_completion = True,
        trigger_rule=TriggerRule.ONE_FAILED,
    )  
  
    
    check_rollingday = ShortCircuitOperator(
        task_id='check_rollingday',
        python_callable=_is_rolling_day,
        op_kwargs={'rolling_day': parameters["RollingDay"]},
    )
    
       

    parameters >> trigger_jupiter_orders_delivery_fdm >> check_rollingday >> trigger_jupiter_rolling_volumes_fdm >> trigger_jupiter_update_rolling >> [trigger_jupiter_rolling_success_notify,trigger_jupiter_rolling_failure_notify]
    parameters >> trigger_jupiter_orders_delivery_fdm >> trigger_jupiter_orders_failure_notify
