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


date = '{{ execution_date }}'
request_body = {
  "execution_date": date
}
json_body = json.dumps(request_body)



def separator_convert_hex_to_string(sep):
    sep_map = {'0x01':'\x01'}
    return sep_map.get(sep, sep)

def handle_datetimeoffset(dto_value):
    tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 0, -6, 0)
    tweaked = [tup[i] // 100 if i == 6 else tup[i] for i in range(len(tup))]
    return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:07d} {:+03d}:{:02d}".format(*tweaked)

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
                  "FileName":file_name,
                  }
    print(parameters)
    return parameters

@task
def get_unprocessed_baseline_files(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    converters = [(-155, handle_datetimeoffset)]
    result = mssql_scripts.get_records(odbc_hook,sql=f"""exec [{schema}].[GetUnprocessedBaselineFiles]""",output_converters=converters)
    return result

@task
def create_night_processing_wait_handler(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    result = odbc_hook.run(sql=f"""exec [{schema}].[CreateNightProcessingWaitHandler]""")
    print(result)

    return result

@task
def generate_upload_script(parameters:dict):
    query = mssql_scripts.generate_db_schema_query(white_list=[f'{parameters["Schema"]}.BaseLine'], black_list=parameters['BlackList'])
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    
    db_schema_df = odbc_hook.get_pandas_df(query)
    db_schema_buf = StringIO()
    db_schema_df.to_csv(db_schema_buf, index=False, sep=CSV_SEPARATOR)
      
    entities_df = mssql_scripts.generate_table_select_query(
        parameters["CurrentUploadDate"], parameters["LastUploadDate"], db_schema_buf)
    entities_json = json.loads(entities_df.to_json(orient="records"))

    return entities_json  

with DAG(
    dag_id='jupiter_process_baseline',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=TAGS,
    render_template_as_native_obj=True,
) as dag:
# Get dag parameters from vault    
    parameters = get_parameters()
    upload_script = generate_upload_script(parameters)
#     unprocessed_baseline_files = get_unprocessed_baseline_files(parameters)
    
   
#     trigger_jupiter_process_baseline = TriggerDagRunOperator.partial(task_id="trigger_jupiter_process_baseline",
#                                                                     wait_for_completion = True,
#                                                                      trigger_dag_id="jupiter_process_baseline",
#                                                                     ).expand(conf=["A","B"],
#     )
