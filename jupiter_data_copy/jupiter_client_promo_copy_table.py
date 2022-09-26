import datetime
import pendulum

from airflow import DAG, XComArg
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.yandex.operators.yandexcloud_dataproc import DataprocCreatePysparkJobOperator
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
import csv


MSSQL_CONNECTION_NAME = 'odbc_jupiter'
HDFS_CONNECTION_NAME = 'webhdfs_default'
VAULT_CONNECTION_NAME = 'vault_default'
AVAILABILITY_ZONE_ID = 'ru-central1-b'
S3_BUCKET_NAME_FOR_JOB_LOGS = 'jupiter-app-test-storage'
BCP_SEPARATOR = '0x01'

MONITORING_DETAIL_DIR_PREFIX = 'MONITORING_DETAIL.CSV'
EXTRACT_ENTITIES_AUTO_FILE = 'EXTRACT_ENTITIES_AUTO.csv'
RAW_SCHEMA_FILE = 'RAW_SCHEMA.csv'
MONITORING_FILE = 'MONITORING.csv'
TAGS=["jupiter", "promo","dev"]

STATUS_FAILURE = 'FAILURE'
STATUS_COMPLETE = 'COMPLETE'
STATUS_PROCESS = 'PROCESS'

DAYS_TO_KEEP_OLD_FILES = 2
CSV_SEPARATOR = '\u0001'


def separator_convert_hex_to_string(sep):
    sep_map = {'0x01': '\x01'}
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
    run_id = urllib.parse.quote_plus(
        parent_run_id) if parent_run_id else urllib.parse.quote_plus(kwargs['run_id'])
    upload_date = kwargs['logical_date'].strftime("%Y-%m-%d %H:%M:%S")

    raw_path = Variable.get("RawPath")
    process_path = Variable.get("ProcessPath")
    output_path = Variable.get("OutputPath")
    black_list = Variable.get("BlackList", default_var=None)
    extract_schema = dag_run.conf.get('extract_schema')    
    white_list = Variable.get("PromoCopyEntites", default_var=None)
    system_name = Variable.get("SystemName")
    last_upload_date = Variable.get("LastUploadDate")
    schema = dag_run.conf.get('schema')
    extract_schema = dag_run.conf.get('extract_schema')
    client_prefix = dag_run.conf.get('client_prefix') 
    client_name = dag_run.conf.get('client_name')
    drop_files_if_errors = dag_run.conf.get('drop_files_if_errors')    
    copy_mode = dag_run.conf.get('copy_mode')
    source_path = dag_run.conf.get('source_path')
    
    parent_handler_id = dag_run.conf.get('parent_handler_id')   
    handler_id = parent_handler_id if parent_handler_id else str(uuid.uuid4())
    timestamp_field = pendulum.now().strftime("%Y%m%d%H%M%S")
    
    client_promo_dir = Variable.get("ClientPromoDir")
    upload_path = source_path    
    
    db_conn = BaseHook.get_connection(MSSQL_CONNECTION_NAME)
    bcp_parameters =  base64.b64encode(('-S {} -d {} -U {} -P {}'.format(db_conn.host, db_conn.schema, db_conn.login,db_conn.password)).encode()).decode()


    parameters = {"RawPath": raw_path,
                  "ProcessPath": process_path,
                  "OutputPath": output_path,
                  "WhiteList": white_list,
                  "BlackList": black_list,
                  "MaintenancePathPrefix": "{}{}{}_{}_".format(raw_path, "/#MAINTENANCE/", process_date, run_id),
                  "BcpParameters": bcp_parameters,
                  "UploadPath": upload_path,
                  "RunId": run_id,
                  "SystemName": system_name,
                  "LastUploadDate": last_upload_date,
                  "CurrentUploadDate": upload_date,
                  "ProcessDate": process_date,
                  "MaintenancePath": "{}{}".format(raw_path, "/#MAINTENANCE/"),
                  "Schema":schema,
                  "ExtractSchema":extract_schema,
                  "ClientPrefix":client_prefix,
                  "ClientName":client_name,
                  "DropFilesIfErrors":drop_files_if_errors,
                  "CopyMode":copy_mode,
                  "HandlerId":handler_id,
                  "TimestampField":timestamp_field,
                  "SourcePath": source_path,
                  }
    print(parameters)
    return parameters


@task
def generate_schema_query(parameters: dict):
    query = mssql_scripts.generate_db_schema_query(
        white_list=parameters['WhiteList'], black_list=parameters['BlackList'])

    return query

@task
def copy_data_db_to_hdfs(query, dst_dir, dst_file):

    dst_path = f"{dst_dir}{dst_file}"
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()

    df = odbc_hook.get_pandas_df(query)
    df.to_csv(f'/tmp/{dst_file}', index=False, sep=CSV_SEPARATOR)
    conn.upload(dst_path, f'/tmp/{dst_file}',overwrite=True)

    return True


@task
def generate_upload_script(prev_task, src_dir, src_file, upload_path, bcp_parameters, current_upload_date, last_upload_date):
    src_path = f"{src_dir}{src_file}"
    tmp_path = f"/tmp/{src_file}"
    print(src_path)

    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()
    conn.download(src_path, tmp_path)

    entities_df = mssql_scripts.generate_table_select_query(
        current_upload_date, last_upload_date, tmp_path)
    entities_json = json.loads(entities_df.to_json(orient="records"))

    tmp_dst_path = f"/tmp/{EXTRACT_ENTITIES_AUTO_FILE}"
    dst_path = f"{src_dir}{EXTRACT_ENTITIES_AUTO_FILE}"

    del entities_df['Extraction']
    entities_df.to_csv(tmp_dst_path, index=False, sep=CSV_SEPARATOR)
    conn.upload(dst_path, tmp_dst_path,overwrite=True)

    return entities_json


@task
def generate_bcp_script(upload_path, bcp_parameters, entities):
    scripts = []
    for entity in entities:
        script = '/utils/exec_query.sh "{}" {}{}.{}.csv "{}" {} {} "{}" '.format(entity["Extraction"].replace("\'\'", "\'\\'").replace(
            "\n", " "), upload_path, entity["Schema"], entity["EntityName"], bcp_parameters, BCP_SEPARATOR, entity["Schema"], entity["Columns"].replace(",", separator_convert_hex_to_string(BCP_SEPARATOR)))
        scripts.append(script)

    return scripts


with DAG(
    dag_id='jupiter_client_promo_copy_table',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=TAGS,
    render_template_as_native_obj=True,
) as dag:
    # Get dag parameters from vault
    parameters = get_parameters()
#     Generate schema extraction query
    schema_query = generate_schema_query(parameters)
    
    extract_schema = copy_data_db_to_hdfs(
        schema_query, parameters["MaintenancePathPrefix"], RAW_SCHEMA_FILE)
    
    upload_tables = BashOperator.partial(task_id="upload_tables", do_xcom_push=True).expand(
        bash_command=generate_bcp_script(
            upload_path=parameters["UploadPath"], bcp_parameters=parameters["BcpParameters"], 
            entities=generate_upload_script(
        extract_schema, parameters["MaintenancePathPrefix"], 
                RAW_SCHEMA_FILE,
                parameters["UploadPath"],
                parameters["BcpParameters"],
                parameters["CurrentUploadDate"],
                parameters["LastUploadDate"])
                                     ),
    )

    cleanup = BashOperator(
        task_id='cleanup',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        bash_command='/utils/hdfs_delete_old_files.sh {{ti.xcom_pull(task_ids="get_parameters",key="MaintenancePath")}} {{params.days_to_keep_old_files}} ',
        params={'days_to_keep_old_files': DAYS_TO_KEEP_OLD_FILES},
    )

    upload_tables >> cleanup
