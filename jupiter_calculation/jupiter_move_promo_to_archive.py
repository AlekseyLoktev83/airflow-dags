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
TAGS=["jupiter", "main", "dev"]

BLOCKED_PROMO_OUTPUT_DIR='BlockedPromo.CSV/*.csv'

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
    raw_archive_path = f'{raw_path}/SOURCES/ARCHIVE/JUPITER/{execution_date}/'
    output_archive_path = f'{output_path}/ARCHIVE/JUPITER/{execution_date}/'
    system_name = Variable.get("SystemName")
    last_upload_date = Variable.get("LastUploadDate")
    
    db_conn = BaseHook.get_connection(MSSQL_CONNECTION_NAME)
    bcp_parameters =  base64.b64encode(('-S {} -d {} -U {} -P {}'.format(db_conn.host, db_conn.schema, db_conn.login,db_conn.password)).encode()).decode()
    bcp_import_parameters =  base64.b64encode((f'DRIVER=ODBC Driver 18 for SQL Server;SERVER={db_conn.host};DATABASE={db_conn.schema};UID={db_conn.login};PWD={db_conn.password};Encrypt=no;').encode()).decode()
    blocked_promo_output_path=f'{process_path}/BlockedPromo/'
    
    parameters = {"RawPath": raw_path,
                  "ProcessPath": process_path,
                  "OutputPath": output_path,
                  "WhiteList": white_list,
                  "BlackList": black_list,
                  "MaintenancePathPrefix":"{}{}{}_{}_".format(raw_path,"/#MAINTENANCE/",process_date,run_id),
                  "BcpParameters": bcp_parameters,
                  "RawArchivePath": raw_archive_path,
                  "OutputArchivePath": output_archive_path,
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
                  "BlockedPromoOutputPath":blocked_promo_output_path,
                  }
    print(parameters)
    return parameters


@task
def generate_copy_script(parameters:dict):
    output_path=parameters['OutputPath']
    raw_path=parameters['RawPath']
    output_archive_path=parameters['OutputArchivePath']
    raw_archive_path=parameters['RawArchivePath']
    
    entities = [
              {'SrcPath':f'{output_path}/Promo/Promo.CSV/*','DstPath':f'{output_archive_path}Promo.CSV'},
              {'SrcPath':f'{output_path}/PromoProduct/PromoProduct.CSV/*','DstPath':f'{output_archive_path}PromoProduct.CSV'},
              {'SrcPath':f'{output_path}/PromoSupportPromo/PromoSupportPromo.CSV/*','DstPath':f'{output_archive_path}PromoSupportPromo.CSV'},
              {'SrcPath':f'{raw_path}/SOURCES/JUPITER/Promo/*','DstPath':f'{raw_archive_path}Promo'},
              {'SrcPath':f'{raw_path}/SOURCES/JUPITER/PromoProduct/*','DstPath':f'{raw_archive_path}PromoProduct'},
              {'SrcPath':f'{raw_path}/SOURCES/JUPITER/PromoSupportPromo/*','DstPath':f'{raw_archive_path}PromoSupportPromo'},
        
             ]
    
    scripts = []
    for entity in entities:
        script = f'hadoop dfs -mkdir -p {entity["DstPath"]} ; hadoop dfs -cp -f {entity["SrcPath"]} {entity["DstPath"]} '
        scripts.append(script)

    return scripts

with DAG(
    dag_id='jupiter_move_promo_to_archive',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=TAGS,
    render_template_as_native_obj=True,
) as dag:
# Get dag parameters from vault    
    parameters = get_parameters()

    copy_entities = BashOperator.partial(task_id="copy_entities", do_xcom_push=True).expand(
        bash_command=generate_copy_script(parameters),
    )
    
    parameters >> copy_entities
