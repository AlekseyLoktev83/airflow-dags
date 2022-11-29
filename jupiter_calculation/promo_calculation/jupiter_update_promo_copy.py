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
AVAILABILITY_ZONE_ID = 'ru-central1-a'
S3_BUCKET_NAME_FOR_JOB_LOGS = 'jupiter-app-test-storage'
BCP_SEPARATOR = '0x01'
CSV_SEPARATOR = '\u0001'
TAGS=["jupiter", "promo", "dev"]

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
    upload_path = f'{raw_path}/{execution_date}/'
    system_name = Variable.get("SystemName")
    last_upload_date = Variable.get("LastUploadDate")
    
    db_conn = BaseHook.get_connection(MSSQL_CONNECTION_NAME)
    bcp_parameters =  base64.b64encode(('-S {} -d {} -U {} -P {}'.format(db_conn.host, db_conn.schema, db_conn.login,db_conn.password)).encode()).decode()
    odbc_extras = mssql_scripts.get_odbc_extras_string(db_conn)
    bcp_import_parameters =  base64.b64encode((f'DRIVER=ODBC Driver 18 for SQL Server;SERVER={db_conn.host};DATABASE={db_conn.schema};UID={db_conn.login};PWD={db_conn.password};{odbc_extras}').encode()).decode()
    blocked_promo_output_path=f'{process_path}/BlockedPromo/'
    
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
                  "BlockedPromoOutputPath":blocked_promo_output_path,
                  }
    print(parameters)
    return parameters

@task
def create_child_dag_config(parameters:dict):
    conf={"parent_run_id":parameters["ParentRunId"],"parent_process_date":parameters["ProcessDate"],"schema":parameters["Schema"]}
    return conf

@task
def update_blocked_promo_table(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    result = odbc_hook.run(sql=f"""exec [{schema}].[AddBlockedPromo]""")
    print(result)

    return result

@task
def truncate_table(parameters:dict, entity):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    table_name = entity['TableName']
    result = odbc_hook.run(sql=f"""truncate table {table_name}""")
    print(result)

    return entity

@task
def generate_bcp_import_script(parameters:dict, entity):
    schema = parameters["Schema"]
    table_name = entity['TableName']
    src_path = entity['SrcPath']
    bcp_import_parameters=parameters['BcpImportParameters']
    script = f'/utils/bcp_import.sh {src_path} {bcp_import_parameters} \"{table_name}\" "1" '
    print(script)

    return script

@task
def generate_entity_list(parameters:dict):
    schema = parameters["Schema"]
    output_path=parameters['OutputPath']
    tables = [
              {'SrcPath':f'{output_path}/Promo/Promo.CSV/*.csv','TableName':f'{schema}.TEMP_PROMO'},
              {'SrcPath':f'{output_path}/PromoProduct/PromoProduct.CSV/*.csv','TableName':f'{schema}.TEMP_PROMOPRODUCT'},
              {'SrcPath':f'{output_path}/PromoSupportPromo/PromoSupportPromo.CSV/*.csv','TableName':f'{schema}.TEMP_PROMOSUPPORTPROMO'},
              {'SrcPath':f'{output_path}/ServiceInfo/ServiceInfo.CSV/*.csv','TableName':f'{schema}.ServiceInfo'},
              {'SrcPath':f'{output_path}/ProductChangeIncident/NewProductChangeIncident.CSV/*.csv','TableName':f'{schema}.TEMP_PRODUCTCHANGEINCIDENTS'},
              {'SrcPath':f'{output_path}/ChangesIncident/ChangesIncident.CSV/*.csv','TableName':f'{schema}.ChangesIncident'},
             ]
    return tables

with DAG(
    dag_id='jupiter_update_promo_copy',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=TAGS,
    render_template_as_native_obj=True,
    default_args={'retries': 2},	
) as dag:
# Get dag parameters from vault    
    parameters = get_parameters()
    trunc_tables = truncate_table.partial(parameters=parameters).expand(entity=generate_entity_list(parameters))

    upload_tables = BashOperator.partial(task_id="import_table",
                                       do_xcom_push=True,
                                      ).expand(bash_command=generate_bcp_import_script.partial(parameters=parameters).expand(entity=trunc_tables),
                                              )

