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

import struct
from contextlib import closing


MSSQL_CONNECTION_NAME = 'odbc_jupiter'
HDFS_CONNECTION_NAME = 'webhdfs_default'
VAULT_CONNECTION_NAME = 'vault_default'
SSH_CONNECTION_NAME = 'ssh_jupiter'

AVAILABILITY_ZONE_ID = 'ru-central1-b'
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
    bcp_parameters = '-S {} -d {} -U {} -P {}'.format(db_conn.host, db_conn.schema, db_conn.login, db_conn.password)
    parent_handler_id = dag_run.conf.get('parent_handler_id')
    handler_id = parent_handler_id if parent_handler_id else str(uuid.uuid4())
    
    interface_sftp_path = Variable.get("InterfaceSftpPath")
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
                  "InterfaceSftpPath":interface_sftp_path,
                  "InterfaceRawPath":interface_raw_path,                  
                   }
    print(parameters)
    return parameters

@task
def create_child_dag_config(parameters:dict):
    conf={"parent_run_id":parameters["ParentRunId"],"parent_process_date":parameters["ProcessDate"],"schema":parameters["Schema"]}
    return conf

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
    process_path=parameters['ProcessPath']
    tables = [
              {'SrcPath':f'{process_path}/UPLOAD_FROM_SCENARIO/Promo/Promo.CSV/*.csv','TableName':f'{schema}.TEMP_SCENARIO_PROMO'},
              {'SrcPath':f'{process_path}/UPLOAD_FROM_SCENARIO/PromoProduct/PromoProduct.CSV/*.csv','TableName':f'{schema}.TEMP_SCENARIO_PROMOPRODUCT'},
              {'SrcPath':f'{process_path}/UPLOAD_FROM_SCENARIO/PromoSupportPromo/PromoSupportPromo.CSV/*.csv','TableName':f'{schema}.TEMP_SCENARIO_PROMOSUPPORTPROMO'},
              {'SrcPath':f'{process_path}/UPLOAD_FROM_SCENARIO/PromoProductsCorrection/PromoProductsCorrection.CSV/*.csv','TableName':f'{schema}.TEMP_SCENARIO_PROMOPRODUCTSCORRECTION'},
              {'SrcPath':f'{process_path}/UPLOAD_FROM_SCENARIO/PromoProductTree/PromoProductTree.CSV/*.csv','TableName':f'{schema}.TEMP_SCENARIO_PROMOPRODUCTTREE'},
              {'SrcPath':f'{process_path}/UPLOAD_FROM_SCENARIO/BTLPromo/BTLPromo.CSV/*.csv','TableName':f'{schema}.TEMP_SCENARIO_BTLPROMO'},
              {'SrcPath':f'{process_path}/UPLOAD_FROM_SCENARIO/BTL/BTL.CSV/*.csv','TableName':f'{schema}.TEMP_SCENARIO_BTL'},
              {'SrcPath':f'{process_path}/UPLOAD_FROM_SCENARIO/PromoSupport/PromoSupport.CSV/*.csv','TableName':f'{schema}.TEMP_SCENARIO_PROMOSUPPORT'},
             ]
    return tables

@task
def get_incoming_files_folder_metadata(parameters:dict):
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()
    src_path=parameters["InterfaceRawPath"]
    files = conn.list(src_path)
    
    entities = []
    for file in files:
        entities.append({'File':file,'SrcPath':f'{src_path}{file}'})
    
    return entities

@task
def copy_file_into_target_folder(parameters:dict, entity):
    interface_sftp_path=parameters["InterfaceSftpPath"]
    src_path=entity["SrcPath"]
    current_process_date = pendulum.now()
    file_name=os.path.splitext(entity["File"])[0] + current_process_date.strftime("_%Y%m%d_%H%M%S.dat")
    
    tmp_path=f'/tmp/{entity["File"]}'
    sftp_path=f'{interface_sftp_path}/{file_name}'
    
    ssh_hook=SSHHook(SSH_CONNECTION_NAME)
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    
    conn = hdfs_hook.get_conn()
    conn.download(src_path, tmp_path, overwrite=True)
    
    with ssh_hook.get_conn() as ssh_client:
     sftp_client = ssh_client.open_sftp()
     sftp_client.put(tmp_path,sftp_path)
    
    return {"File":entity["File"].replace(".csv",".dat"), "ProcessDate":current_process_date.isoformat()}

@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def add_filebuffer_sp(parameters:dict,entity):
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
    upload_files = copy_file_into_target_folder.partial(parameters=parameters).expand(entity=get_incoming_files_folder_metadata(parameters))
    add_filebuffer_sp.partial(parameters=parameters).expand(entity=upload_files)
#     upload_tables = BashOperator.partial(task_id="import_table",
#                                        do_xcom_push=True,
#                                       ).expand(bash_command=generate_bcp_import_script.partial(parameters=parameters).expand(entity=trunc_tables),
#                                               )
