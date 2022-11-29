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
TAGS=["jupiter", "copy", "dev"]
BASELINE_ENTITY_NAME='BaseLine'
PARAMETERS_FILE = 'PARAMETERS.csv'

MONITORING_FILE = 'MONITORING.csv'
STATUS_FAILURE = 'FAILURE'
STATUS_COMPLETE = 'COMPLETE'
STATUS_PROCESS = 'PROCESS'

DAYS_TO_KEEP_OLD_FILES = 14

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
 
    dag = kwargs['dag']
    
    entity_output_dir = f'{output_path}/UNIVERSALCATALOG/MARS_UNIVERSAL_PETCARE_MATERIALS.CSV/*.csv'
    cluster_id = Variable.get("JupiterDataprocClusterId")
    
    parameters = {"RawPath": raw_path,
                  "ProcessPath": process_path,
                  "OutputPath": output_path,
                  "WhiteList": white_list,
                  "BlackList": black_list,
                  "MaintenancePathPrefix":"{}{}{}_{}_".format(output_path,"/#MAINTENANCE/",process_date,run_id),
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
                  "DagId":dag.dag_id,
                  "DateDir":execution_date,
                  "StartDate":pendulum.now().isoformat(),
                  "BcpImportParameters":bcp_import_parameters,
                  "EntityOutputDir":entity_output_dir,
                  "JupiterDataprocClusterId":cluster_id,
                  }
    print(parameters)
    return parameters

@task(task_id='save_parameters')
def save_parameters(parameters:dict):
    parameters_file_path=f'{parameters["MaintenancePathPrefix"]}{PARAMETERS_FILE}'

    temp_file_path =f'/tmp/{PARAMETERS_FILE}'
    df = pd.DataFrame(parameters.items(),columns=['Key', 'Value'])
    df.to_csv(temp_file_path, index=False, sep=CSV_SEPARATOR)
    
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()
    conn.upload(parameters_file_path,temp_file_path,overwrite=True)
    
    
    args = json.dumps({"MaintenancePathPrefix":parameters["MaintenancePathPrefix"],"ProcessDate":parameters["ProcessDate"],"Schema":parameters["Schema"],"PipelineName":parameters["DagId"]})
                                                                            
                                                                                            
    return [args]

def _update_output_monitoring(parameters:dict,prev_task_result):
    monitoring_file_path = f'{parameters["MaintenancePathPrefix"]}{MONITORING_FILE}'
    entity_path = f'{parameters["OutputPath"]}/{parameters["DagId"]}/MARS_UNIVERSAL_PETCARE_MATERIALS.CSV'
    end_date = pendulum.now()
    start_date = pendulum.parse(parameters["StartDate"])
    duration = end_date.diff(start_date).in_seconds()

    temp_file_path = f'/tmp/{MONITORING_FILE}'
    df = pd.DataFrame([{'PipelineRunId': parameters["RunId"],
                        'EntityName':'MARS_UNIVERSAL_PETCARE_MATERIALS.CSV',
                        'StartDate': pendulum.now(),
                        'Status': STATUS_COMPLETE if prev_task_result else STATUS_FAILURE,
                        'TargetPath': entity_path,
                        'TargetFormat':'CSV',
                        'Duration':duration, 
                        }])
    df.to_csv(temp_file_path, index=False, sep=CSV_SEPARATOR)

    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()
    conn.upload(monitoring_file_path, temp_file_path, overwrite=True)

    return True

@task(task_id="update_output_monitoring_failure",trigger_rule=TriggerRule.ONE_FAILED)
def update_output_monitoring_failure(parameters:dict):
    _update_output_monitoring(parameters,False)

@task(task_id="update_output_monitoring_success")
def update_output_monitoring_success(parameters:dict):
    _update_output_monitoring(parameters,True)
    
@task
def truncate_table(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    result = odbc_hook.run(sql=f"""truncate table [{schema}].[MARS_UNIVERSAL_PETCARE_MATERIALS]""")
    print(result)

    return result

with DAG(
    dag_id='jupiter_materials_copy',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=TAGS,
    render_template_as_native_obj=True,
) as dag:
# Get dag parameters from vault    
    parameters = get_parameters()
    save_params = save_parameters(parameters)
    truncate_table = truncate_table(parameters)
    build_model = DataprocCreatePysparkJobOperator(
        task_id='build_model',
        cluster_id=parameters['JupiterDataprocClusterId'],
        main_python_file_uri='hdfs:///SRC/JUPITER/UNIVERSALCATALOG/MATERIALS_FILTER.py',
        python_file_uris=[
            'hdfs:///SRC/SHARED/EXTRACT_SETTING.py',
            'hdfs:///SRC/SHARED/SUPPORT_FUNCTIONS.py',
        ],
        file_uris=[
            's3a://data-proc-public/jobs/sources/data/config.json',
        ],
        args=save_params,
        properties={
            'spark.submit.deployMode': 'cluster'
        },
        packages=['org.slf4j:slf4j-simple:1.7.30'],
        repositories=['https://repo1.maven.org/maven2'],
        exclude_packages=['com.amazonaws:amazon-kinesis-client'],
    )
    
    copy_output_data_to_db = BashOperator(task_id="copy_output_data_to_db",
                                 do_xcom_push=True,
                                 bash_command='/utils/bcp_import.sh {{ti.xcom_pull(task_ids="get_parameters",key="EntityOutputDir")}} {{ti.xcom_pull(task_ids="get_parameters",key="BcpImportParameters")}} \"{{ti.xcom_pull(task_ids="get_parameters",key="Schema")}}.MARS_UNIVERSAL_PETCARE_MATERIALS\" "1" ',
                                )
    
    cleanup = BashOperator(
        task_id='cleanup',
        trigger_rule=TriggerRule.ALL_DONE,
        bash_command='/utils/hdfs_delete_old_files.sh {{ti.xcom_pull(task_ids="get_parameters",key="MaintenancePath")}} {{params.days_to_keep_old_files}} ',
        params={'days_to_keep_old_files': DAYS_TO_KEEP_OLD_FILES},
    )
    
    mon_success = update_output_monitoring_success(parameters)
    mon_failure = update_output_monitoring_failure(parameters)
    
    build_model >> truncate_table >> copy_output_data_to_db >> [mon_success, mon_failure] >> cleanup
