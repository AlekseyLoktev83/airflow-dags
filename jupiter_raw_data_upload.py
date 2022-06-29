import datetime
import pendulum

from airflow import DAG, XComArg
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.yandex.operators.yandexcloud_dataproc import  DataprocCreatePysparkJobOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
# from airflow.providers.hashicorp.hooks.vault import VaultHook
import uuid
from io import StringIO
import urllib.parse
import subprocess

import cloud_scripts.mssql_scripts as mssql_scripts
import json
import pandas as pd
import glob
import os


MSSQL_CONNECTION_NAME = 'odbc_default'
HDFS_CONNECTION_NAME = 'webhdfs_default'
VAULT_CONNECTION_NAME = 'vault_default'
AVAILABILITY_ZONE_ID = 'ru-central1-b'
S3_BUCKET_NAME_FOR_JOB_LOGS = 'jupiter-app-test-storage'
BCP_SEPARATOR = '0x01'

MONITORING_DETAIL_DIR_PREFIX = 'MONITORING_DETAIL.CSV'
EXTRACT_ENTITIES_AUTO_FILE = 'EXTRACT_ENTITIES_AUTO.csv'
MONITORING_FILE = 'MONITORING.csv'

STATUS_FAILURE='FAILURE'
STATUS_COMPLETE='COMPLETE'
STATUS_PROCESS='PROCESS'

def separator_convert_hex_to_string(sep):
    sep_map = {'0x01':'\x01'}
    return sep_map.get(sep, sep)

@task(multiple_outputs=True)
def get_parameters(**kwargs):
    ti = kwargs['ti']
    ds = kwargs['ds']
    execution_date = kwargs['execution_date'].strftime("%Y/%m/%d")
    run_id = urllib.parse.quote_plus(kwargs['run_id'])
    logical_date = kwargs['logical_date'].strftime("%Y-%m-%d %H:%M:%S")
    
    raw_path = Variable.get("RawPath")
    white_list = Variable.get("WhiteList")
    upload_path = f'{raw_path}/{execution_date}/'
    system_name = Variable.get("SystemName")
    last_upload_date = Variable.get("LastUploadDate")
    
    print(last_upload_date)

    db_conn = BaseHook.get_connection(HDFS_CONNECTION_NAME)
    bcp_parameters = '-S {} -d {} -U {} -P {}'.format(db_conn.host, db_conn.schema, db_conn.login, db_conn.password)
    
    parameters = {"RawPath": raw_path,
                  "WhiteList": white_list,
                  "MaintenancePath":"{}{}{}_{}_".format(raw_path,"/#MAINTENANCE/",ds,run_id),
                  "BcpParameters": bcp_parameters,
                  "UploadPath": upload_path,
                  "RunId":run_id,
                  "SystemName":system_name,
                  "CurrentUploadDate":logical_date,
                  "LastUploadDate":last_upload_date
                  }
    print(parameters)
    return parameters

@task
def generate_schema_query(parameters: dict):
    query = mssql_scripts.generate_db_schema_query(
        white_list=parameters['WhiteList'])
    
    return query

@task
def copy_data_db_to_hdfs(query,dst_dir,dst_file):
    
    dst_path = f"{dst_dir}{dst_file}"
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()

    df =  odbc_hook.get_pandas_df(query)
    df.to_csv(f'/tmp/{dst_file}', index=False)
    conn.upload(dst_path,f'/tmp/{dst_file}')
    
    return True

@task    
def generate_upload_script(prev_task,src_dir,src_file,upload_path,bcp_parameters):
    src_path = f"{src_dir}{src_file}"
    tmp_path = f"/tmp/{src_file}"
    print(src_path)
    
    hdfs_hook = WebHDFSHook()
    conn = hdfs_hook.get_conn()
    conn.download(src_path, tmp_path)
    
    entities = mssql_scripts.generate_table_select_query('2022-06-20','2022-06-20',tmp_path)
  
    return entities
    

@task    
def generate_bcp_script(src_dir,src_file,upload_path,bcp_parameters,entity):
    src_path = f"{src_dir}{src_file}"
    tmp_path = f"/tmp/{src_file}"
    print(src_path)
        
    script = 'cp -r /tmp/data/src/. ~/ && chmod +x ~/exec_query.sh && ~/exec_query.sh "{}" {}{}/{}/{}/{}.csv "{}" {} {} "{}" '.format(entity["Extraction"].replace("\'\'","\'\\'").replace("\n"," "),upload_path,entity["Schema"],entity["EntityName"],entity["Method"],entity["EntityName"],bcp_parameters,BCP_SEPARATOR,entity["Schema"],entity["Columns"].replace(",",separator_convert_hex_to_string(BCP_SEPARATOR)))

    return  script

@task
def start_monitoring(prev_task,dst_dir,system_name,run_id=None):
    monitoring_file_path=f'{dst_dir}{MONITORING_FILE}'

    temp_file_path =f'/tmp/{MONITORING_FILE}'
    df = pd.DataFrame([{'PipelineRunId':urllib.parse.quote_plus(run_id),
                        'SystemName':system_name,
                        'StartDate':pendulum.now(),
                        'EndDate':None,
                        'Status':STATUS_PROCESS
                       }])
    df.to_csv(temp_file_path, index=False)
    
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()
    conn.upload(monitoring_file_path,temp_file_path,overwrite=True)
    
    return True

@task
def start_monitoring_detail(dst_dir,upload_path,input,run_id=None):
    schema = input["Schema"]
    entity_name = input["EntityName"]
    method = input["Method"]
    monitoring_file_path=f'{dst_dir}{MONITORING_DETAIL_DIR_PREFIX}/{schema}_{entity_name}.csv'

    temp_file_path =f'/tmp/{schema}_{entity_name}.csv'
    df = pd.DataFrame([{'PipelineRunId':urllib.parse.quote_plus(run_id),
                        'Schema':schema,
                        'EntityName':entity_name,
                        'TargetPath':f'{upload_path}{schema}/{entity_name}/{method}/{entity_name}.csv',
                        'TargetFormat':'CSV',
                        'StartDate':pendulum.now(),
                        'Duration':0,
                        'Status':STATUS_PROCESS,
                        'ErrorDescription':None
                       }])
    df.to_csv(temp_file_path, index=False)
    
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()
    conn.upload(monitoring_file_path,temp_file_path)
    
    return input

@task
def end_monitoring_detail(dst_dir,input):
    prev_tast_output = json.loads(input)
    
    schema = prev_tast_output["Schema"]
    entity_name = prev_tast_output["EntityName"]
    prev_task_result = prev_tast_output["Result"]
    
    temp_file_path =f'/tmp/{schema}_{entity_name}.csv'
    monitoring_file_path=f'{dst_dir}{MONITORING_DETAIL_DIR_PREFIX}/{schema}_{entity_name}.csv'
    
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()
    conn.download(monitoring_file_path,temp_file_path)
    
    df = pd.read_csv(temp_file_path, keep_default_na=False)
    df['Status'] = STATUS_COMPLETE if prev_task_result else STATUS_FAILURE
    df['Duration'] =  prev_tast_output["Duration"]
    
    df.to_csv(temp_file_path, index=False)
    conn.upload(monitoring_file_path,temp_file_path,overwrite=True)
    
    return prev_tast_output

   
@task(trigger_rule=TriggerRule.ALL_DONE)
def get_upload_result(dst_dir,input):
    monintoring_details = list(input)
    print(monintoring_details)
    return not any(d['Result'] == False for d in monintoring_details)
     
def _end_monitoring(dst_dir,status):    
    monitoring_file_path=f'{dst_dir}{MONITORING_FILE}'
    temp_file_path =f'/tmp/{MONITORING_FILE}'

    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()
    conn.download(monitoring_file_path,temp_file_path)
    
    df = pd.read_csv(temp_file_path, keep_default_na=False)
    df['Status'] = STATUS_SUCCESS if status else STATUS_FAILURE 
    df['EndDate'] = pendulum.now()
    
    df.to_csv(temp_file_path, index=False)
    conn.upload(monitoring_file_path,temp_file_path,overwrite=True)    

def _check_upload_result(**kwargs):
    return ['end_monitoring_success'] if kwargs['input'] else ['end_monitoring_failure']

@task(task_id="end_monitoring_success")
def end_monitoring_success(dst_dir):
    _end_monitoring(dst_dir,True)

@task(task_id="end_monitoring_failure")
def end_monitoring_failure(dst_dir):
    _end_monitoring(dst_dir,False)  

with DAG(
    dag_id='jupiter_raw_data_upload',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["jupiter", "dev"],
    render_template_as_native_obj=True,
) as dag:
# Get dag parameters from vault    
    parameters = get_parameters()
#     Generate schema extraction query
    schema_query = generate_schema_query(parameters)
#     Extract db schema and save result to hdfs
    extract_schema = copy_data_db_to_hdfs(schema_query,parameters["MaintenancePath"],EXTRACT_ENTITIES_AUTO_FILE)
    start_mon=start_monitoring(extract_schema,dst_dir=parameters["MaintenancePath"],system_name=parameters["SystemName"])
#    Create entities list and start monitoring for them
    start_mon_detail = start_monitoring_detail.partial(dst_dir=parameters["MaintenancePath"],upload_path=parameters["UploadPath"]).expand(input = generate_upload_script(start_mon,parameters["MaintenancePath"],EXTRACT_ENTITIES_AUTO_FILE,parameters["UploadPath"],parameters["BcpParameters"]))
# Upload entities from sql to hdfs in parallel
    upload_tables=BashOperator.partial(task_id="upload_tables", do_xcom_push=True).expand(
       bash_command= generate_bcp_script.partial(src_dir=parameters["MaintenancePath"],src_file=EXTRACT_ENTITIES_AUTO_FILE,upload_path=parameters["UploadPath"],bcp_parameters=parameters["BcpParameters"]).expand(entity=start_mon_detail),
    )
#     Check entities upload results and update monitoring files
    end_mon_detail = end_monitoring_detail.partial(dst_dir=parameters["MaintenancePath"]).expand(input=XComArg(upload_tables))
    upload_result = get_upload_result(dst_dir=parameters["MaintenancePath"],input=end_mon_detail)
    
    branch_task = BranchPythonOperator(
    task_id='branching',
    python_callable=_check_upload_result,
    op_kwargs={'input': upload_result},    
    )
    
    join = DummyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    branch_task  >> [end_monitoring_success(dst_dir=parameters["MaintenancePath"]),end_monitoring_failure(dst_dir=parameters["MaintenancePath"])] >> join
    
