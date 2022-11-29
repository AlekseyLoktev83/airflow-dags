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
TAGS=["jupiter", "promo", "dev"]
BASELINE_ENTITY_NAME='BaseLine'
PARAMETERS_FILE = 'PARAMETERS.csv'
PROMOPRODUCT_PROCESS_DIR='/PromoProduct/NewPromoProduct.CSV/*.csv'


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
    
    parent_handler_id = dag_run.conf.get('parent_handler_id')
    handler_id = parent_handler_id if parent_handler_id else str(uuid.uuid4())
    
    db_conn = BaseHook.get_connection(MSSQL_CONNECTION_NAME)
    bcp_parameters =  base64.b64encode(('-S {} -d {} -U {} -P {}'.format(db_conn.host, db_conn.schema, db_conn.login,db_conn.password)).encode()).decode()
    odbc_extras = mssql_scripts.get_odbc_extras_string(db_conn)
    bcp_import_parameters =  base64.b64encode((f'DRIVER=ODBC Driver 18 for SQL Server;SERVER={db_conn.host};DATABASE={db_conn.schema};UID={db_conn.login};PWD={db_conn.password};{odbc_extras}').encode()).decode()
    
    cluster_id = Variable.get("JupiterDataprocClusterId")
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
                  "JupiterDataprocClusterId":cluster_id,
                  "HandlerId":handler_id,
                  }
    print(parameters)
    return parameters

@task
def truncate_temp_promoproduct(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    result = odbc_hook.run(sql=f"""truncate table [{schema}].[TEMP_PROMOPRODUCT]""")
    print(result)

    return result

@task(task_id='save_parameters')
def save_parameters(parameters:dict):
    parameters_file_path=f'{parameters["MaintenancePathPrefix"]}{PARAMETERS_FILE}'

    temp_file_path =f'/tmp/{PARAMETERS_FILE}'
    df = pd.DataFrame(parameters.items(),columns=['Key', 'Value'])
    df.to_csv(temp_file_path, index=False, sep=CSV_SEPARATOR)
    
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()
    conn.upload(parameters_file_path,temp_file_path,overwrite=True)
    
    
    args = json.dumps({"MaintenancePathPrefix":parameters["MaintenancePathPrefix"],"ProcessDate":parameters["ProcessDate"],"Schema":parameters["Schema"],"HandlerId":parameters["HandlerId"]})
                                                                            
                                                                                            
    return [args]

@task
def update_promoproduct(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    result = odbc_hook.run(sql=f"""exec [{schema}].[AddNewPromoProduct]""")
    print(result)

    return result

with DAG(
    dag_id='jupiter_all_parameters_calc',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=TAGS,
    render_template_as_native_obj=True,
    default_args={'retries': 2},	
) as dag:
# Get dag parameters from vault    
    parameters = get_parameters()
    save_params = save_parameters(parameters)
    
    parameters_calculation = DataprocCreatePysparkJobOperator(
        task_id='parameters_calculation',
        cluster_id=parameters['JupiterDataprocClusterId'],
        main_python_file_uri='hdfs:///SRC/JUPITER/PROMO_PARAMETERS_CALCULATION/PARAMETERS_CALCULATION.py	',
        python_file_uris=[
            'hdfs:///SRC/JUPITER/PROMO_PARAMETERS_CALCULATION/PLAN_PARAMETERS_CALCULATION.py',
            'hdfs:///SRC/JUPITER/PROMO_PARAMETERS_CALCULATION/ACTUAL_PARAMETERS_CALCULATION.py',
            'hdfs:///SRC/SHARED/EXTRACT_SETTING.py',
            'hdfs:///SRC/SHARED/SUPPORT_FUNCTIONS.py',
            'hdfs:///SRC/JUPITER/PROMO_PARAMETERS_CALCULATION/SET_PROMO_PRODUCT.py',
            'hdfs:///SRC/JUPITER/PROMO_PARAMETERS_CALCULATION/PLAN_PRODUCT_PARAMS_CALCULATION_PROCESS.py',
            'hdfs:///SRC/JUPITER/PROMO_PARAMETERS_CALCULATION/PLAN_PROMO_PARAMS_CALCULATION_PROCESS.py',
            'hdfs:///SRC/JUPITER/PROMO_PARAMETERS_CALCULATION/PLAN_SUPPORT_PARAMS_CALCULATION_PROCESS.py', 
            'hdfs:///SRC/JUPITER/PROMO_PARAMETERS_CALCULATION/COGS_TI_CALCULATION.py', 
            'hdfs:///SRC/JUPITER/PROMO_PARAMETERS_CALCULATION/RA_TI_SHOPPER_CALCULATION.py',
            'hdfs:///SRC/JUPITER/PROMO_PARAMETERS_CALCULATION/ACTUAL_PRODUCT_PARAMS_CALCULATION_PROCESS.py',
            'hdfs:///SRC/JUPITER/PROMO_PARAMETERS_CALCULATION/ACTUAL_PROMO_PARAMS_CALCULATION_PROCESS.py',
            'hdfs:///SRC/JUPITER/PROMO_PARAMETERS_CALCULATION/ACTUAL_SUPPORT_PARAMS_CALCULATION_PROCESS.py', 
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
    
    truncate_temp = truncate_temp_promoproduct(parameters)
    
    add_new_promoproduct = BashOperator(task_id="add_new_promoproduct",
                                 do_xcom_push=True,
                                 bash_command='/utils/bcp_import.sh {{ti.xcom_pull(task_ids="get_parameters",key="ProcessPath")}}{{params.PROCESS_DIR}} {{ti.xcom_pull(task_ids="get_parameters",key="BcpImportParameters")}} \"{{ti.xcom_pull(task_ids="get_parameters",key="Schema")}}.TEMP_PROMOPRODUCT\" "1" ',
                                 params={'PROCESS_DIR':PROMOPRODUCT_PROCESS_DIR},  
                                )
    
    upd_promoproduct=update_promoproduct(parameters)
    
    parameters_calculation >> truncate_temp >> add_new_promoproduct >> upd_promoproduct