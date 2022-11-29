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
# from cloud_scripts.trigger_dagrun import TriggerDagRunOperator as CustomTriggerDagRunOperator

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
TAGS=["jupiter","promo","dev"]
PARAMETERS_FILE = 'PARAMETERS.csv'
COPY_MODE_DATABASE='Database'

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
    client_promo_dir = Variable.get("ClientPromoDir")

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
                  "ClientPromoDir":client_promo_dir,
                  }
    print(parameters)
    return parameters

def _check_if_clients_empty(**kwargs):
    return kwargs['clients']
  
@task
def get_clients_to_copy(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    schema = parameters["Schema"]
    converters = [(-155, handle_datetimeoffset)]
    result = mssql_scripts.get_records(odbc_hook,sql=f"""SELECT * FROM {schema}.ScenarioCopyTask WHERE [Disabled] = 0 """,output_converters=converters)
    
    result = [{k: v for k, v in d.items() if k not in ['CreateDate','ProcessDate','DeletedDate']} for d in result]
    return result
  
  
@task
def create_dag_config_copy_from_db(parameters:dict, clients):
    
    conf={"parent_run_id":parameters["ParentRunId"],
          "parent_process_date":parameters["ProcessDate"],
          "schema":parameters["Schema"],
          "client_prefix":clients[0]["ClientObjectId"],
          "client_name":clients[0]["ClientPrefix"],
          "emails":clients[0]["Email"],
          "drop_files_if_errors":True,
          "copy_mode":COPY_MODE_DATABASE,
          "source_path":f'{parameters["RawPath"]}/{parameters["ClientPromoDir"]}/{clients[0]["ClientObjectId"]}_{pendulum.now().strftime("%Y%m%d%H%M%S")}/',
          "Id":clients[0]["Id"],		  
         }
    return conf


@task
def create_dag_config_copy_from_adls(parameters:dict, db_conf:dict,clients):
    conf_list = []
    
    for client in clients[1:]:
        conf={"parent_run_id":parameters["ParentRunId"],
          "parent_process_date":parameters["ProcessDate"],
          "schema":parameters["Schema"],
          "client_prefix":client["ClientObjectId"],
          "client_name":client["ClientPrefix"],
          "emails":client["Email"],
          "drop_files_if_errors":True,
          "source_path":db_conf["source_path"],
          "Id":client["Id"],				  
            }
        conf_list.append(conf)
        
    return conf_list
  
  
with DAG(
    dag_id='jupiter_clients_promo_copy',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=TAGS,
    render_template_as_native_obj=True,
    default_args={'retries': 2},	
) as dag:
# Get dag parameters from vault    
    parameters = get_parameters()
    get_clients_to_copy = get_clients_to_copy(parameters)
    create_dag_config_copy_from_db = create_dag_config_copy_from_db(parameters,get_clients_to_copy)
  
    trigger_jupiter_client_promo_copy_from_db = TriggerDagRunOperator(
        task_id="trigger_jupiter_client_promo_copy_from_db",
        trigger_dag_id="jupiter_client_promo_copy",  
        conf='{{ti.xcom_pull(task_ids="create_dag_config_copy_from_db")}}',
        wait_for_completion = True,
    )
    
    create_dag_config_copy_from_adls = create_dag_config_copy_from_adls(parameters, create_dag_config_copy_from_db, get_clients_to_copy)
    
    trigger_jupiter_client_promo_copy_from_adls = TriggerDagRunOperator.partial(task_id="trigger_jupiter_client_promo_copy_from_adls",
                                                                    wait_for_completion = True,
                                                                     trigger_dag_id="jupiter_client_promo_copy",
                                                                    ).expand(conf=create_dag_config_copy_from_adls)
 
 
       

    parameters >> get_clients_to_copy >> create_dag_config_copy_from_db >> trigger_jupiter_client_promo_copy_from_db >> create_dag_config_copy_from_adls >> trigger_jupiter_client_promo_copy_from_adls 
