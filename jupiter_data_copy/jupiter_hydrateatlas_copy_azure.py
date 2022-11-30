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
import cloud_scripts.azure_scripts as azure_scripts
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
AZURE_CONNECTION_NAME = 'azure_jupiter_sp'
TAGS=["jupiter", "promo", "copy"]


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
    azure_conn = BaseHook.get_connection(AZURE_CONNECTION_NAME)
    print(azure_conn)
    
    dst_dir = f'{raw_path}/SOURCES/HYDRATEATLAS/'
    

    parameters = {"RawPath": raw_path,
                  "ProcessPath": process_path,
                  "OutputPath": output_path,
                  "WhiteList": white_list,
                  "BlackList": black_list,
                  "MaintenancePathPrefix":"{}{}{}_{}_".format(raw_path,"/#MAINTENANCE/",process_date,run_id),
                  "UploadPath": upload_path,
                  "RunId":run_id,
                  "SystemName":system_name,
                  "LastUploadDate":last_upload_date,
                  "CurrentUploadDate":upload_date,
                  "ProcessDate":process_date,
                  "MaintenancePath":"{}{}".format(raw_path,"/#MAINTENANCE/"),
                  "Schema":schema,
                  "ParentRunId":parent_run_id,
                  "DstDir":dst_dir,
                  }
    print(parameters)
    return parameters



@task
def generate_azure_copy_script(parameters:dict, entity):
    src_path = entity['SrcPath']
    dst_path = entity['DstPath']

    script = azure_scripts.generate_adls_to_hdfs_copy_folder_command(azure_connection_name=AZURE_CONNECTION_NAME,
                                                     src_path=src_path,
                                                     dst_path=dst_path,
                                                     folders_from_tail_count = 1)
    return script

@task
def generate_entity_list(parameters:dict):
    raw_path=parameters['RawPath']
    dst_dir=parameters['DstDir'] 
    entities = [
              {'SrcPath':'https://marsanalyticsprodadls.dfs.core.windows.net/output/RUSSIA_DATA_FOUNDATION/HYDRATEATLAS/MASTERDATA/CUSTOMER/0CUST_SALES_ATTR.PARQUET','DstPath':dst_dir},
              {'SrcPath':'https://marsanalyticsprodadls.dfs.core.windows.net/output/RUSSIA_DATA_FOUNDATION/HYDRATEATLAS/MODEL/SALESORDER/SALESORDERITEM/Z2LIS_11_VAITM.PARQUET','DstPath':dst_dir},
              {'SrcPath':'https://marsanalyticsprodadls.dfs.core.windows.net/output/RUSSIA_DATA_FOUNDATION/HYDRATEATLAS/MODEL/DELIVERY/DELIVERYITEM/Z2LIS_12_VCITM.PARQUET','DstPath':dst_dir},
             ]
    return entities

with DAG(
    dag_id='jupiter_hydrateatlas_copy_azure',
    schedule_interval='0 20 * * *',
    start_date=pendulum.datetime(2022, 11, 29, tz="UTC"),
    catchup=False,
    tags=TAGS,
    render_template_as_native_obj=True,
) as dag:
# Get dag parameters from vault    
    parameters = get_parameters()  
  
    copy_entities = BashOperator.partial(task_id="copy_entity",
                                       do_xcom_push=True,
                                      ).expand(bash_command=generate_azure_copy_script.partial(parameters=parameters).expand(entity=generate_entity_list(parameters)),
                                              )
