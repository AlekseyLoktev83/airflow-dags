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
from airflow.utils.log.secrets_masker import mask_secret

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
TAGS=["jupiter", "scenario", "dev"]
PARAMETERS_FILE = 'PARAMETERS.csv'

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
    
    parent_handler_id = dag_run.conf.get('parent_handler_id')
    handler_id = parent_handler_id if parent_handler_id else str(uuid.uuid4())
    
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
    
    budget_year = dag_run.conf.get('budget_year')
    client_list = dag_run.conf.get('client_list')
    
    db_conn = BaseHook.get_connection(MSSQL_CONNECTION_NAME)
    bcp_parameters =  base64.b64encode(('-S {} -d {} -U {} -P {}'.format(db_conn.host, db_conn.schema, db_conn.login,db_conn.password)).encode()).decode()

    mask_secret(MSSQL_CONNECTION_NAME) 
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
                  "HandlerId":handler_id,
                  "BudgetYear":budget_year,
                  "ClientList":client_list,
                  }
    print(parameters)
    return parameters

@task
def disable_previous_scenario_data(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    budget_year=parameters["BudgetYear"]
    client_list=parameters["ClientList"]
    
    result = odbc_hook.run(sql=f"""exec [Jupiter].[DisablePreviousScenarioData]  @ClientList=?, @BudgetYear=? """,parameters=(client_list,budget_year))
    print(result)

    return result

@task
def add_scenario_promo(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
 
    result = odbc_hook.run(sql=f"""exec [Jupiter].[AddScenarioPromo] """)
    print(result)

    return result

@task
def add_scenario_promoproduct(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
 
    result = odbc_hook.run(sql=f"""exec [Jupiter].[AddScenarioPromoProduct] """)
    print(result)

    return result

@task
def add_scenario_promoproductscorrection(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
 
    result = odbc_hook.run(sql=f"""exec [Jupiter].[AddScenarioPromoProductsCorrection] """)
    print(result)

    return result

@task
def add_scenario_promoproducttree(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
 
    result = odbc_hook.run(sql=f"""exec [Jupiter].[AddScenarioPromoProductTree] """)
    print(result)

    return result

@task
def add_scenario_promosupport(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
 
    result = odbc_hook.run(sql=f"""exec [Jupiter].[AddScenarioPromoSupport] """)
    print(result)

    return result

@task
def add_scenario_promosupportpromo(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
 
    result = odbc_hook.run(sql=f"""exec [Jupiter].[AddScenarioPromoSupportPromo] """)
    print(result)

    return result


@task
def add_scenario_btl(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
 
    result = odbc_hook.run(sql=f"""exec [Jupiter].[AddScenarioBTL] """)
    print(result)

    return result

@task
def add_scenario_btlpromo(parameters:dict):
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
 
    result = odbc_hook.run(sql=f"""exec [Jupiter].[AddScenarioBTLPromo] """)
    print(result)

    return result

with DAG(
    dag_id='jupiter_update_target_promo_tables',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=TAGS,
    render_template_as_native_obj=True,
) as dag:
# Get dag parameters from vault    
    parameters = get_parameters()
  
    disable_previous_scenario_data = disable_previous_scenario_data(parameters)
    add_scenario_promo = add_scenario_promo(parameters)
    add_scenario_promoproduct = add_scenario_promoproduct(parameters)
    add_scenario_promoproductscorrection = add_scenario_promoproductscorrection(parameters)
    add_scenario_promoproducttree = add_scenario_promoproducttree(parameters)
    add_scenario_promosupport=add_scenario_promosupport(parameters)
    add_scenario_promosupportpromo=add_scenario_promosupportpromo(parameters)
    add_scenario_btl=add_scenario_btl(parameters)
    add_scenario_btlpromo=add_scenario_btlpromo(parameters)
    
    disable_previous_scenario_data >> add_scenario_promo >> add_scenario_promoproduct >> add_scenario_promoproductscorrection
    disable_previous_scenario_data >> add_scenario_promo >> add_scenario_promoproducttree
    disable_previous_scenario_data >> add_scenario_promo >> add_scenario_promosupport >> add_scenario_promosupportpromo
    disable_previous_scenario_data >> add_scenario_promo >> add_scenario_btl >> add_scenario_btlpromo

