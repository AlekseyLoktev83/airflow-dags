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


HDFS_ATLAS_CONNECTION_NAME = 'webhdfs_atlas'
HDFS_CONNECTION_NAME = 'webhdfs_default'



@task(multiple_outputs=True)
def get_parameters(**kwargs):
    ti = kwargs['ti']
    ds = kwargs['ds']
    dag_run = kwargs['dag_run']
    hdfs_atlas_conn = BaseHook.get_connection(HDFS_ATLAS_CONNECTION_NAME)
    print(hdfs_atlas_conn.host)
    return str(hdfs_atlas_conn.host)
#     hdfs_atlas_parameters = '-S {} -d {} -U {} -P {}'.format(
#         hdfs_atlas_conn.host, hdfs_atlas_conn.schema, db_conn.login, db_conn.password)

#     parameters = {"RawPath": raw_path,
#                   "ProcessPath": process_path,
#                   "OutputPath": output_path,
#                   "WhiteList": white_list,
#                   "BlackList": black_list,
#                   "MaintenancePathPrefix": "{}{}{}_{}_".format(raw_path, "/#MAINTENANCE/", process_date, run_id),
#                   "BcpParameters": bcp_parameters,
#                   "UploadPath": upload_path,
#                   "RunId": run_id,
#                   "SystemName": system_name,
#                   "LastUploadDate": last_upload_date,
#                   "CurrentUploadDate": upload_date,
#                   "ProcessDate": process_date,
#                   "MaintenancePath": "{}{}".format(raw_path, "/#MAINTENANCE/"),
#                   }
#     print(parameters)
#     return parameters



with DAG(
    dag_id='hdfs_to_hdfs_copy',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["expiremental"],
    render_template_as_native_obj=True,
) as dag:
    # Get dag parameters from vault
    parameters = get_parameters()
    
    hdfs_to_hdfs = BashOperator(
        task_id='hdfs_to_hdfs',
        bash_command='hadoop dfs -ls hdfs://airflow@rc1b-dataproc-m-9cq245wo3unikye2.mdb.yandexcloud.net/ATLAS ',
    )
    
    copy = BashOperator(
        task_id='copy',
        bash_command='cp -r /tmp/data/src/. ~/ && chmod +x ~/hdfs_to_hdfs.sh && ~/hdfs_to_hdfs.sh /MIP/RAW/2022/09/23/dbo/ProdOperationMars/DELTA/* /MIP/RAW/ProdOp.csv',
    )
