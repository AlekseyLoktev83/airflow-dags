import datetime
import pendulum

from airflow import DAG, XComArg
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
from airflow.providers.hashicorp.hooks.vault import VaultHook
from airflow.operators.email import EmailOperator

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


VAULT_CONNECTION_NAME = 'vault_default'
MIN_DAYS_TO_NOTIFY = 29


def _check_approle_expiration():
    email_to=Variable.get("EmailTo")
    
    vault_hook = VaultHook(VAULT_CONNECTION_NAME)
    conn = vault_hook.get_conn()
    resp = conn.auth.approle.read_secret_id(role_name='airflow-role', secret_id=vault_hook.connection.password)
    print(str(resp))
    
    expiration_time = pendulum.parse(resp['data']['expiration_time'])
    today = pendulum.now()
    diff = (expiration_time - today).days
    
    return diff <= MIN_DAYS_TO_NOTIFY
    


with DAG(
    dag_id='app_role_expiration_check',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["maintenance"],
    render_template_as_native_obj=True,
) as dag:
    check_approle_expiration = ShortCircuitOperator(
        task_id='check_approle_expiration',
        python_callable=_check_approle_expiration,
           )
    
    
#     send_email = EmailOperator( 
#           task_id='send_email', 
#           to='{{ti.xcom_pull(task_ids="get_parameters")["EmailTo"]}}', 
#           subject='Rolling volumes notification', 
#           html_content='<h2>{{ti.xcom_pull(task_ids="get_parameters")["Message"]}}</h2>',
#     )
