import datetime
import pendulum

from airflow import DAG, XComArg
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
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


VAULT_CONNECTION_NAME = 'vault_default'

@task
def get_app_role_info():
    vault_hook = VaultHook(VAULT_CONNECTION_NAME)
    conn = vault_hook.get_conn()
    resp = conn.auth.approle.list_secret_id_accessors(
    role_name='airflow-role',
                  )
    print(str(resp))


with DAG(
    dag_id='app_role_expiration_check',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["maintenance"],
    render_template_as_native_obj=True,
) as dag:
    get_app_role_info()
