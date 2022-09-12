from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.configuration import ensure_secrets_loaded
from airflow.providers.hashicorp.secrets.vault import VaultBackend
from airflow.providers.hashicorp.hooks.vault import VaultHook
import time
    

with DAG('bash_seq_test', start_date=datetime(2022, 1, 1), schedule_interval=None) as dag:
  ping = BashOperator(
        task_id='ping',
        bash_command="echo 1;dsfdsfsd;echo 3 ",
            )
