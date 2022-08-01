import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

with DAG(
    dag_id='ssh_test',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
  ssh_task=SSHOperator(
    task_id='list_ssh',ssh_conn_id='jupiter_dev_ssh',command='ls . ')

