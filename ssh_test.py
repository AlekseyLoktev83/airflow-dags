import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator,SFTPOperation

with DAG(
    dag_id='ssh_test',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
#   ssh_task=SSHOperator(
#     task_id='list_ssh',ssh_conn_id='ssh_jupiter',command='ls . ')
   sftp_get = SFTPOperator(task_id='sftp_get',
                           ssh_conn_id='ssh_jupiter',
                           operation=SFTPOperation.GET,
                           local_filepath="/tmp/YA_DATAMART4.csv",
                           remote_filepath="/home/smartadmin/YA_DATAMART4.csv",
                           create_intermediate_dirs=True,
                          )


