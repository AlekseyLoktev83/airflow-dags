import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator,SFTPOperation
from airflow.decorators import dag, task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.ssh.hooks.ssh import SSHHook

HDFS_CONNECTION_NAME = 'webhdfs_default'
SSH_CONNECTION_NAME = 'ssh_jupiter'

@task
def copy_sftp_to_hdfs():
    local_filepath="/tmp/YA_DATAMART4.csv",
    remote_filepath="/home/smartadmin/YA_DATAMART4.csv",
    dst_path = '/JUPITER/RAW/SOURCES/BASELINE/YA_DATAMART4.csv'
    
    ssh_hook=SSHHook(SSH_CONNECTION_NAME)
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    
    with ssh_hook.get_conn() as ssh_client:
     sftp_client = ssh_client.open_sftp()
     sftp_client.get(remote_filepath, local_filepath)
    
    
    conn = hdfs_hook.get_conn()
    conn.upload(dst_path, local_filepath)

    return True

with DAG(
    dag_id='ssh_test',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
#   ssh_task=SSHOperator(
#     task_id='list_ssh',ssh_conn_id='ssh_jupiter',command='ls . ')
#    sftp_get = SFTPOperator(task_id='sftp_get',
#                            ssh_conn_id='ssh_jupiter',
#                            operation=SFTPOperation.GET,
#                            local_filepath="/tmp/YA_DATAMART4.csv",
#                            remote_filepath="/home/smartadmin/YA_DATAMART4.csv",
#                            create_intermediate_dirs=True,
#                           )
   copy_sftp_to_hdfs()


