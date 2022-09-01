import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task
import uuid

@task
def gen_file_name():
    return str(uuid.uuid4())

with DAG(
    dag_id='s3_mount_check',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
  gen_file_name = gen_file_name()  
  list_dir = BashOperator(
        task_id='list_dir',
        bash_command="ls -la /tmp/data/src/ ",
      )
  
  create_file = BashOperator(
        task_id='create_file',
        bash_command="touch /tmp/data/src/check{{gen_file_name)}} ",
      )  
  gen_file_name >> list_dir >> create_file
