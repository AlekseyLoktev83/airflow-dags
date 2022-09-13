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
    dag_id='db_to_s3_test',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
  gen_file_name = gen_file_name()  
#   list_dir = BashOperator(
#         task_id='list_dir',
#         bash_command="ls -la /tmp/data/src/ ",
#       )
  
  export_to_s3 = BashOperator(
        task_id='export_to_s3',
        bash_command='cp -r /tmp/data/src/. ~/ && chmod +x ~/exec_query_s3.sh && ~/exec_query_s3.sh  "SELECT [NAME],[INDEX],[INDEXES],[ID],[METHA_ID],[TYPE_ID],[CALC_TYPE_ID],[KLS_ID],[KLS_FIELD_ID],[UNIQUE_ID],[STAT_ID] , (SELECT count(*) FROM dbo.[METHA_ATTR]) [#QCCount] FROM dbo.[METHA_ATTR]" /tmp/data/src/METHA_ATTR.csv "-S 192.168.10.39 -d MIP_Prod_20220415 -U userdb -P qwerty1" 0x01 dbo "NAME\x01INDEX\x01INDEXES\x01ID\x01METHA_ID\x01TYPE_ID\x01CALC_TYPE_ID\x01KLS_ID\x01KLS_FIELD_ID\x01UNIQUE_ID\x01STAT_ID\x01#QCCount" ',
      )  
  export_to_s3
