import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id='hdfs_delete_old_files',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:

    delete_old_files = BashOperator(
        task_id='delete_old_files',
        bash_command="/utils/hdfs_delete_old_files.sh /JUPITER/RAW/#MAINTENANCE/ 3 ",
            )

    
    delete_old_files
   
  
