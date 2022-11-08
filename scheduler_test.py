from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.decorators import dag, task

 
@task
def task2():
  print(2)  

with DAG('scheduler_test',
         start_date=datetime(2022, 11, 8),
         schedule_interval='6 57 * * *',
         default_args=default_args,
         catchup=False,
        ) as dag:
      task2
    
