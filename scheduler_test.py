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
         start_date=datetime(2022, 11, 7),
         schedule_interval='7 15 * * *',
         catchup=False,
        ) as dag:
      task2()
    
