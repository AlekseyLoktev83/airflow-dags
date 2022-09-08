from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.decorators import dag, task

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2018, 1, 30),
	'email': ['aleksey.loktev@smartcom.software'],
	'email_on_failure': True
}

@task
def task1():
  x = 1/0
  print(1)
  
@task
def task2():
  print(2)  

with DAG('test-task',
         start_date=datetime(2020, 1, 1),
         schedule_interval=None,
         email =['aleksey.loktev@smartcom.software'],
	       email_on_failure = True,
        ) as dag:
    task1=task1()
    task2=task2()
    task1 >> task2
    
