import json
import pendulum
from datetime import datetime
from airflow import DAG, XComArg
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

@task
def make_list1():
    return ['echo 1 ','echo 2 ']
  
@task
def make_list2():
    return ['echo 3 ','echo 4 ']

@task
def combine(*input):
    for i in input:
         print(list(i))



with DAG(dag_id="combine_results", 
         start_date=datetime(2022, 4, 2),
         schedule_interval=None,
        ) as dag:
  
    make_list1 = make_list1()
    make_list2 = make_list2()
   
    echo_op1=BashOperator.partial(task_id="echo_op1", do_xcom_push=True).expand(
       bash_command=make_list1,
    )
    echo_op2=BashOperator.partial(task_id="echo_op2", do_xcom_push=True).expand(
       bash_command=make_list2,
    )
    
    combine(XComArg(echo_op1),XComArg(echo_op2))
    
    
#     consumer.expand(arg=make_list())
