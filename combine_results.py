import json
import pendulum
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

@task
def make_list1():
    return 'echo 1 '
  
@task
def make_list2():
    return 'echo 2 '  

@task
def combine(input):
    print(list(input))


with DAG(dag_id="combine_results", 
         start_date=datetime(2022, 4, 2),
         schedule_interval=None,
        ) as dag:
  
    make_list1 = make_list1()
    make_list2 = make_list2()
   
    echo_op1=BashOperator.partial(task_id="echo_op1", do_xcom_push=False).expand(
       bash_command=make_list1,
    )
    echo_op2=BashOperator.partial(task_id="echo_op2", do_xcom_push=False).expand(
       bash_command=make_list2,
    )
    
    combine(input=[XComArg(echo_op1),XComArg(echo_op2)])
    
    
#     consumer.expand(arg=make_list())
