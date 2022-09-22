import json
import pendulum
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

@task
def make_list():
    return [f'sleep 60; echo {x}; ' for x in list(range(200))]


@task
def consumer(arg):
    print(list(arg))


with DAG(dag_id="dynamic-map", 
         start_date=datetime(2022, 4, 2),
         schedule_interval=None,
        ) as dag:

    echo_op=BashOperator.partial(task_id="bash", do_xcom_push=False).expand(
       bash_command=make_list(),
    )
#     consumer.expand(arg=make_list())
