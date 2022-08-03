import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

TAGS=["jupiter", "baseline", "dev"]

with DAG(
    dag_id='bcp_import_test',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=TAGS,
) as dag:
    bcp_import = BashOperator(
        task_id='bcp_import',
        bash_command="/bcp_import_src/bcp_import ",
        )
