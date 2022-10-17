"""Диспетчер загрузки EVO"""
import pendulum

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="evo_dispatcher",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval=None,
    tags=["evo", "dev"],
) as dag:

    trigger_dag_raw_upload = TriggerDagRunOperator(
        task_id="trigger_raw_data_upload",
        trigger_dag_id="evo_raw_data_upload",
        conf={"parent_run_id": "{{run_id}}", "parent_process_date": "{{ds}}"},
        wait_for_completion=True,
    )

    trigger_dag_raw_upload
