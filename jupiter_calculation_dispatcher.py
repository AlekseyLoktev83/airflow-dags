import pendulum

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="jupiter_calculation_dispatcher",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval=None,
    tags=["jupiter", "dev"],
) as dag:
   
    trigger_dag_raw_upload = TriggerDagRunOperator(
        task_id="trigger_jupiter_calc_copy",
        trigger_dag_id="jupiter_calc_copy",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":"{{dag_run.conf.get('schema')}}"},
        wait_for_completion = True,
    )
    
    trigger_dag_raw_upload
