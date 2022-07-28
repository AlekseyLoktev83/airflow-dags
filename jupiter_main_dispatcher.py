import pendulum

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator

SCHEMA = "Jupiter"

with DAG(
    dag_id="jupiter_main_dispatcher",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval=None,
    tags=["jupiter", "dev"],
) as dag:
    trigger_jupiter_start_night_processing = TriggerDagRunOperator(
        task_id="trigger_jupiter_start_night_processing",
        trigger_dag_id="jupiter_start_night_processing",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":SCHEMA},
        wait_for_completion = True,
    )
    
    trigger_jupiter_calculation_dispatcher = TriggerDagRunOperator(
        task_id="trigger_jupiter_calculation_dispatcher",
        trigger_dag_id="jupiter_calculation_dispatcher",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":SCHEMA},
        wait_for_completion = True,
    )
    
    trigger_jupiter_end_night_processing = TriggerDagRunOperator(
        task_id="trigger_jupiter_end_night_processing",
        trigger_dag_id="jupiter_end_night_processing",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":SCHEMA},
        wait_for_completion = True,
    )
    
    trigger_jupiter_error_processing = TriggerDagRunOperator(
        task_id="trigger_jupiter_error_processing",
        trigger_dag_id="jupiter_error_processing",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":SCHEMA},
        wait_for_completion = True,
        trigger_rule=TriggerRule.ONE_FAILED
    )    
#     trigger_rule=TriggerRule.ALL_DONE
    trigger_jupiter_start_night_processing >> trigger_jupiter_calculation_dispatcher >> [trigger_jupiter_end_night_processing,trigger_jupiter_error_processing ]
