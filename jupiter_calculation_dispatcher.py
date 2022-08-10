import pendulum

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator

TAGS=["jupiter", "dev"]

with DAG(
    dag_id="jupiter_calculation_dispatcher",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval=None,
    tags=TAGS,
) as dag:
   
    trigger_jupiter_calc_copy = TriggerDagRunOperator(
        task_id="trigger_jupiter_calc_copy",
        trigger_dag_id="jupiter_calc_copy",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":"{{dag_run.conf.get('schema')}}"},
        wait_for_completion = True,
    )
    
    trigger_jupiter_baseline_dispatcher = TriggerDagRunOperator(
        task_id="trigger_jupiter_baseline_dispatcher",
        trigger_dag_id="jjupiter_baseline_dispatcher",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":"{{dag_run.conf.get('schema')}}"},
        wait_for_completion = True,
    )
    
    trigger_jupiter_copy_after_baseline_update = TriggerDagRunOperator(
        task_id="trigger_jupiter_copy_after_baseline_update",
        trigger_dag_id="jupiter_calc_copy",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":"{{dag_run.conf.get('schema')}}"},
        wait_for_completion = True,
    )
    
    trigger_jupiter_promo_filtering = TriggerDagRunOperator(
        task_id="trigger_jupiter_promo_filtering",
        trigger_dag_id="jupiter_promo_filtering",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":"{{dag_run.conf.get('schema')}}"},
        wait_for_completion = True,
    )
    
    trigger_jupiter_calc_copy >> trigger_jupiter_baseline_dispatcher >> trigger_jupiter_copy_after_baseline_update >> trigger_jupiter_promo_filtering
