import pendulum

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
import uuid

TAGS=["jupiter", "dev"]

@task(task_id='generate_handler_id')
def generate_handler_id(**kwargs):
    dag_run = kwargs['dag_run']
    parent_handler_id = dag_run.conf.get('parent_handler_id')
    handler_id = parent_handler_id if parent_handler_id else str(uuid.uuid4())
    return handler_id

with DAG(
    dag_id="jupiter_calculation_dispatcher",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval=None,
    tags=TAGS,
    default_args={'retries': 2},	
) as dag:
    
    handler_id=generate_handler_id()
   
    trigger_jupiter_calc_copy = TriggerDagRunOperator(
        task_id="trigger_jupiter_calc_copy",
        trigger_dag_id="jupiter_calc_copy",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":"{{dag_run.conf.get('schema')}}"},
        wait_for_completion = True,
    )
    
    trigger_jupiter_baseline_dispatcher = TriggerDagRunOperator(
        task_id="trigger_jupiter_baseline_dispatcher",
        trigger_dag_id="jupiter_baseline_dispatcher",  
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
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":"{{dag_run.conf.get('schema')}}","parent_handler_id":"{{ti.xcom_pull(task_ids='generate_handler_id')}}"},
        wait_for_completion = True,
    )

    trigger_jupiter_all_parameters_calc = TriggerDagRunOperator(
        task_id="trigger_jupiter_all_parameters_calc",
        trigger_dag_id="jupiter_all_parameters_calc",  
        conf={"parent_run_id":"{{dag_run.conf.get('parent_run_id')}}","parent_process_date":"{{ds}}","schema":"{{dag_run.conf.get('schema')}}","parent_handler_id":"{{ti.xcom_pull(task_ids='generate_handler_id')}}"},
        wait_for_completion = True,
    )

    trigger_jupiter_update_promo = TriggerDagRunOperator(
        task_id="trigger_jupiter_update_promo",
        trigger_dag_id="jupiter_update_promo",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":"{{dag_run.conf.get('schema')}}","parent_handler_id":"{{ti.xcom_pull(task_ids='generate_handler_id')}}"},
        wait_for_completion = True,
    )
    
    trigger_jupiter_unblock_promo = TriggerDagRunOperator(
        task_id="trigger_jupiter_unblock_promo",
        trigger_dag_id="jupiter_unblock_promo",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":"{{dag_run.conf.get('schema')}}","parent_handler_id":"{{ti.xcom_pull(task_ids='generate_handler_id')}}"},
        wait_for_completion = True,
    )        

    trigger_jupiter_incremental_processing = TriggerDagRunOperator(
        task_id="trigger_jupiter_incremental_processing",
        trigger_dag_id="jupiter_incremental_processing",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":"{{dag_run.conf.get('schema')}}","parent_handler_id":"{{ti.xcom_pull(task_ids='generate_handler_id')}}"},
        wait_for_completion = True,
    )  
    
    handler_id >> trigger_jupiter_calc_copy >> trigger_jupiter_baseline_dispatcher >> trigger_jupiter_copy_after_baseline_update >> trigger_jupiter_promo_filtering >> trigger_jupiter_all_parameters_calc >> trigger_jupiter_update_promo >> trigger_jupiter_unblock_promo >> trigger_jupiter_incremental_processing

	
    
