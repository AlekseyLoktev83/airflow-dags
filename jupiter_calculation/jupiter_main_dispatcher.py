import pendulum

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import dag, task
import uuid

SCHEMA = "Jupiter"

@task(task_id='generate_handler_id')
def generate_handler_id():
    return str(uuid.uuid4())

with DAG(
    dag_id="jupiter_main_dispatcher",
    start_date=pendulum.datetime(2022, 7, 28, 7, 20, tz="UTC"),
    catchup=False,
    schedule_interval=None,
#     schedule_interval='20 7 * * *',
    tags=["jupiter", "dev","main"],
) as dag:
    handler_id=generate_handler_id()
    
    trigger_jupiter_start_night_processing = TriggerDagRunOperator(
        task_id="trigger_jupiter_start_night_processing",
        trigger_dag_id="jupiter_start_night_processing",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":SCHEMA},
        wait_for_completion = True,
    )
    
    trigger_jupiter_calculation_dispatcher = TriggerDagRunOperator(
        task_id="trigger_jupiter_calculation_dispatcher",
        trigger_dag_id="jupiter_calculation_dispatcher",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":SCHEMA,"parent_handler_id":"{{ti.xcom_pull(task_ids='generate_handler_id')}}"},
        wait_for_completion = True,
    )
    
    trigger_jupiter_end_night_processing = TriggerDagRunOperator(
        task_id="trigger_jupiter_end_night_processing",
        trigger_dag_id="jupiter_end_night_processing",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":SCHEMA},
        wait_for_completion = True,
    )
    
    trigger_jupiter_move_logs_to_sftp = TriggerDagRunOperator(
        task_id="trigger_jupiter_move_logs_to_sftp",
        trigger_dag_id="jupiter_move_logs_to_sftp",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":SCHEMA,"parent_handler_id":"{{ti.xcom_pull(task_ids='generate_handler_id')}}"},
        wait_for_completion = True,
    )
    
    trigger_jupiter_move_promo_to_archive = TriggerDagRunOperator(
        task_id="trigger_jupiter_move_promo_to_archive",
        trigger_dag_id="jupiter_move_promo_to_archive",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":SCHEMA},
        wait_for_completion = True,
    )
    
    trigger_jupiter_error_processing = TriggerDagRunOperator(
        task_id="trigger_jupiter_error_processing",
        trigger_dag_id="jupiter_error_processing",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":SCHEMA,"error_message":"Jupiter calculation was failed!"},
        wait_for_completion = True,
        trigger_rule=TriggerRule.ONE_FAILED
    )
    
    trigger_jupiter_error_end_night_processing = TriggerDagRunOperator(
        task_id="trigger_jupiter_error_end_night_processing",
        trigger_dag_id="jupiter_end_night_processing",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":SCHEMA,"night_processing_value":2},
        wait_for_completion = True,
        trigger_rule=TriggerRule.ONE_FAILED
    )
    
    trigger_jupiter_error_move_logs_to_sftp = TriggerDagRunOperator(
        task_id="trigger_jupiter_error_move_logs_to_sftp",
        trigger_dag_id="jupiter_move_logs_to_sftp",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":SCHEMA,"parent_handler_id":"{{ti.xcom_pull(task_ids='generate_handler_id')}}"},
        wait_for_completion = True,
        trigger_rule=TriggerRule.ONE_FAILED
    )
    
    trigger_jupiter_error_move_promo_to_archive = TriggerDagRunOperator(
        task_id="trigger_jupiter_error_move_promo_to_archive",
        trigger_dag_id="jupiter_move_promo_to_archive",  
        conf={"parent_run_id":"{{run_id}}","parent_process_date":"{{ds}}","schema":SCHEMA},
        wait_for_completion = True,
        trigger_rule=TriggerRule.ONE_FAILED
    )        
    handler_id >> trigger_jupiter_start_night_processing >> trigger_jupiter_calculation_dispatcher
#     Success branch
    trigger_jupiter_calculation_dispatcher >> trigger_jupiter_end_night_processing >> trigger_jupiter_move_logs_to_sftp >> trigger_jupiter_move_promo_to_archive
#     Error branch
    trigger_jupiter_calculation_dispatcher >> trigger_jupiter_error_processing >> trigger_jupiter_error_end_night_processing >> trigger_jupiter_error_move_logs_to_sftp >> trigger_jupiter_error_move_promo_to_archive
