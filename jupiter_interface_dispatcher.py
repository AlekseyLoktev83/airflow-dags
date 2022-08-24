import pendulum

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

TAGS = ["jupiter", "dev","interface"]

with DAG(
    dag_id="jupiter_interface_dispatcher",
    start_date=pendulum.datetime(2022, 7, 28, 7, 20, tz="UTC"),
    catchup=False,
    schedule_interval=None,
#     schedule_interval='20 7 * * *',
    tags=TAGS,
) as dag:
    trigger_jupiter_incoming_file_collect = TriggerDagRunOperator(
        task_id="trigger_jupiter_incoming_file_collect",
        trigger_dag_id="jupiter_incoming_file_collect",  
        wait_for_completion = True,
    )
    
   
