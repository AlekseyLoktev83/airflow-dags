from airflow import DAG, XComArg
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
     'start_date': datetime(2020, 1, 1)
}

def _choose_best_model():
    accuracy = 6
    if accuracy > 5:
        return 'pos_task'
    return 'neg_task'

@task
def pos_task():
     raise ValueError('POS TASK ERROR')
     print('pos task')
     
@task
def neg_task():
     print('neg task')
     
     
with DAG('branching', 
schedule_interval=None,
default_args=default_args,
catchup=False) as dag:
    choose_best_model = BranchPythonOperator(
                        task_id='choose_best_model',
                        python_callable=_choose_best_model
                                            )
    on_error = DummyOperator(
                        task_id='on_error',
                        trigger_rule=TriggerRule.ALL_FAILED,
                            )
    on_success = DummyOperator(
                        task_id='on_success',
                        trigger_rule=TriggerRule.NONE_FAILED,
                             )
    pos_task = pos_task()
    neg_task = neg_task()

    choose_best_model >> pos_task >> on_success >> on_error
    choose_best_model >> neg_task >> on_success >> on_error
