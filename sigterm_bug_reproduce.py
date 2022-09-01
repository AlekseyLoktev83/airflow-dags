from airflow import DAG
from airflow.operators.python import PythonOperator

import time
from datetime import datetime

dag = DAG('sigterm_bug_reproduce',
          description='test',
          schedule_interval=None,
          start_date=datetime(2021, 4, 1),
          max_active_runs=1,
          concurrency=40,
          catchup=False)


def my_sleeping_function(t):
    time.sleep(t)


tasks = []
for i in range(400):
    task = PythonOperator(task_id='sleep_for_' + str(i),
                          python_callable=my_sleeping_function,
                          op_kwargs={'t': 60},
                          dag=dag)
