# this is not production code. just useful for testing connectivity.
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import base64

with DAG(
    dag_id="postgres_test",
    default_args={},
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["example"],
    catchup=False,
) as dag:
    create_table = PostgresOperator(
        task_id='query_1',
        postgres_conn_id="postgres_default",
        sql='''select * from "Animal";''',
    )
