# this is not production code. just useful for testing connectivity.
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
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
    
    postgres_export = BashOperator(
        task_id='postgres_export',
        bash_command='PGPASSWORD=Tl41s9 psql -h 192.168.10.234 -d EVORUS_InitialDevelopment_Main_Dev11_Current -U ptw_user -c "\copy (SELECT * FROM \"WOMBAT_ANALYSIS_DETAIL_FACT\") to STDOUT with csv header"|hadoop dfs -put -f - /PTW/RAW/WOMBAT_ANALYSIS_DETAIL_FACT.csv ',
        )    
