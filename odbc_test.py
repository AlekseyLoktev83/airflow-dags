# this is not production code. just useful for testing connectivity.
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook

dag = DAG(
    dag_id="odbc_example",
    default_args={},
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["example"],
    catchup=False,
)

def sample_select():
#     odbc_hook = OdbcHook("odbc_jupiter_secret") 
    db_conn = BaseHook.get_connection("odbc_jupiter_secret")
    bcp_parameters = '-S {} -d {} -U {} -P {}'.format(db_conn.host, db_conn.schema, db_conn.login, db_conn.password)
    
    return bcp_parameters
#     rec = odbc_hook.get_records("SELECT * from Country;")
#     df =  odbc_hook.get_pandas_df("SELECT * from Jupiter.Promo;")
#     print(df.head(3).to_markdown())
#     print(rec)
#     cnxn = odbc_hook.get_conn()

#     cursor = cnxn.cursor()
#     cursor.execute("SELECT * from Country;")
#     row = cursor.fetchone()
#     while row:
#         print(row[2])
#         row = cursor.fetchone()

def hdfs_test():
    hdfs_hook = WebHDFSHook()
    conn = hdfs_hook.get_conn()
    files = conn.list('/user/smartadmin/data')
    print(files)

PythonOperator(
    task_id="sample_select",
    python_callable=sample_select,
    dag=dag,
)

PythonOperator(
    task_id="hdfs_list_files",
    python_callable=hdfs_test,
    dag=dag,
)
