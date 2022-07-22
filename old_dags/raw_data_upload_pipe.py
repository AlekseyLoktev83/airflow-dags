from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


def create_dag(dag_id,
               schedule,
               dag_number,
               default_args):

    def print_info_py(*args):
        print('This is DAG: {}'.format(str(dag_number)))
        print(default_args['table_name'])

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        print_info = PythonOperator(
            task_id='print_info',
            python_callable=print_info_py)
        
        download_table = BashOperator(
        task_id='download_table',
        bash_command="cp -r /opt/airflow/logs/src/. ~/ && chmod +x ~/download_table.sh && ~/download_table.sh {{params.table_name}} ",
        params = {'table_name':default_args['table_name']}
            )
        print_info >> download_table

    return dag

def read_tables_list():
  import sys
  import hdfs
  
  cl=hdfs.client.Client(url="http://rc1b-dataproc-m-3iu6zt2tusazxrxi.mdb.yandexcloud.net:9870")
  src = "/user/smartadmin/schema/schema.csv"
  dst = "/tmp/schema.csv" 
  with cl.read(src, encoding='utf-8') as reader:
    file = reader.read()
    lines = file.splitlines()
    print(lines)
    return lines

tables = read_tables_list()
for n, table_name in enumerate(tables):
    dag_id = 'raw_data_upload_table_{}'.format(str(table_name))

    default_args = {'owner': 'airflow',
                    'start_date': datetime(2021, 1, 1),
                    'table_name': table_name
                    }

    schedule = None
    dag_number = n

    globals()[dag_id] = create_dag(dag_id,
                                  schedule,
                                  dag_number,
                                  default_args)
