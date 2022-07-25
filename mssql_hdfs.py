import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id='mssql_to_hdfs_export',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    download_table = BashOperator(
        task_id='download_table',
        bash_command="cp -r /tmp/data/src/. ~/ && chmod +x ~/exec_query.sh && ~/exec_query.sh \"SELECT [ID],REPLACE(REPLACE([DTO],CHAR(10),'\'),CHAR(13),'\') [DTO],REPLACE(REPLACE([ROLE],CHAR(10),'\'),CHAR(13),'\') [ROLE],REPLACE(REPLACE([FIELDS],CHAR(10),'\'),CHAR(13),'\') [FIELDS],[STAT_ID],[STAMP],[INTERNALID],[ADDITIONFIELDS],[REMOVEDATE] , (SELECT count(*) FROM dbo.[EDITABLEPRESETS]) [#QCCount] FROM dbo.[EDITABLEPRESETS]\" /JUPITER/RAW/2022/07/22/dbo/EDITABLEPRESETS/FULL/EDITABLEPRESETS.csv \"-S 192.168.10.39 -d MIP_UtilizeOutbound_Main_Dev_Current -U userdb -P qwerty1\" 0x01 dbo \"IDDTOROLEFIELDSSTAT_IDSTAMPINTERNALIDADDITIONFIELDSREMOVEDATE#QCCount\" ",
        params = {'table_name':'YA_DATAMART4'},
        )
    

   
  
