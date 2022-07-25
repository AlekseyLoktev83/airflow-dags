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
        bash_command="cp -r /tmp/data/src/. ~/ && chmod +x ~/exec_query_local.sh && ~/exec_query_local.sh \"SELECT [ID],[SECURITYUSERID],[MERCURYUSERID],REPLACE(REPLACE([OPERATIONNAME],CHAR(10),'\'),CHAR(13),'\') [OPERATIONNAME],[STAT_ID],[STAMP],[INTERNALID],[ADDITIONFIELDS],[REMOVEDATE] , (SELECT count(*) FROM dbo.[MERCURYOPERATIONUSERLINK]) [#QCCount] FROM dbo.[MERCURYOPERATIONUSERLINK]\" /JUPITER/RAW/2022/07/22/dbo/MERCURYOPERATIONUSERLINK/FULL/MERCURYOPERATIONUSERLINK.csv \"-S 192.168.10.39 -d MIP_UtilizeOutbound_Main_Dev_Current -U userdb -P qwerty1\" 0x01 dbo \"HDR_HDR\" ",
        params = {'table_name':'YA_DATAMART4'},
        )
    

   
  
