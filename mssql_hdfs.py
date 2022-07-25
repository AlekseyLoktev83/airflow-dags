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
        bash_command="cp -r /tmp/data/src/. ~/ && chmod +x ~/exec_query.sh && ~/exec_query.sh \"SELECT [ID],[EventID],[Priority],REPLACE(REPLACE([Severity],CHAR(10),'\'),CHAR(13),'\') [Severity],REPLACE(REPLACE([Title],CHAR(10),'\'),CHAR(13),'\') [Title],[TimeOf],REPLACE(REPLACE([MachineName],CHAR(10),'\'),CHAR(13),'\') [MachineName],REPLACE(REPLACE([AppDomainName],CHAR(10),'\'),CHAR(13),'\') [AppDomainName],REPLACE(REPLACE([ProcessID],CHAR(10),'\'),CHAR(13),'\') [ProcessID],REPLACE(REPLACE([ProcessName],CHAR(10),'\'),CHAR(13),'\') [ProcessName],REPLACE(REPLACE([ThreadName],CHAR(10),'\'),CHAR(13),'\') [ThreadName],REPLACE(REPLACE([Win32ThreadId],CHAR(10),'\'),CHAR(13),'\') [Win32ThreadId],REPLACE(REPLACE([Message],CHAR(10),'\'),CHAR(13),'\') [Message],[UserID],REPLACE(REPLACE([RoleName],CHAR(10),'\'),CHAR(13),'\') [RoleName],REPLACE(REPLACE([ModuleName],CHAR(10),'\'),CHAR(13),'\') [ModuleName] , (SELECT count(*) FROM dbo.[EL_LOG]) [#QCCount] FROM dbo.[EL_LOG]\" /JUPITER/RAW/2022/07/22/dbo/EL_LOG/FULL/EL_LOG2.csv \"-S 192.168.10.39 -d MIP_UtilizeOutbound_Main_Dev_Current -U userdb -P qwerty1\" 0x01 dbo \"IDEventIDPrioritySeverityTitleTimeOfMachineNameAppDomainNameProcessIDProcessNameThreadNameWin32ThreadIdMessageUserIDRoleNameModuleName#QCCount\" ",
        params = {'table_name':'YA_DATAMART4'},
        )
    

   
  
