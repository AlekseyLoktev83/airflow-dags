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
        bash_command="p -r /tmp/data/src/. ~/ && chmod +x ~/exec_query.sh && ~/exec_query.sh \"SELECT [ID],[USERID],IIF(DATEPART(YEAR, [RUNTIME])<1900,                 IIF(DATEPART(TZ, [RUNTIME])<0,                         DATEADD(HOUR, -3, CONVERT(DATETIME,DATEADD(YEAR, 1900-DATEPART(YEAR, [RUNTIME]), [RUNTIME]),0)),                             CONVERT(DATETIME,DATEADD(YEAR, 1900-DATEPART(YEAR, [RUNTIME]), [RUNTIME]),0)),                             IIF(DATEPART(TZ, [RUNTIME])<0,DATEADD(HOUR, -3,                              CONVERT(DATETIME,[RUNTIME],0)),CONVERT(DATETIME,[RUNTIME],1))) [RUNTIME],REPLACE(REPLACE([MERCURYSTATUS],CHAR(10),'\'),CHAR(13),'\') [MERCURYSTATUS],[STAT_ID],[STAMP],[INTERNALID],REPLACE(REPLACE([ADDITIONFIELDS],CHAR(10),'\'),CHAR(13),'\') [ADDITIONFIELDS],[REMOVEDATE],REPLACE(REPLACE([TRANSACTIONID],CHAR(10),'\'),CHAR(13),'\') [TRANSACTIONID],REPLACE(REPLACE([PARAMETERS],CHAR(10),'\'),CHAR(13),'\') [PARAMETERS],[ENTERPRISEID] , (SELECT count(*) FROM dbo.[VETDOCUMENTGETOPERATION]) [#QCCount] FROM dbo.[VETDOCUMENTGETOPERATION]\" /JUPITER/RAW/2022/07/25/dbo/VETDOCUMENTGETOPERATION/FULL/VETDOCUMENTGETOPERATION.csv \"-S 192.168.10.39 -d MIP_UtilizeOutbound_Main_Dev_Current -U userdb -P qwerty1\" 0x01 dbo \"IDUSERIDRUNTIMEMERCURYSTATUSSTAT_IDSTAMPINTERNALIDADDITIONFIELDSREMOVEDATETRANSACTIONIDPARAMETERSENTERPRISEID#QCCount\" ",
        params = {'table_name':'YA_DATAMART4'},
        )
    

   
  
