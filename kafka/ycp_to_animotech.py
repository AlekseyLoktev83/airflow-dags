import datetime
import pendulum
import logging

from airflow import DAG, XComArg
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from cloud_scripts.custom_dataproc import  DataprocCreatePysparkJobOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from cloud_scripts.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
from airflow.providers.hashicorp.hooks.vault import VaultHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator
from airflow_provider_kafka.operators.consume_from_topic import ConsumeFromTopicOperator
from airflow_provider_kafka.operators.await_message import AwaitKafkaMessageOperator

import uuid
from io import StringIO
import urllib.parse
import subprocess

import cloud_scripts.mssql_scripts as mssql_scripts
import cloud_scripts.kafka_scripts as kafka_scripts
import json
import pandas as pd
import glob
import os
import base64

import struct
from contextlib import closing


MSSQL_CONNECTION_NAME = 'odbc_jupiter'
HDFS_CONNECTION_NAME = 'webhdfs_default'
VAULT_CONNECTION_NAME = 'vault_default'
S3_CONNECTION_NAME = 's3_default'
TAGS=["animotech"]

connection_config = {
    "bootstrap.servers": "rc1b-0qd2fn83sq9vp78r.mdb.yandexcloud.net:9091",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "jupiter-user",
    "sasl.password": "pass1234",
    "ssl.ca.location": "/tmp/YandexCA.crt"
}

my_topic = "jupiter"
consumer_logger = logging.getLogger("airflow")


@task(multiple_outputs=True)
def get_parameters(**kwargs):
    ti = kwargs['ti']
    ds = kwargs['ds']
    dag_run = kwargs['dag_run']
    parent_process_date = dag_run.conf.get('parent_process_date')
    process_date = parent_process_date if parent_process_date else ds
    execution_date = kwargs['execution_date'].strftime("%Y/%m/%d")
    parent_run_id = dag_run.conf.get('parent_run_id')
    run_id = urllib.parse.quote_plus(parent_run_id) if parent_run_id else urllib.parse.quote_plus(kwargs['run_id'])
    
    schema = dag_run.conf.get('schema')
    upload_date = kwargs['logical_date'].strftime("%Y-%m-%d %H:%M:%S")
    file_name = dag_run.conf.get('FileName')
    create_date = dag_run.conf.get('CreateDate')

    raw_path = Variable.get("RawPath")
    process_path = Variable.get("ProcessPath")
    output_path = Variable.get("OutputPath")
    white_list = Variable.get("WhiteList",default_var=None)
    black_list = Variable.get("BlackList",default_var=None)
    upload_path = f'{raw_path}/{execution_date}/'
    system_name = Variable.get("SystemName")
    last_upload_date = Variable.get("LastUploadDate")
    
    db_conn = BaseHook.get_connection(MSSQL_CONNECTION_NAME)
    dst_dir = f'{raw_path}/SOURCES/SELLIN/'
    

    parameters = {"RawPath": raw_path,
                  "ProcessPath": process_path,
                  "OutputPath": output_path,
                  "WhiteList": white_list,
                  "BlackList": black_list,
                  "MaintenancePathPrefix":"{}{}{}_{}_".format(raw_path,"/#MAINTENANCE/",process_date,run_id),
                  "UploadPath": upload_path,
                  "RunId":run_id,
                  "SystemName":system_name,
                  "LastUploadDate":last_upload_date,
                  "CurrentUploadDate":upload_date,
                  "ProcessDate":process_date,
                  "MaintenancePath":"{}{}".format(raw_path,"/#MAINTENANCE/"),
                  "Schema":schema,
                  "ParentRunId":parent_run_id,
                  "DstDir":dst_dir,
                  }
    print(parameters)
    return parameters



@task
def generate_copy_script(parameters:dict, entity):
    src_path = entity['SrcPath']
    dst_path = entity['DstPath']
    filename =  entity['Filename']

    script = kafka_scripts.generate_hdfs_to_s3_copy_file_command(s3_connection_name=S3_CONNECTION_NAME,
                                                     filename=filename,            
                                                     src_path=src_path,
                                                     dst_path=dst_path)
    return script

@task
def generate_entity_list(parameters:dict):
    raw_path=parameters['RawPath']
    dst_dir=parameters['DstDir']
    date_str=pendulum.now().strftime("%Y%m%d")
    
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()
    
    entities = [{'Filename':f'{date_str}_{e}','SrcPath':f'{raw_path}/SCHEMAS/{e}' ,'DstPath':'s3://jupiter-app-test-storage/animotech/in/' } for e in conn.list(f'{raw_path}/SCHEMAS/')]
    entities = entities[0:3]
    
    return entities

def producer_function(entities=None):
    print(entities)
    for i,e in enumerate(entities):
        yield (json.dumps(i), json.dumps(e))
#     for e in list(entities):
#         yield (e)

with DAG(
    dag_id='ycp_to_animotech',
#     schedule_interval='0 20 * * *',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 11, 29, tz="UTC"),
    catchup=False,
    tags=TAGS,
    render_template_as_native_obj=True,
) as dag:
# Get dag parameters from vault    
    parameters = get_parameters()
    generate_entity_list = generate_entity_list(parameters)
    copy_entities = BashOperator.partial(task_id="copy_entity",
                                       do_xcom_push=True,
                                      ).expand(bash_command=generate_copy_script.partial(parameters=parameters).expand(entity=generate_entity_list),
                                              )
    
    producer_task = ProduceToTopicOperator(
        task_id=f"produce_to_{my_topic}",
        topic=my_topic,
        producer_function=producer_function,
        producer_function_kwargs={"entities": generate_entity_list},
        kafka_config=connection_config,
        do_xcom_push=True,
    )
    
    copy_entities >> producer_task
    
