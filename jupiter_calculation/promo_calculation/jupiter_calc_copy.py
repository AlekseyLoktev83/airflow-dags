import datetime
import pendulum

from airflow import DAG, XComArg
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.yandex.operators.yandexcloud_dataproc import DataprocCreatePysparkJobOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
from airflow.providers.hashicorp.hooks.vault import VaultHook

import uuid
from io import StringIO
import urllib.parse
import subprocess

import cloud_scripts.mssql_scripts as mssql_scripts
import json
import pandas as pd
import glob
import os
import csv
import base64


MSSQL_CONNECTION_NAME = 'odbc_jupiter'
HDFS_CONNECTION_NAME = 'webhdfs_default'
VAULT_CONNECTION_NAME = 'vault_default'
AVAILABILITY_ZONE_ID = 'ru-central1-a'
S3_BUCKET_NAME_FOR_JOB_LOGS = 'jupiter-app-test-storage'
BCP_SEPARATOR = '0x01'

MONITORING_DETAIL_DIR_PREFIX = 'MONITORING_DETAIL.CSV'
EXTRACT_ENTITIES_AUTO_FILE = 'EXTRACT_ENTITIES_AUTO.csv'
RAW_SCHEMA_FILE = 'RAW_SCHEMA.csv'
MONITORING_FILE = 'MONITORING.csv'

STATUS_FAILURE = 'FAILURE'
STATUS_COMPLETE = 'COMPLETE'
STATUS_PROCESS = 'PROCESS'

DAYS_TO_KEEP_OLD_FILES = 2
CSV_SEPARATOR = '\u0001'


def separator_convert_hex_to_string(sep):
    sep_map = {'0x01': '\x01'}
    return sep_map.get(sep, sep)


@task(multiple_outputs=True)
def get_parameters(**kwargs):
    ti = kwargs['ti']
    ds = kwargs['ds']
    dag_run = kwargs['dag_run']
    parent_process_date = dag_run.conf.get('parent_process_date')
    process_date = parent_process_date if parent_process_date else ds
    execution_date = kwargs['execution_date'].strftime("%Y/%m/%d")
    parent_run_id = dag_run.conf.get('parent_run_id')
    run_id = urllib.parse.quote_plus(
        parent_run_id) if parent_run_id else urllib.parse.quote_plus(kwargs['run_id'])
    upload_date = kwargs['logical_date'].strftime("%Y-%m-%d %H:%M:%S")

    raw_path = Variable.get("RawPath")
    process_path = Variable.get("ProcessPath")
    output_path = Variable.get("OutputPath")
    white_list = Variable.get("PromoCalculationEntites", default_var=None)
    black_list = Variable.get("BlackList", default_var=None)
    upload_path = f'{raw_path}/SOURCES/JUPITER/'
    system_name = Variable.get("SystemName")
    last_upload_date = Variable.get("LastUploadDate")

    db_conn = BaseHook.get_connection(MSSQL_CONNECTION_NAME)
    bcp_parameters =  base64.b64encode(('-S {} -d {} -U {} -P {}'.format(
        db_conn.host, db_conn.schema, db_conn.login, db_conn.password)).encode()).decode()

    parameters = {"RawPath": raw_path,
                  "ProcessPath": process_path,
                  "OutputPath": output_path,
                  "WhiteList": white_list,
                  "BlackList": black_list,
                  "MaintenancePathPrefix": "{}{}{}_{}_".format(raw_path, "/#MAINTENANCE/", process_date, run_id),
                  "BcpParameters": bcp_parameters,
                  "UploadPath": upload_path,
                  "RunId": run_id,
                  "SystemName": system_name,
                  "LastUploadDate": last_upload_date,
                  "CurrentUploadDate": upload_date,
                  "ProcessDate": process_date,
                  "MaintenancePath": "{}{}".format(raw_path, "/#MAINTENANCE/"),
                  }
    print(parameters)
    return parameters


@task
def generate_schema_query(parameters: dict):
    query = mssql_scripts.generate_db_schema_query(
        white_list=parameters['WhiteList'], black_list=parameters['BlackList'])

    return query


@task
def copy_data_db_to_hdfs(query, dst_dir, dst_file):

    dst_path = f"{dst_dir}{dst_file}"
    odbc_hook = OdbcHook(MSSQL_CONNECTION_NAME)
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()

    df = odbc_hook.get_pandas_df(query)
    df.to_csv(f'/tmp/{dst_file}', index=False, sep=CSV_SEPARATOR)
    conn.upload(dst_path, f'/tmp/{dst_file}',overwrite=True)

    return True


@task
def generate_upload_script(prev_task, src_dir, src_file, upload_path, bcp_parameters, current_upload_date, last_upload_date):
    src_path = f"{src_dir}{src_file}"
    tmp_path = f"/tmp/{src_file}"
    print(src_path)

    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()
    conn.download(src_path, tmp_path)

    entities_df = mssql_scripts.generate_table_select_query(
        current_upload_date, last_upload_date, tmp_path)
    entities_json = json.loads(entities_df.to_json(orient="records"))

    tmp_dst_path = f"/tmp/{EXTRACT_ENTITIES_AUTO_FILE}"
    dst_path = f"{src_dir}{EXTRACT_ENTITIES_AUTO_FILE}"

    del entities_df['Extraction']
    entities_df.to_csv(tmp_dst_path, index=False, sep=CSV_SEPARATOR)
    conn.upload(dst_path, tmp_dst_path,overwrite=True)

    return entities_json


@task
def generate_bcp_script(upload_path, bcp_parameters, entities):
    scripts = []
    for entity in entities:
        script = '/utils/exec_query.sh "{}" {}{}/{}.csv "{}" {} {} "{}" '.format(entity["Extraction"].replace("\'\'", "\'\\'").replace(
            "\n", " "), upload_path, entity["EntityName"], entity["EntityName"], bcp_parameters, BCP_SEPARATOR, entity["Schema"], entity["Columns"].replace(",", separator_convert_hex_to_string(BCP_SEPARATOR)))
        scripts.append(script)

    return scripts


@task
def start_monitoring(prev_task, dst_dir, system_name, runid):
    monitoring_file_path = f'{dst_dir}{MONITORING_FILE}'

    temp_file_path = f'/tmp/{MONITORING_FILE}'
    df = pd.DataFrame([{'PipelineRunId': runid,
                        'SystemName': system_name,
                        'StartDate': pendulum.now(),
                        'EndDate': None,
                        'Status': STATUS_PROCESS
                        }])
    df.to_csv(temp_file_path, index=False, sep=CSV_SEPARATOR)

    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()
    conn.upload(monitoring_file_path, temp_file_path, overwrite=True)

    return True


@task
def start_monitoring_detail(dst_dir, upload_path, runid, entities):
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()

    for ent in entities:
        schema = ent["Schema"]
        entity_name = ent["EntityName"]
        method = ent["Method"]
        monitoring_file_path = f'{dst_dir}{MONITORING_DETAIL_DIR_PREFIX}/{schema}_{entity_name}.csv'

        temp_file_path = f'/tmp/{schema}_{entity_name}.csv'
        df = pd.DataFrame([{'PipelineRunId': runid,
                           'Schema': schema,
                            'EntityName': entity_name,
                            'TargetPath': f'{upload_path}{entity_name}/{entity_name}.csv',
                            'TargetFormat': 'CSV',
                            'StartDate': pendulum.now(),
                            'Duration': 0,
                            'Status': STATUS_PROCESS,
                            'ErrorDescription': None
                            }])
        df.to_csv(temp_file_path, index=False, sep=CSV_SEPARATOR)
        conn.upload(monitoring_file_path, temp_file_path,overwrite=True)

    return entities


@task
def end_monitoring_detail(dst_dir, entities):
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()

    result = []
    for ent in list(entities):

        prev_tast_output = json.loads(ent)
        schema = prev_tast_output["Schema"]
        entity_name = prev_tast_output["EntityName"]
        prev_task_result = prev_tast_output["Result"]

        temp_file_path = f'/tmp/{schema}_{entity_name}.csv'
        monitoring_file_path = f'{dst_dir}{MONITORING_DETAIL_DIR_PREFIX}/{schema}_{entity_name}.csv'

        conn.download(monitoring_file_path, temp_file_path)

        df = pd.read_csv(
            temp_file_path, keep_default_na=False, sep=CSV_SEPARATOR)
        df['Status'] = STATUS_COMPLETE if prev_task_result else STATUS_FAILURE
        df['Duration'] = prev_tast_output["Duration"]

        df.to_csv(temp_file_path, index=False, sep=CSV_SEPARATOR)
        conn.upload(monitoring_file_path, temp_file_path, overwrite=True)
        result.append(prev_tast_output)

    return result


@task(trigger_rule=TriggerRule.ALL_DONE)
def get_upload_result(dst_dir, input):
    monintoring_details = input
    print(monintoring_details)
    return not any(d['Result'] == False for d in monintoring_details)


def _end_monitoring(dst_dir, status):
    monitoring_file_path = f'{dst_dir}{MONITORING_FILE}'
    temp_file_path = f'/tmp/{MONITORING_FILE}'

    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()
    conn.download(monitoring_file_path, temp_file_path)

    df = pd.read_csv(temp_file_path, keep_default_na=False, sep=CSV_SEPARATOR)
    df['Status'] = STATUS_COMPLETE if status else STATUS_FAILURE
    df['EndDate'] = pendulum.now()

    df.to_csv(temp_file_path, index=False, sep=CSV_SEPARATOR)
    conn.upload(monitoring_file_path, temp_file_path, overwrite=True)


def _check_upload_result(**kwargs):
    return ['end_monitoring_success'] if kwargs['input'] else ['end_monitoring_failure']


@task(task_id="end_monitoring_success")
def end_monitoring_success(dst_dir):
    _end_monitoring(dst_dir, True)


@task(task_id="end_monitoring_failure")
def end_monitoring_failure(dst_dir):
    _end_monitoring(dst_dir, False)


@task(task_id="update_last_upload_date")
def update_last_upload_date(last_upload_date):
    vault_hook = VaultHook(VAULT_CONNECTION_NAME)
    conn = vault_hook.get_conn()
    conn.secrets.kv.v2.create_or_update_secret(
        path="variables/LastUploadDate", secret={"value": last_upload_date})


with DAG(
    dag_id='jupiter_calc_copy',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["jupiter", "dev"],
    render_template_as_native_obj=True,
    default_args={'retries': 2},
) as dag:
    # Get dag parameters from vault
    parameters = get_parameters()
#     Generate schema extraction query
    schema_query = generate_schema_query(parameters)
#     Extract db schema and save result to hdfs
    extract_schema = copy_data_db_to_hdfs(
        schema_query, parameters["MaintenancePathPrefix"], RAW_SCHEMA_FILE)
    start_mon = start_monitoring(
        extract_schema, dst_dir=parameters["MaintenancePathPrefix"], system_name=parameters["SystemName"], runid=parameters["RunId"])
#    Create entities list and start monitoring for them
    start_mon_detail = start_monitoring_detail(dst_dir=parameters["MaintenancePathPrefix"], upload_path=parameters["UploadPath"], runid=parameters["RunId"], entities=generate_upload_script(
        start_mon, parameters["MaintenancePathPrefix"], RAW_SCHEMA_FILE, parameters["UploadPath"], parameters["BcpParameters"], parameters["CurrentUploadDate"], parameters["LastUploadDate"]))
# Upload entities from sql to hdfs in parallel
    upload_tables = BashOperator.partial(task_id="upload_tables", do_xcom_push=True,execution_timeout=datetime.timedelta(minutes=30), retries=2).expand(
        bash_command=generate_bcp_script(
            upload_path=parameters["UploadPath"], bcp_parameters=parameters["BcpParameters"], entities=start_mon_detail),
    )
#     Check entities upload results and update monitoring files
    end_mon_detail = end_monitoring_detail(
        dst_dir=parameters["MaintenancePathPrefix"], entities=XComArg(upload_tables))
    upload_result = get_upload_result(
        dst_dir=parameters["MaintenancePathPrefix"], input=end_mon_detail)

    branch_task = BranchPythonOperator(
        task_id='branching',
        python_callable=_check_upload_result,
        op_kwargs={'input': upload_result},
    )

    cleanup = BashOperator(
        task_id='cleanup',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        bash_command='/utils/hdfs_delete_old_files.sh {{ti.xcom_pull(task_ids="get_parameters",key="MaintenancePath")}} {{params.days_to_keep_old_files}} ',
        params={'days_to_keep_old_files': DAYS_TO_KEEP_OLD_FILES},
    )

    branch_task >> end_monitoring_success(dst_dir=parameters["MaintenancePathPrefix"]) >> update_last_upload_date(
        last_upload_date=parameters["CurrentUploadDate"]) >> cleanup
    branch_task >> end_monitoring_failure(
        dst_dir=parameters["MaintenancePathPrefix"]) >> cleanup
