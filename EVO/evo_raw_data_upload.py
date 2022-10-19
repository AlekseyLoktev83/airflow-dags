"""Dag загрузки таблиц EVO в hdfs"""
import datetime
import pendulum

from airflow import DAG, XComArg
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.yandex.operators.yandexcloud_dataproc import DataprocCreatePysparkJobOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
from airflow.providers.hashicorp.hooks.vault import VaultHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

import uuid
from io import StringIO
import urllib.parse
import subprocess

import cloud_scripts.postgres_scripts as postgres_scripts
import json
import pandas as pd
import glob
import os
import csv
import base64

# Названия connections
POSTGRES_CONNECTION_NAME = 'postgres_evo'
POSTGRES_CONNECTION2_NAME = 'postgres_evo2'
HDFS_CONNECTION_NAME = 'webhdfs_default'
VAULT_CONNECTION_NAME = 'vault_default'
# Префикс файла с деталями монитоинга
MONITORING_DETAIL_DIR_PREFIX = 'MONITORING_DETAIL.CSV'
# Файл со скриптами извлечения таблиц
EXTRACT_ENTITIES_AUTO_FILE = 'EXTRACT_ENTITIES_AUTO.csv'
# Файл со схемой таблиц
RAW_SCHEMA_FILE = 'RAW_SCHEMA.csv'
# Файл моинторинга
MONITORING_FILE = 'MONITORING.csv'
# Статусы загрузки
STATUS_FAILURE = 'FAILURE'
STATUS_COMPLETE = 'COMPLETE'
STATUS_PROCESS = 'PROCESS'
# Сколько дней хранить файлы мониторинга и настроек
DAYS_TO_KEEP_OLD_FILES = 2
CSV_SEPARATOR = '\u0001'


@task(multiple_outputs=True)
def get_parameters(**kwargs):
    """Получение параметров из Vault

    Returns:
        dict:Параметры
    """
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

    raw_path = Variable.get("RawPath#EVO")
    process_path = Variable.get("ProcessPath#EVO")
    output_path = Variable.get("OutputPath#EVO")
    white_list = Variable.get("WhiteList#EVO", default_var=None)
    black_list = Variable.get("BlackList#EVO", default_var=None)
    upload_path = f'{raw_path}/{execution_date}/'
    system_name = Variable.get("SystemName#EVO")
    last_upload_date = Variable.get("LastUploadDate#EVO")
    env_name1 = Variable.get("Environment1#EVO")
    env_name2 = Variable.get("Environment2#EVO")
    
    postgres_hook = PostgresHook(POSTGRES_CONNECTION_NAME)
    current_env_name = postgres_hook.get_first('SELECT "Value" FROM public."EnvironmentInfo"')
    print(current_env_name[0])
    current_env_name = 'EvoDev2_1'
    current_db_conn_name = POSTGRES_CONNECTION_NAME if current_env_name[0] == env_name1 else POSTGRES_CONNECTION2_NAME
    

    db_conn = BaseHook.get_connection(current_db_conn_name)
    postgres_copy_parameters = base64.b64encode(
        (f'psql -h {db_conn.host} -d {db_conn.schema} -U {db_conn.login}').encode()).decode()
    postgres_password = base64.b64encode(
        (f'{db_conn.password}').encode()).decode()

    parameters = {
        "RawPath": raw_path,
        "ProcessPath": process_path,
        "OutputPath": output_path,
        "WhiteList": white_list,
        "BlackList": black_list,
        "MaintenancePathPrefix": "{}{}{}_{}_".format(
            raw_path,
            "/#MAINTENANCE/",
            process_date,
            run_id),
        "PostgresCopyParameters": postgres_copy_parameters,
        "PostgresPassword": postgres_password,
        "UploadPath": upload_path,
        "RunId": run_id,
        "SystemName": system_name,
        "LastUploadDate": last_upload_date,
        "CurrentUploadDate": upload_date,
        "ProcessDate": process_date,
        "MaintenancePath": "{}{}".format(
            raw_path,
            "/#MAINTENANCE/"),
        "CurrentDbConnName":current_db_conn_name,
    }
    print(parameters)
    return parameters


@task
def generate_schema_query(parameters: dict):
    """генерация запроса со схемой таблиц

    Args:
        parameters (dict): Параметры

    Returns:
        str: Запрос sql
    """
    query = postgres_scripts.generate_db_schema_query(
        white_list=parameters['WhiteList'], black_list=parameters['BlackList'])

    return query


@task
def copy_data_db_to_hdfs(parameters: dict, query, dst_file):
    """Копирование результатов sql запроса в файл на hdfs

    Args:
        parameters (dict): Параметры
        query (str): Sql запрос
        dst_file (str): Название файла

    Returns:
        bool: Результат операции
    """
    dst_dir = parameters["MaintenancePathPrefix"]
    dst_path = f"{dst_dir}{dst_file}"
    postgres_hook = PostgresHook(parameters["CurrentDbConnName"])
    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()

    df = postgres_hook.get_pandas_df(query)
    df.to_csv(f'/tmp/{dst_file}', index=False, sep=CSV_SEPARATOR)
    conn.upload(dst_path, f'/tmp/{dst_file}', overwrite=True)

    return True


@task
def generate_upload_script(
        prev_task,
        src_dir,
        src_file,
        upload_path,
        postgres_copy_parameters,
        current_upload_date,
        last_upload_date):
    """Генерация скрипта загрузки таблиц

    Args:
        prev_task (task): Предыдущий таск Aiflow
        src_dir (str): Путь к hdfs каталогу со схемой
        src_file (str): Название файла со схемой
        upload_path (str): Путь к hdfs каталогу-назначению
        postgres_copy_parameters (str): Парметры подключения к бд postgres
        current_upload_date (str): Дата текущей загрузки
        last_upload_date (str): Дата последней успешной загрузки

    Returns:
        json: Скрипты загрузки таблиц
    """
    src_path = f"{src_dir}{src_file}"
    tmp_path = f"/tmp/{src_file}"
    print(src_path)

    hdfs_hook = WebHDFSHook(HDFS_CONNECTION_NAME)
    conn = hdfs_hook.get_conn()
    conn.download(src_path, tmp_path)

    entities_df = postgres_scripts.generate_table_select_query(
        current_upload_date, last_upload_date, tmp_path)
    entities_json = json.loads(entities_df.to_json(orient="records"))

    tmp_dst_path = f"/tmp/{EXTRACT_ENTITIES_AUTO_FILE}"
    dst_path = f"{src_dir}{EXTRACT_ENTITIES_AUTO_FILE}"

    del entities_df['Extraction']
    entities_df.to_csv(tmp_dst_path, index=False, sep=CSV_SEPARATOR)
    conn.upload(dst_path, tmp_dst_path, overwrite=True)

    return entities_json


@task
def generate_postgres_copy_script(
        upload_path,
        postgres_copy_parameters,
        postgres_password,
        entities):
    """Генерация скрипта копирования postgres -> hdfs

    Args:
        upload_path (str): Путь с результатами загрузки таблицы
        postgres_copy_parameters (str): Параметры бд postgres
        postgres_password (str): Пароль postgres
        entities (list): Список сущностей

    Returns:
        list: Список скриптов копирования таблиц
    """
    scripts = []
    for entity in entities:
        script = 'cp -r /tmp/data/src/. ~/ && chmod +x ~/exec_query_postgres.sh && ~/exec_query_postgres.sh "{}" {}{}/{}/{}/{}.csv "{}" "{}" "{}" {} '.format(
            entity["Extraction"].replace(
                '"',
                '\\"'),
            upload_path,
            entity["Schema"],
            entity["EntityName"],
            entity["Method"],
            entity["EntityName"],
            postgres_copy_parameters,
            postgres_password,
            CSV_SEPARATOR,
            entity["Schema"])
        scripts.append(script)

    return scripts


@task
def start_monitoring(prev_task, dst_dir, system_name, runid):
    """Запуск мониторинга загрузки

    Args:
        prev_task (task): Предыдущий таск Aiflow
        dst_dir (str): Каталог hdfs с результатами мониторинга
        system_name (str): Имя загружаемой системы
        runid (str): Airflow runid

    Returns:
        bool: Результат операции
    """
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
    """Запуск записи деталей мониторинга

    Args:
        dst_dir (str): Каталог hdfs с результатами мониторинга
        upload_path (str): Путь с результатами загрузки таблицы
        runid (str): Airflow runid
        entities (list): Cписок сущностей

    Returns:
        list: Cписок сущностей
    """
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
                            'TargetPath': f'{upload_path}{schema}/{entity_name}/{method}/{entity_name}.csv',
                            'TargetFormat': 'CSV',
                            'StartDate': pendulum.now(),
                            'Duration': 0,
                            'Status': STATUS_PROCESS,
                            'ErrorDescription': None}])
        df.to_csv(temp_file_path, index=False, sep=CSV_SEPARATOR)
        conn.upload(monitoring_file_path, temp_file_path, overwrite=True)

    return entities


@task
def end_monitoring_detail(dst_dir, entities):
    """Завершение записи деталей мониторинга

    Args:
        dst_dir (str): Каталог hdfs с результатами мониторинга
        entities (list): Cписок сущностей

    Returns:
        list: Список с результатми загрузки таблиц
    """
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
    """Получение результата загрузки таблиц

    Args:
        dst_dir (str): Каталог hdfs с результатами мониторинга
        input (list): Список с результатми загрузки таблиц

    Returns:
        bool: Результат загрузки таблиц
    """
    monintoring_details = input
    print(monintoring_details)
    return not any(d['Result'] == False for d in monintoring_details)


def _end_monitoring(dst_dir, status):
    """Callable завершения мониторинга

    Args:
        dst_dir (str): Каталог hdfs с результатами мониторинга
        status (status): Статус загрузки таблиц
    """
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
    """Callable для проверки результата загрузки и выбора следующего task'а

    Returns:
        str: Taskid
    """
    return ['end_monitoring_success'] if kwargs['input'] else [
        'end_monitoring_failure']


@task(task_id="end_monitoring_success")
def end_monitoring_success(dst_dir):
    """Успешное завершение загрузки

    Args:
        dst_dir (str): Каталог hdfs с результатами мониторинга
    """
    _end_monitoring(dst_dir, True)


@task(task_id="end_monitoring_failure")
def end_monitoring_failure(dst_dir):
    """Загрузка с ошибкой

    Args:
        dst_dir (str): Каталог hdfs с результатами мониторинга
    """
    _end_monitoring(dst_dir, False)


@task(task_id="update_last_upload_date")
def update_last_upload_date(last_upload_date):
    """Обновить дату последней успешной загрузки в Vault

    Args:
        last_upload_date (str): Дата последней успешной загрузки
    """
    vault_hook = VaultHook(VAULT_CONNECTION_NAME)
    conn = vault_hook.get_conn()
    conn.secrets.kv.v2.create_or_update_secret(
        path="variables/LastUploadDate#EVO",
        secret={
            "value": last_upload_date})


with DAG(
    dag_id='evo_raw_data_upload',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["evo", "dev"],
    render_template_as_native_obj=True,
) as dag:
    """Получение параметров из Vault
    """
    parameters = get_parameters()
    """Генерация запроса со схемой таблиц
    """
    schema_query = generate_schema_query(parameters)
    """Копирование схемы таблиц в hdfs
    """
    extract_schema = copy_data_db_to_hdfs(
        parameters, schema_query, RAW_SCHEMA_FILE)
    """Начало записи мониторинга
    """
    start_mon = start_monitoring(
        extract_schema,
        dst_dir=parameters["MaintenancePathPrefix"],
        system_name=parameters["SystemName"],
        runid=parameters["RunId"])
    start_mon_detail = start_monitoring_detail(
        dst_dir=parameters["MaintenancePathPrefix"],
        upload_path=parameters["UploadPath"],
        runid=parameters["RunId"],
        entities=generate_upload_script(
            start_mon,
            parameters["MaintenancePathPrefix"],
            RAW_SCHEMA_FILE,
            parameters["UploadPath"],
            parameters["PostgresCopyParameters"],
            parameters["CurrentUploadDate"],
            parameters["LastUploadDate"]))
    """Параллельная загрузка таблиц sql -> hdfs
    """
    upload_tables = BashOperator.partial(
        task_id="upload_tables",
        do_xcom_push=True,
        execution_timeout=datetime.timedelta(
            minutes=240),
        retries=3).expand(
            bash_command=generate_postgres_copy_script(
                upload_path=parameters["UploadPath"],
                postgres_copy_parameters=parameters["PostgresCopyParameters"],
                postgres_password=parameters["PostgresPassword"],
                entities=start_mon_detail),
    )
    """Получение результатов мониторинга
    """
    end_mon_detail = end_monitoring_detail(
        dst_dir=parameters["MaintenancePathPrefix"],
        entities=XComArg(upload_tables))
    """Получение общего результата загрузки
    """
    upload_result = get_upload_result(
        dst_dir=parameters["MaintenancePathPrefix"], input=end_mon_detail)
    """Проверка результата и принятие решения
    """
    if_upload_success = BranchPythonOperator(
        task_id='if_upload_success',
        python_callable=_check_upload_result,
        op_kwargs={'input': upload_result},
    )

    cleanup = BashOperator(
        task_id='cleanup',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        bash_command='/utils/hdfs_delete_old_files.sh {{ti.xcom_pull(task_ids="get_parameters",key="MaintenancePath")}} {{params.days_to_keep_old_files}} ',
        params={
            'days_to_keep_old_files': DAYS_TO_KEEP_OLD_FILES},
    )

    if_upload_success >> end_monitoring_success(
        dst_dir=parameters["MaintenancePathPrefix"]) >> update_last_upload_date(
        last_upload_date=parameters["CurrentUploadDate"]) >> cleanup
    if_upload_success >> end_monitoring_failure(
        dst_dir=parameters["MaintenancePathPrefix"]) >> cleanup
