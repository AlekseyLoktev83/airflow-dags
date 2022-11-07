from datetime import datetime
import pandas as pd
from itertools import groupby
from io import StringIO
import json
from contextlib import closing

PARAM_DELIMETER = ";"
METHOD_FULL = "FULL"
METHOD_DELTA = "DELTA"
CSV_SEPARATOR = '\u0001'


def generate_db_schema_query(
        environment=None,
        upload_date=None,
        black_list=None,
        white_list=None):
    """Функция создания запроса для выборки схемы таблиц

    Args:
        environment (str, optional): Среда . Defaults to None.
        upload_date (str, optional): Строка с датой загрузки. Defaults to None.
        black_list (str, optional): Список исключенных таблиц(разделитель: ;). Defaults to None.
        white_list (str, optional): Список разрешенных таблиц(разделитель: ;). Defaults to None.

    Returns:
        str: Строка с запросом
    """""""""
    environment_name = environment.strip() if environment else ""
    upload_date = upload_date if upload_date else datetime.today().strftime(
        "%Y-%m-%d %H:%M:%S")

    black_list_sql = " AND ".join(["NOT TABLE_SCHEMA||'.'||TABLE_NAME Like '{}'".format(
        x) for x in black_list.split(PARAM_DELIMETER)]) if black_list else ""

    combined_list_sql = " AND {}".format(
        black_list_sql) if black_list_sql else ""

    white_list_sql = "(" + (" OR ".join(["TABLE_SCHEMA||'.'||TABLE_NAME Like '{}'".format(
        x) for x in white_list.split(PARAM_DELIMETER)])) + ")" if white_list else ""

    combined_list_sql = "{} AND {}".format(
        combined_list_sql,
        white_list_sql) if white_list_sql else combined_list_sql

    script = """Select
                TABLE_SCHEMA AS "Schema",
                TABLE_NAME as "TableName",
                COLUMN_NAME as "FieldName",
                ORDINAL_POSITION as "Position",
                DATA_TYPE as "FieldType",
                COALESCE(CHARACTER_MAXIMUM_LENGTH,
		                 NUMERIC_PRECISION,
		                 DATETIME_PRECISION) as "Size",
                case
	             when IS_NULLABLE = 'NO' then 0
	             when IS_NULLABLE = 'YES' then 1
                end AS "IsNull",
                to_date('{}','YYYY-MM-DD') as "updateDate",
                NUMERIC_PRECISION as "Scale"
                from  INFORMATION_SCHEMA.COLUMNS
                where TABLE_SCHEMA <>'pg_catalog' and TABLE_SCHEMA <> 'information_schema' {}""".format(upload_date, combined_list_sql)

    print(script)
    return script


def get_records(odbc_hook, sql, parameters=None, output_converters=[]):
    """Получение записей в результате выполнения sql запроса

    Args:
        odbc_hook (OdbcHook): Хук odbc
        sql (str): Запрос sql
        parameters (dict, optional): Параметры запроса. Defaults to None.
        output_converters (list, optional): Список конвертеров запроса. Defaults to [].

    Returns:
        List of dicts: Список строк с результатми выполнения запроса
    """""""""
    with closing(odbc_hook.get_conn()) as conn:
        for conv in output_converters:
            conn.add_output_converter(conv[0], conv[1])
        with closing(conn.cursor()) as cur:
            if parameters is not None:
                cur.execute(sql, parameters)
            else:
                cur.execute(sql)
            columns = [column[0] for column in cur.description]

            results = []
            for row in cur.fetchall():
                results.append(dict(zip(columns, row)))
            return results


def get_first(odbc_hook, sql, parameters=None):
    """Получение первой записи в результате выполнения sql запроса

    Args:
        odbc_hook (OdbcHook): Хук odbc
        sql (str): Запрос sql
        parameters (_type_, optional): Параметры запроса. Defaults to None.

    Returns:
        dict: Первая запись результата запроса
    """
    with closing(odbc_hook.get_conn()) as conn:
        with closing(conn.cursor()) as cur:
            if parameters is not None:
                cur.execute(sql, parameters)
            else:
                cur.execute(sql)

            columns = [column[0] for column in cur.description]
            row = cur.fetchone()
            return dict(zip(columns, row))


def generate_table_select_query(
        current_upload_date,
        last_upload_date,
        actual_schema_file):
    """Функция создания запроса для выборки таблицы

    Args:
        current_upload_date (str): Дата текущей загрузки
        last_upload_date (str): Дата последней успешной загрузки
        actual_schema_file (str): Путь к файлу со схемой таблицы

    Returns:
        Pandas Dataframe: Dataframe с запросами для извлечения таблиц из бд
    """    """"""
    df = pd.read_csv(
        actual_schema_file,
        keep_default_na=False,
        sep=CSV_SEPARATOR)
    rows = df.to_dict('records')
    grouped_rows = {i: list(j) for (i, j) in groupby(
        rows, lambda x: (x["Schema"], x["TableName"]))}

    result = []
    for table, columns in grouped_rows.items():
        print(table)
        print()
        method = METHOD_FULL

        fields_list = []
        column_name_list = []
        for column in columns:
            if column["FieldType"] == 'nvarchar':
                fields_list.append(
                    """REPLACE(REPLACE("{field_name}",CHR(10),''),CHR(13),'') "{field_name}" """.format(
                        field_name=column["FieldName"]))
            elif column["FieldType"] == 'character varying':
                fields_list.append(
                    """REPLACE(REPLACE("{field_name}",CHR(10),''),CHR(13),'') "{field_name}" """.format(
                        field_name=column["FieldName"]))
            else:
                fields_list.append(""" "{field_name}" """.format(
                    field_name=column["FieldName"]))

            if column["FieldName"] == 'STAMP':
                method = METHOD_DELTA

            column_name_list.append(column["FieldName"])

        fields = ",".join(fields_list)

        column_name_list.append('#QCCount')
        column_names = ",".join(column_name_list)

        if method == METHOD_DELTA:
            script = "SELECT {fields} , (SELECT count(*) FROM {schema}.[{table_name}] WHERE STAMP BETWEEN CONVERT(nvarchar(20),'{last_modified_date}', 120) AND CONVERT(nvarchar(20),'{current_upload_date}', 120)) [#QCCount] FROM {schema}.[{table_name}] t WHERE t.STAMP BETWEEN CONVERT(nvarchar(20),'{last_modified_date}', 120) AND CONVERT(nvarchar(20),'{current_upload_date}', 120)".format(
                fields=fields, schema=table[0], table_name=table[1], last_modified_date=last_upload_date, current_upload_date=current_upload_date)
        else:
            script = """SELECT {fields} , (SELECT count(*) FROM {schema}."{table_name}") "#QCCount" FROM {schema}."{table_name}" """.format(
                fields=fields, schema=table[0], table_name=table[1])

        result.append({"Schema": table[0],
                       "EntityName": table[1],
                       "Extraction": script,
                       "Method": method,
                       "Columns": column_names})

    result_df = pd.DataFrame(result)
    return result_df


def generate_copy_command(
        query,
        dst_path,
        db_params,
        db_password,
        sep,
        schema):
    """Функция генерации shell скрипта для копирования результатов запроса в csv файл

    Args:
        query (str): Текст sql запроса
        dst_path (str): hdfs путь к файлу
        db_params (str): параметры бд
        db_password (str): пароль бд (base64 encoded)
        sep (str): Csv разделитель
        schema (str): Схема бд

    Returns:
        json: Результат загрузки файла
    """
    return f"""
    get_duration()
    {{
        local start_ts=$1
        local end_ts=$(date +%s%N | cut -b1-13)
        local duration=$((end_ts - start_ts))

        return $duration
    }}

    start_ts=$(date +%s%N | cut -b1-13)

    db_params=$(echo {db_params}|base64 -d)

    export PGPASSWORD=$(echo {db_password}|base64 -d)
    echo {sep}

    $db_params -c "\\copy ({query}) to STDOUT with csv header delimiter E'{sep}'"|hadoop dfs -put -f - {dst_path}
    ret_code=$?

    echo "Postgres copy return code="$ret_code

    get_duration $start_ts
    duration=$?

    if [ $ret_code -eq 0 ];# Check bcp result
    then
       echo "{{\\"Schema\\":\\"{schema}\\",\\"EntityName\\":\\"$(basename {dst_path} .csv)\\",\\"Result\\":true,\\"Duration\\":\\"$duration\\"}}"
    else
       echo "{{\\"Schema\\":\\"{schema}\\",\\"EntityName\\":\\"$(basename {dst_path} .csv)\\",\\"Result\\":false,\\"Duration\\":\\"$duration\\"}}"
       exit $ret_code
    fi
    """


def generate_delete_old_files_command(dir, days_to_keep_old_files):
    """Функция генерации shell скрипта удаления файлов старше n дней из hdfs директории

    Args:
        dir (str): hdfs директория
        days_to_keep_old_files (int): Колличество дней

    Returns:
        int: Результат выполенения скрипта
    """
    return f"""today=`date +'%s'`
hdfs dfs -ls {dir} | grep "^d\\|^-" | while read line ; do
dir_date=$(echo ${{line}} | awk '{{print $6}}')
difference=$(( ( ${{today}} - $(date -d ${{dir_date}} +%s) ) / ( 24*60*60 ) ))
filePath=$(echo ${{line}} | awk '{{print $8}}')

if [ ${{difference}} -gt "{days_to_keep_old_files}" ]; then
    hdfs dfs -rm -r $filePath
fi
done"""
