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


def generate_db_schema_query(environment=None, upload_date=None, black_list=None, white_list=None):
    environment_name = environment.strip() if environment else ""
    upload_date = upload_date if upload_date else datetime.today().strftime("%Y-%m-%d %H:%M:%S")

    black_list_sql = " AND ".join(
        ["NOT TABLE_SCHEMA+'.'+TABLE_NAME Like '{}'".format(x) for x in black_list.split(PARAM_DELIMETER)]) if black_list else ""

    combined_list_sql = " AND {}".format(
        black_list_sql) if black_list_sql else ""

    white_list_sql = "("+(" OR ".join(["TABLE_SCHEMA+'.'+TABLE_NAME Like '{}'".format(
        x) for x in white_list.split(PARAM_DELIMETER)])) + ")" if white_list else ""

    combined_list_sql = "{} AND {}".format(combined_list_sql,
                                           white_list_sql) if white_list_sql else combined_list_sql

    script = """Select 
                Table_Schema as [Schema],
                TABLE_NAME as TableName,
                Column_Name as FieldName,
                Ordinal_Position as Position,
                Data_Type as FieldType,
                Coalesce(CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, DATETIME_PRECISION) as Size,
                IIF(IS_NULLABLE='NO',0, IIF(IS_NULLABLE='YES',1,null)) as [IsNull],
                CONVERT(nvarchar(20),'{}',23) as updateDate,
                NUMERIC_PRECISION as Scale
                from  INFORMATION_SCHEMA.COLUMNS
                Where Table_Schema <>'sys' {}""".format(upload_date, combined_list_sql)

    print(script)
    return script


def generate_table_select_query(current_upload_date, last_upload_date, actual_schema_file, delta_tables = []):
    df = pd.read_csv(actual_schema_file, keep_default_na=False,sep=CSV_SEPARATOR)
    rows = df.to_dict('records')
    grouped_rows = {i: list(j) for (i, j) in groupby(
        rows, lambda x: (x["Schema"], x["TableName"]))}

    result = []
    for table, columns in grouped_rows.items():
        print(table)
        print()
        fields_list = []
        column_name_list = []
        for column in columns:
            if column["FieldType"] == 'nvarchar':
                fields_list.append("REPLACE(REPLACE([{field_name}],CHAR(10),''),CHAR(13),'') [{field_name}]".format(
                    field_name=column["FieldName"]))
            elif column["FieldType"] == 'numeric' and column["Size"] == 38:
                fields_list.append("CONVERT(NVARCHAR(40),[{field_name}]) [{field_name}]".format(
                    field_name=column["FieldName"]))
            elif column["FieldType"] == 'datetimeoffset' and column["FieldName"] != 'STAMP':
                fields_list.append("""IIF(DATEPART(YEAR, [{field_name}])<1900,
                IIF(DATEPART(TZ, [{field_name}])<0,
                        DATEADD(HOUR, -3, CONVERT(DATETIME,DATEADD(YEAR, 1900-DATEPART(YEAR, [{field_name}]), [{field_name}]),0)),
                            CONVERT(DATETIME,DATEADD(YEAR, 1900-DATEPART(YEAR, [{field_name}]), [{field_name}]),0)),
                            IIF(DATEPART(TZ, [{field_name}])<0,DATEADD(HOUR, -3,
                             CONVERT(DATETIME,[{field_name}],0)),CONVERT(DATETIME,[{field_name}],1))) [{field_name}]""".format(field_name=column["FieldName"]))
            elif column["FieldType"] == 'datetimeoffset' and column["FieldName"] == 'STAMP':
                fields_list.append("CONVERT(DATETIME,[{field_name}],1) [{field_name}]".format(
                    field_name=column["FieldName"]))
            else:
                fields_list.append("[{field_name}]".format(
                    field_name=column["FieldName"]))

            column_name_list.append(column["FieldName"])        

        fields = ",".join(fields_list)

        column_name_list.append("#QCCount")
        column_names = ",".join(column_name_list)



        method = METHOD_FULL
        if table[1] in delta_tables:
            method = METHOD_DELTA

            script = "SELECT {fields} , (SELECT count(*) FROM {schema}.[{table_name}] WHERE LastModifiedDate BETWEEN CONVERT(nvarchar(20),'{last_modified_date}', 120) AND CONVERT(nvarchar(20),'{current_upload_date}', 120)) [#QCCount] FROM {schema}.[{table_name}] t WHERE t.LastModifiedDate BETWEEN CONVERT(nvarchar(20),'{last_modified_date}', 120) AND CONVERT(nvarchar(20),'{current_upload_date}', 120)".format(
                fields=fields, schema=table[0], table_name=table[1], last_modified_date=last_upload_date, current_upload_date=current_upload_date)
        else:
            script = "SELECT {fields} , (SELECT count(*) FROM {schema}.[{table_name}]) [#QCCount] FROM {schema}.[{table_name}]".format(
                fields=fields, schema=table[0], table_name=table[1])

        result.append(
            {"Schema": table[0], "EntityName": table[1], "Extraction": script, "Method": method, "Columns":column_names})

    result_df = pd.DataFrame(result)
    return result_df

def get_records(odbc_hook, sql, parameters=None, output_converters=[]):
    """
        Executes the sql and returns a list of dicts.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :param parameters: The parameters to render the SQL query with.
        :param output_converters: List of connection output converters
    """
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
    """
        Executes the sql and returns the first resulting row as dict.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :param parameters: The parameters to render the SQL query with.
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
        
def mip_generate_table_select_query(current_upload_date, last_upload_date, actual_schema_file):
    df = pd.read_csv(actual_schema_file, keep_default_na=False,sep=CSV_SEPARATOR)
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
                fields_list.append("REPLACE(REPLACE([{field_name}],CHAR(10),''),CHAR(13),'') [{field_name}]".format(
                    field_name=column["FieldName"]))
            elif column["FieldType"] == 'numeric' and column["Size"] == 38:
                fields_list.append("CONVERT(NVARCHAR(40),[{field_name}]) [{field_name}]".format(
                    field_name=column["FieldName"]))
            elif column["FieldType"] == 'datetimeoffset' and column["FieldName"] != 'STAMP':
                fields_list.append("""IIF(DATEPART(YEAR, [{field_name}])<1900,
                IIF(DATEPART(TZ, [{field_name}])<0,
                        DATEADD(HOUR, -3, CONVERT(DATETIME,DATEADD(YEAR, 1900-DATEPART(YEAR, [{field_name}]), [{field_name}]),0)),
                            CONVERT(DATETIME,DATEADD(YEAR, 1900-DATEPART(YEAR, [{field_name}]), [{field_name}]),0)),
                            IIF(DATEPART(TZ, [{field_name}])<0,DATEADD(HOUR, -3,
                             CONVERT(DATETIME,[{field_name}],0)),CONVERT(DATETIME,[{field_name}],1))) [{field_name}]""".format(field_name=column["FieldName"]))
            elif column["FieldType"] == 'datetimeoffset' and column["FieldName"] == 'STAMP':
                fields_list.append("CONVERT(DATETIME,[{field_name}],1) [{field_name}]".format(
                    field_name=column["FieldName"]))
            else:
                fields_list.append("[{field_name}]".format(
                    field_name=column["FieldName"]))
                
            if column["FieldName"] == 'STAMP':   
                method = METHOD_DELTA
                
            column_name_list.append(column["FieldName"])        

        fields = ",".join(fields_list)

        column_name_list.append("#QCCount")
        column_names = ",".join(column_name_list)



        if method == METHOD_DELTA:
            script = "SELECT {fields} , (SELECT count(*) FROM {schema}.[{table_name}] WHERE STAMP BETWEEN CONVERT(nvarchar(20),'{last_modified_date}', 120) AND CONVERT(nvarchar(20),'{current_upload_date}', 120)) [#QCCount] FROM {schema}.[{table_name}] t WHERE t.STAMP BETWEEN CONVERT(nvarchar(20),'{last_modified_date}', 120) AND CONVERT(nvarchar(20),'{current_upload_date}', 120)".format(
                fields=fields, schema=table[0], table_name=table[1], last_modified_date=last_upload_date, current_upload_date=current_upload_date)
        else:
            script = "SELECT {fields} , (SELECT count(*) FROM {schema}.[{table_name}]) [#QCCount] FROM {schema}.[{table_name}]".format(
                fields=fields, schema=table[0], table_name=table[1])

        result.append(
            {"Schema": table[0], "EntityName": table[1], "Extraction": script, "Method": method, "Columns":column_names})

    result_df = pd.DataFrame(result)
    return result_df
        
