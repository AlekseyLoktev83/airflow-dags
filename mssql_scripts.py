from datetime import datetime
import pandas as pd
from itertools import groupby
from io import StringIO

PARAM_DELIMETER = ";"
METHOD_FULL = "FULL"
METHOD_DELTA = "DELTA"


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

    script = """Select 'Schema', 'TableName', 'FieldName', 'Position', 'FieldType', 'Size', 'IsNull','UpdateDate','Scale'
                union all
                Select 
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


def generate_table_select_query(current_upload_date, last_upload_date, actual_schema_file):
    df = pd.read_csv(actual_schema_file, keep_default_na=False)
    rows = df.to_dict('records')
    grouped_rows = {i: list(j) for (i, j) in groupby(
        rows, lambda x: (x["Schema"], x["TableName"]))}

    result = []
    for table, columns in grouped_rows.items():
        print(table)
        print()
        fields_list = []
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

        fields = ",".join(fields_list)

        method = METHOD_FULL
        if table[1] in ["BaseLine", "IncrementalPromo", "YA_DATAMART_DELTA"]:
            method = METHOD_DELTA

            script = "SELECT {fields} , (SELECT count(*) FROM {schema}.[{table_name}] WHERE LastModifiedDate BETWEEN CONVERT(nvarchar(20),'{last_modified_date}', 120) AND CONVERT(nvarchar(20),'{current_upload_date}', 120)) [$QCCount] FROM {schema}.[{table_name}] t WHERE t.LastModifiedDate BETWEEN CONVERT(nvarchar(20),'{last_modified_date}', 120) AND CONVERT(nvarchar(20),'{current_upload_date}', 120)".format(
                fields=fields, schema=table[0], table_name=table[1], last_modified_date=last_upload_date, current_upload_date=current_upload_date)
        else:
            script = "SELECT {fields} , (SELECT count(*) FROM {schema}.[{table_name}]) [$QCCount] FROM {schema}.[{table_name}]".format(
                fields=fields, schema=table[0], table_name=table[1])

        result.append(
            {"Schema": table[0], "EntityName": table[1], "Extraction": script, "Method": method})

    result_df = pd.DataFrame(result)
    csv_buffer = StringIO()
    result_df.to_csv(csv_buffer, index=False)

    print(csv_buffer.getvalue())
    return csv_buffer.getvalue()


