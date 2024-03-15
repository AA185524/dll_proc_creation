import os
import csv
from config import connect
import concurrent.futures
import pyodbc

def get_table_sql_query(schema_name):
    table_sql_query = f"""SELECT
    TRIM(c.table_name) as table_name,c.column_name,
    c.data_type,
    c.character_maximum_length,
    CASE 
    WHEN UPPER(c.data_type) = 'DECIMAL' THEN '['+c.column_name+']'+ ' ' + '['+c.data_type+']' + ' (' + COALESCE(CAST(c.numeric_precision AS VARCHAR(20)),'') +','+COALESCE(CAST(c.numeric_scale AS VARCHAR(20)),'')+') '+COALESCE(CAST(' COLLATE '+ c.collation_name AS VARCHAR(50)),'')
    WHEN UPPER(c.data_type) = 'VARCHAR' or UPPER(c.data_type) = 'NVARCHAR' or UPPER(c.data_type) = 'CHAR' OR UPPER(c.data_type) = 'VARBINARY' THEN '['+c.column_name+']'+ ' ' + '['+c.data_type+']'+'('+CAST(c.character_maximum_length AS VARCHAR(20))+')'+COALESCE(CAST(' COLLATE '+ c.collation_name AS VARCHAR(50)),'')
    WHEN UPPER(c.data_type) = 'DATETIME2'  THEN '['+c.column_name+']'+ ' ' + '['+c.data_type+']'+'(7)'+COALESCE(CAST(' COLLATE '+c.collation_name AS VARCHAR(50)),'')
    ELSE '['+c.column_name+']'+ ' ' + '['+c.data_type+']'+COALESCE(CAST(' COLLATE '+c.collation_name AS VARCHAR(50)),'') END as derivedcolumn,
    et.location,
    eff.name as ext_file_format,
    eds.name as ext_data_source
    FROM INFORMATION_SCHEMA.COLUMNS c
    LEFT JOIN sys.external_tables et
    ON TRIM(c.table_name) = TRIM(et.name)
    LEFT JOIN sys.external_file_formats eff 
    on et.file_format_id = eff.file_format_id
    LEFT JOIN sys.external_data_sources eds 
    on eds.data_source_id = et.data_source_id
    WHERE c.table_schema = '{schema_name}'
    GROUP BY 
    TRIM(c.table_name),
    c.column_name,
    c.data_type,
    c.character_maximum_length,
    et.location,
    eff.name,
    eds.name,
    c.numeric_precision,
    c.numeric_scale,
    c.collation_name,
    c.table_name,
     c.ORDINAL_POSITION
    ORDER BY c.table_name, c.ORDINAL_POSITION asc"""
    return table_sql_query

def export_query_result_to_csv(schema_name):
    cursor = connect.cursor()
    table_sql_query = get_table_sql_query(schema_name)
    cursor.execute(table_sql_query)
    table_rows = cursor.fetchall()
    csv_path = f'serverless1/csv_files'
    if not os.path.exists(csv_path):
        os.makedirs(csv_path)
    csv_file_path = f"{csv_path}/{schema_name}_query_result.csv"

    with open(csv_file_path, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Write header
        header = ['table_name', 'column_name', 'data_type', 'character_maximum_length', 'derivedcolumn', 'location','ext_file_format','ext_data_source']
        csv_writer.writerow(header)

        # Write data
        csv_writer.writerows(table_rows)

#To generate DDL from CSV
    create_table_ddl_files_from_csv(schema_name)


def create_table_ddl_files_from_csv(schema_name):
    csv_path = f'serverless1/csv_files'
    csv_file_path = f"{csv_path}/{schema_name}_query_result.csv"
    with open(csv_file_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        current_table = None
        current_columns = []

        for row in csv_reader:
            table_name = row['table_name']
            derived_column = row['derivedcolumn']
            location = row['location']
            ext_file_format = row['ext_file_format']
            ext_data_source = row['ext_data_source']

            if current_table is None:
                current_table = table_name
                current_columns = []
                current_location = location
                current_ext_file_format = ext_file_format
                current_ext_data_source = ext_data_source

            if table_name != current_table:
                # Process the previous table and create DDL
                create_table_ddl(current_table, current_columns, current_location, schema_name, current_ext_file_format, current_ext_data_source)

                # Reset for the new table
                current_table = table_name
                current_columns = []
                current_location = location
                current_ext_file_format = ext_file_format
                current_ext_data_source = ext_data_source
                

            current_columns.append(derived_column)

        # Process the last table
        create_table_ddl(current_table, current_columns, current_location, schema_name, current_ext_file_format, current_ext_data_source)


def create_table_ddl(table_name, derived_columns, location, schema_name, ext_file_format, ext_data_source):
    ddl_path = f'serverless1/external_tables/{schema_name}'
    if not os.path.exists(ddl_path):
        os.makedirs(ddl_path)
    ddl = f"CREATE EXTERNAL TABLE {schema_name}.{table_name} (\n"
    for column in derived_columns:
        ddl += f"    {column},\n"
    ddl = ddl.rstrip(',\n')  # Remove the trailing comma and newline
    ddl += "\n)\nWITH\n"
    ddl += '('
    ddl += f"    DATA_SOURCE = [{ext_data_source}],\n"
    ddl += f"     LOCATION = N'{location}',\n"
    ddl += f'     FILE_FORMAT = [{ext_file_format}]\n)'

    ddl_file_path = (f"{ddl_path}/{table_name}.sql")
    with open(ddl_file_path, "w") as ddl_file:
        ddl_file.write(ddl)

def extract_views(schema_name_v):
    rls_path = f'serverless1/views_rls/{schema_name_v}'
    wo_rls_path = f'serverless1/views_wo_rls/{schema_name_v}'
    if not os.path.exists(rls_path):
        os.makedirs(rls_path)
    
    if not os.path.exists(wo_rls_path):
        os.makedirs(wo_rls_path)

    view_sql_query = f"""SELECT
      TRIM(TABLE_NAME) AS TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS
      WHERE TABLE_SCHEMA = '{schema_name_v}'
      --AND TABLE_NAME = 'GL_Detail_Transactional_Details_vw2'
      AND TRIM(TABLE_NAME) != ''
      ORDER BY TABLE_NAME"""
    cursor = connect.cursor()
    cursor.execute(view_sql_query)
    table_rows = cursor.fetchall()

    for name in table_rows:
        name = name[0]
        query = f"""SELECT TRIM(OBJECT_DEFINITION
        (OBJECT_ID(N'{schema_name_v}.{name}')))AS [Object Definition]"""
        connect.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        connect.setencoding('utf-8')
        cursor = connect.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        result = data[0]
        print


    
        if 'separation_security' in result[0]:
            with open(f"{rls_path}/{name}.sql",'w',encoding='utf-8') as sql_file:
                sql_file.write(result[0])
        else:
            # if name == 'CS_Customer_Engineer_Service_Request_Count_by_Country_for_Industry_Account_Territory_with_Us_Data_vw':
            #     name = 'CS_Customer_Engineer_Service_Request_Count_by_with_US_data_vw'

            with open(f"{wo_rls_path}/{name}.sql",'w',encoding='utf-8') as sql_file:
                sql_file.write(result[0])  
def get_schema(file_name):
        return open(file_name).read().replace('\n', ' ').strip().split(' ')

# Take input for schema
schema_name = get_schema('schema.txt')
schema_name_v = get_schema('view_schema.txt')
schema_name_v = ['pub_generalledger_SAFT_reporting_v']

with concurrent.futures.ThreadPoolExecutor(1) as executor:
    executor.map(extract_views, schema_name_v)

with concurrent.futures.ThreadPoolExecutor(1) as executor:
    executor.map(export_query_result_to_csv, schema_name)
