import os
from connection import connect

def get_table_sql_query(schema_name):
    table_sql_query = f"""SELECT 
    c.table_name,c.column_name,
    c.data_type,
    c.character_maximum_length,
    CASE 
    WHEN UPPER(c.data_type) = 'DECIMAL' THEN '['+c.column_name+']'+ ' ' + '['+c.data_type+']' + ' (' + COALESCE(CAST(c.numeric_precision AS VARCHAR(20)),'') +','+COALESCE(CAST(c.numeric_scale AS VARCHAR(20)),'')+')'
    WHEN UPPER(c.data_type) = 'VARCHAR' or UPPER(c.data_type) = 'NVARCHAR' or UPPER(c.data_type) = 'CHAR' THEN '['+c.column_name+']'+ ' ' + '['+c.data_type+']'+'('+CAST(c.character_maximum_length AS VARCHAR(20))+')'
    WHEN UPPER(c.data_type) = 'DATETIME2'  THEN '['+c.column_name+']'+ ' ' + '['+c.data_type+']'+'(7)'
    ELSE '['+c.column_name+']'+ ' ' + '['+c.data_type+']' END as derivedcolumn,
    CASE WHEN c.collation_name IS NOT NULL THEN 'COLLATE '+c.collation_name ELSE c.collation_name END AS collation_name,
    et.location
    FROM INFORMATION_SCHEMA.COLUMNS c
    LEFT JOIN sys.external_tables et
    ON TRIM(c.table_name) = TRIM(et.name)
    WHERE c.table_schema = '{schema_name}'
    ORDER BY c.table_name, c.ORDINAL_POSITION asc"""
    return table_sql_query
def create_table_ddl_files(schema_name, cursor):
   table_sql_query = get_table_sql_query(schema_name)
   cursor.execute(table_sql_query)
   table_rows = cursor.fetchall()
   ddl_directory = f"{schema_name}_ExternalTables_DDL"
   os.makedirs(ddl_directory, exist_ok=True)
   for row in table_rows:
       table_name, _, _, _, derived_column, _, location = row
       data_source = "hub" if schema_name[:3].lower() == "hub" else "pub"
       ddl = f"CREATE EXTERNAL TABLE {schema_name}.{table_name} (\n"
       ddl += f"    {derived_column},\n"
       ddl += f"    DATA_SOURCE = {data_source}\n"
       ddl += f"    LOCATION = '{location}'\n"
       ddl += "    FILE_FORMAT = [SynapseParquetFormat]\n"
       ddl += ");"
       file_path = os.path.join(ddl_directory, f"{table_name}.sql")
       with open(file_path, "w") as ddl_file:
           ddl_file.write(ddl)
# Take input for schema
schema_name = input('Enter Schema Name: ')
# Get table SQL query and execute
cursor = connect.cursor()
create_table_ddl_files(schema_name, cursor)
