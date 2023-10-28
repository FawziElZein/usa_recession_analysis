import os
from lookups import ErrorHandling,LoggerMessages,ETLStep,InputTypes,DestinationDatabase,FinvizWebScrape
from database_handler import return_query,execute_query, create_connection, close_connection,return_data_as_df
from pandas_data_handler import return_create_statement_from_df,return_insert_into_sql_statement_from_df
from logging_handler import show_error_message,show_logger_message


def return_staging_tables_as_list(db_session,target_schema):
    
    get_staging_tables_query = f"""
    SELECT 
        table_name
    FROM information_schema.tables
    WHERE
        table_type = 'BASE TABLE'
        AND table_schema = '{target_schema.value}'
        AND table_name LIKE 'stg%';
    """
    staging_tables = return_query(db_session, get_staging_tables_query)

    table_names = [item[0] for item in staging_tables]
    return table_names

    
def is_hook_file_title_executable(etl_step,file_title,table_types):

    if etl_step.value == ETLStep.PRE_HOOK.value:
        return True
    
    for table_type in table_types:
        if file_title == table_type.value:
            return True
    
    return False

def execute_sql_folder(db_session, sql_command_directory_path, etl_step, table_types, target_schema = DestinationDatabase.SCHEMA_NAME):

    sql_files = [sqlfile for sqlfile in os.listdir(sql_command_directory_path) if sqlfile.endswith('.sql')]
    sorted_sql_files = sorted(sql_files, key=lambda x: int(x[1:x.index('-')]))
    
    for sql_file in sorted_sql_files:
        file_title = sql_file.split('-')
        if file_title[1] == etl_step.value and is_hook_file_title_executable(etl_step,file_title[2],table_types):
            
            with open(os.path.join(sql_command_directory_path,sql_file), 'r') as file:
                sql_query = file.read()
                sql_query = sql_query.replace('target_schema', target_schema.value)
                return_val = execute_query(db_session= db_session, query= sql_query)
                if not return_val == ErrorHandling.NO_ERROR:
                    raise Exception(f"Error executing SQL File on = " +  str(sql_file))

    logger_string_prefix = etl_step.value
    logger_string_suffix = LoggerMessages.SQL_FOLDER_EXECUTION.value
    show_logger_message(logger_string_prefix,logger_string_suffix)


def create_sql_table_index(db_session,source_name, table_name, index_val):
    query = f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{index_val} ON {source_name}.{table_name} ({index_val});"
    execute_query(db_session,query)



