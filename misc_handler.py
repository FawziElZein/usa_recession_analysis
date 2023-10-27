import os
from lookups import ErrorHandling,LoggerMessages,ETLStep,InputTypes,DateField,DestinationDatabase,FinvizWebScrape
from database_handler import return_query,execute_query, create_connection, close_connection,return_data_as_df
from pandas_data_handler import return_create_statement_from_df,return_insert_into_sql_statement_from_df
from logging_handler import show_error_message,show_logger_message


def return_staging_tables_as_list(db_session):
    
    get_staging_tables_query = """
    SELECT 
        table_name
    FROM information_schema.tables
    WHERE
        table_type = 'BASE TABLE'
        AND table_schema = 'dw_reporting'
        AND table_name LIKE 'stg%';
    """
    staging_tables = return_query(db_session, get_staging_tables_query)

    table_names = [item[0] for item in staging_tables]
    return table_names

    
def return_lookup_items_as_dict(lookup_item):
    enum_dict = {str(item.name).lower():item.value.replace(item.name.lower() + "_","") for item in lookup_item}
    return enum_dict


def return_tables_by_schema(schema_name):
    schema_tables = list()
    tables = [table.value for table in SQLTablesToReplicate]
    for table in tables:
        if table.split('.')[0] == schema_name:
            schema_tables.append(table.split('.')[1])
    return schema_tables

def is_hook_file_title_executable(etl_step,file_title,table_types):

    if etl_step.value == ETLStep.PRE_HOOK.value:
        return True
    
    for table_type in table_types:
        if file_title == table_type:
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
    logger_string_suffix = LoggerMessages.SQL_FOLDER_EXECUTION
    show_logger_message(logger_string_prefix,logger_string_suffix)


def create_sql_table_index(db_session,source_name, table_name, index_val):
    query = f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{index_val} ON {source_name}.{table_name} ({index_val});"
    execute_query(db_session,query)

def create_insert_sql(db_session,source_names,df_titles,df_source_list,etl_step,etl_date = None):
    destination_schema_name = DestinationDatabase.SCHEMA_NAME.value
    try:
        for source_name,df_title, df_source in zip(source_names,df_titles,df_source_list):
            dst_table = f"stg_{source_name}_{df_title}"
            if etl_step == ETLStep.PRE_HOOK:
                create_stmt = return_create_statement_from_df(df_source, destination_schema_name, dst_table)
                execute_query(db_session = db_session, query= create_stmt)
                create_sql_table_index(db_session, destination_schema_name, dst_table, df_source.index.name)
            elif etl_step == ETLStep.HOOK:
                date_dict = return_lookup_items_as_dict(DateField)
                date_column = date_dict.get(df_title)
                if date_column =='index':
                    staging_df = df_source[df_source.index>etl_date]
                else:
                    staging_df = df_source[df_source[date_column]>etl_date]
                if len(staging_df):
                    insert_stmt = return_insert_into_sql_statement_from_df(staging_df, destination_schema_name, dst_table)
                    execute_query(db_session=db_session, query= insert_stmt)
        logger_string_prefix = ETLStep.PRE_HOOK.value
        logger_string_suffix = LoggerMessages.SQL_FOLDER_EXECUTION.value
        show_logger_message(logger_string_prefix,logger_string_suffix)
    except Exception as error:
        suffix = str(error)
        error_prefix = ErrorHandling.CREATE_INSERT_STAGING_TABLES_ERROR
        show_error_message(error_prefix.value, suffix)
        raise Exception("Error creating/insert into staging tables")


