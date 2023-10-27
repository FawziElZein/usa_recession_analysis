from pandas_data_handler import return_create_statement_from_df,return_insert_into_sql_statement_from_df,get_online_csv_into_df_list
from lookups import ErrorHandling, InputTypes,DestinationDatabase,ETLStep
from database_handler import return_query,execute_query, create_connection, close_connection,return_data_as_df
from misc_handler import execute_sql_folder,create_insert_sql
from logging_handler import show_error_message,show_logger_message
import datetime
import os


def execute_prehook(sql_command_directory_path = './SQL_Commands'):

    logger_string_prefix = ETLStep.HOOK.value
    logger_string_postfix = "executing prehook"
    show_logger_message(logger_string_prefix,logger_string_postfix)

    try:
        db_session = create_connection()
        execute_sql_folder(db_session, sql_command_directory_path, ETLStep.PRE_HOOK, table_types = None, target_schema= DestinationDatabase.SCHEMA_NAME)
        close_connection(db_session)

    except Exception as error:
        suffix = str(error)
        error_prefix = ErrorHandling.PREHOOK_SQL_ERROR
        show_error_message(error_prefix.value, suffix)
        raise Exception("Important Step Failed")
