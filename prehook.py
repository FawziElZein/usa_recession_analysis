from pandas_data_handler import return_create_statement_from_df,return_insert_into_sql_statement_from_df
from lookups import Logger, ErrorHandling, InputTypes,DestinationDatabase,ETLStep
from database_handler import return_query,execute_query, create_connection, close_connection,return_data_as_df
from misc_handler import execute_sql_folder
from logging_handler import show_error_message,show_logger_message
import datetime


def execute_prehook(sql_command_directory_path = './SQL_Commands'):

    etl_step = ETLStep.PRE_HOOK.value
    logger_string_postfix = Logger.EXECUTE.value
    show_logger_message(etl_step,logger_string_postfix)

    try:
        logger_string_postfix = Logger.CREATE_CONNECTION.value
        show_logger_message(etl_step,logger_string_postfix)
        db_session = create_connection()
        logger_string_postfix = Logger.EXECUTE_SQL_FOLDER.value
        show_logger_message(etl_step,logger_string_postfix)
        execute_sql_folder(db_session, sql_command_directory_path, ETLStep.PRE_HOOK, table_types = None, target_schema= DestinationDatabase.SCHEMA_NAME)
        logger_string_postfix = Logger.CLOSE_DB_CONNECTION.value
        show_logger_message(etl_step,logger_string_postfix)
        close_connection(db_session)

    except Exception as error:
        suffix = str(error)
        error_prefix = ErrorHandling.PREHOOK_SQL_ERROR
        show_error_message(error_prefix.value, suffix)
        raise Exception("Important Step Failed")
