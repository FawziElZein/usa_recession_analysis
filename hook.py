from database_handler import execute_query, create_connection, close_connection, return_data_as_df
from pandas_data_handler import return_insert_into_sql_statement_from_df, return_create_statement_from_df
from lookups import LoggerMessages, ErrorHandling, InputTypes, ETLStep, DestinationDatabase, FinvizWebScrape, PoliticianSpeeches, FredEconomicDataWebScrape, TABLE_TYPE
from datetime import datetime
from misc_handler import execute_sql_folder, create_insert_sql, create_sql_table_index
from logging_handler import show_error_message, show_logger_message
from webscrape_data_handler import get_stock_market_news, get_usa_economic_data, get_states_economic_data, get_politician_speeches
from sentiment_analysis_data_handler import get_sentiment_analysis_results
from stock_market_data_handler import get_stock_market_prices
import logging


def create_etl_checkpoint(db_session, schema_name):
    try:
        query = f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.etl_checkpoint
            (
                etl_last_run_date TIMESTAMP
            )
            """
        execute_query(db_session, query)
    except Exception as error:
        suffix = str(error)
        error_prefix = ErrorHandling.ETL_CHECKPOINT_ERROR
        show_error_message(error_prefix.value, suffix)
        raise Exception("Important Step Failed")


def insert_or_update_etl_checkpoint(db_session, schema_name, does_etl_time_exists, etl_date=None):

    if does_etl_time_exists:

        status, status_message = ErrorHandling.ETL_UPDATE_CHECKPOINT_ERROR, "updating"
        insert_update_stmnt = f"UPDATE {schema_name}.etl_checkpoint SET etl_last_run_date = '{etl_date}'"
    else:
        status, status_message = ErrorHandling.ETL_INSERT_CHECKPOINT_ERROR, "inserting"
        insert_update_stmnt = f"INSERT INTO {schema_name}.etl_checkpoint (etl_last_run_date) VALUES ('{etl_date}')"

    try:
        execute_query(db_session, insert_update_stmnt)
    except Exception as e:
        suffix = str(error)
        error_prefix = status
        show_error_message(error_prefix.value, suffix)
        raise Exception(f"Error while {status_message} ETL checkpoint")


def return_etl_last_updated_date(db_session):

    does_etl_time_exists = False
    query = "SELECT etl_last_run_date FROM dw_reporting.etl_checkpoint ORDER BY etl_last_run_date DESC LIMIT 1"

    try:

        etl_df = return_data_as_df(
            file_executor=query,
            input_type=InputTypes.SQL,
            db_session=db_session
        )
        if len(etl_df) == 0:
            return_date = datetime(2007, 1, 1)
        else:
            return_date = etl_df['etl_last_run_date'].iloc[0]
            does_etl_time_exists = True
    except Exception as error:
        suffix = str(error)
        error_prefix = ErrorHandling.RETURN_ETL_LAST_UPDATE_ERROR
        show_error_message(error_prefix.value, suffix)
    finally:
        return return_date, does_etl_time_exists


def create_and_store_into_table(db_session, df_table_title, sql_table_type, destination_schema):

    target_schema = destination_schema.value
    try:

        for table_title, df in df_table_title:

            dst_table = f"{sql_table_type}_{table_title}"

            create_stmt = return_create_statement_from_df(
                df, target_schema, dst_table)
            execute_query(db_session=db_session, query=create_stmt)

            create_sql_table_index(
                db_session, target_schema, dst_table, df.index.name)

            upsert_query = return_insert_into_sql_statement_from_df(
                df, target_schema, dst_table, is_upsert=True)
            execute_query(db_session, upsert_query)

        logger_string_prefix = ETLStep.HOOK.value
        logger_string_postfix = LoggerMessages.CREATE_AND_STORE_INTO_FACT_AGG_TABLE
        show_logger_message(logger_string_prefix, logger_string_postfix)
    except Exception as e:
        error_prefix = ErrorHandling.CREATE_AND_STORE_INTO_FACT_AGG_TABLE_ERROR
        suffix = str(error)
        show_error_message(error_prefix.value, suffix)

def extract_phase(db_session,etl_date,does_etl_exists):

    get_stock_market_prices(db_session = db_session, etl_datetime = etl_date)
    get_stock_market_news( db_session = db_session, etl_date = etl_date, does_etl_exists = does_etl_time_exists)
    get_usa_economic_data( db_session = db_session, etl_datetime = etl_date, does_etl_exists = does_etl_time_exists)
    get_states_economic_data( db_session = db_session, etl_datetime = etl_date, does_etl_exists = does_etl_time_exists)
    get_politician_speeches(db_session = db_session, etl_datetime = etl_date)

def transform_phase(db_session,schema_name):

    execute_sql_folder(db_session, './SQL_Commands', ETLStep.HOOK, table_types=[TABLE_TYPE.DIM])
    list_df_title_pairs = get_sentiment_analysis_results(db_session, [FinvizWebScrape, PoliticianSpeeches])
    create_and_store_into_table(db_session, list_df_title_pairs, sql_table_type=TABLE_TYPE.FACT, destination_schema=schema_name)
    execute_sql_folder(db_session, './SQL_Commands', ETLStep.HOOK, table_types=[TABLE_TYPE.FACT, TABLE_TYPE.AGG])

def load_phase(db_session):
    execute_sql_folder(db_session, './SQL_Commands', ETLStep.HOOK, table_types=[TABLE_TYPE.VIEW])

def execute_hook():

    logger_string_prefix = ETLStep.HOOK.value
    logger_string_postfix = "start"
    show_logger_message(logger_string_prefix, logger_string_postfix)
    schema_name = DestinationDatabase.SCHEMA_NAME
    try:
        db_session = create_connection()
        create_etl_checkpoint(db_session,schema_name)
        etl_date, does_etl_time_exists = return_etl_last_updated_date(
            db_session)

        extract_phase(db_session = db_session, etl_date = etl_date, does_etl_exists= does_etl_time_exists )
    
        transform_phase(db_session = db_session,schema_name = schema_name)
        
        load_phase(db_session = db_session)
    
        # last step
        insert_or_update_etl_checkpoint(
            db_session, schema_name, does_etl_time_exists, datetime.now())
        close_connection(db_session)

        logger_string_prefix = ETLStep.HOOK.value
        logger_string_postfix = "executed successfully"
        show_logger_message(logger_string_prefix, logger_string_postfix)

    except Exception as error:
        suffix = str(error)
        error_prefix = ErrorHandling.HOOK_SQL_ERROR
        show_error_message(error_prefix.value, suffix)
        raise Exception("Important Step Failed")
