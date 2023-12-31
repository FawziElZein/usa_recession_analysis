from database_handler import execute_query, create_connection, close_connection, return_data_as_df
from pandas_data_handler import return_insert_into_sql_statement_from_df, return_create_statement_from_df
from lookups import Logger, ErrorHandling, InputTypes, ETLStep, DestinationDatabase, FinvizWebScrape, PoliticianSpeeches, FredEconomicDataWebScrape, TABLE_TYPE
from datetime import datetime
from misc_handler import execute_sql_folder, create_sql_table_index
from logging_handler import show_error_message, show_logger_message
from webscrape_data_handler import get_stock_market_news, get_usa_economic_data, get_states_economic_data, get_politician_speeches
from sentiment_analysis_data_handler import get_sentiment_analysis_results
from stock_market_data_handler import get_stock_market_prices
import logging
from selenium import webdriver
from tempfile import mkdtemp


def create_etl_checkpoint(db_session, schema_name):
    schema_name = schema_name.value
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
    schema_name = schema_name.value
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
    sql_table_type = sql_table_type.value
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


    except Exception as e:
        error_prefix = ErrorHandling.CREATE_AND_STORE_INTO_FACT_AGG_TABLE_ERROR
        suffix = str(error)
        show_error_message(error_prefix.value, suffix)

def intialize_chrome_webdriver():

    options = webdriver.ChromeOptions()
    options.binary_location = '/opt/chrome/chrome'
    options.add_argument("--headless=new")
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280x1696")
    options.add_argument("--single-process")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-dev-tools")
    options.add_argument("--no-zygote")
    options.add_argument(f"--user-data-dir={mkdtemp()}")
    options.add_argument(f"--data-path={mkdtemp()}")
    options.add_argument(f"--disk-cache-dir={mkdtemp()}")
    options.add_argument("--remote-debugging-port=9222")
    return options

def extract_phase(db_session,etl_date,does_etl_exists):

    options = intialize_chrome_webdriver()
    get_stock_market_prices(db_session = db_session, etl_datetime = etl_date)
    get_stock_market_news( db_session = db_session, etl_date = etl_date, does_etl_exists = does_etl_exists)
    get_usa_economic_data( db_session = db_session, etl_datetime = etl_date, does_etl_exists = does_etl_exists, options = options)
    get_states_economic_data( db_session = db_session, etl_datetime = etl_date, does_etl_exists = does_etl_exists, options = options)
    get_politician_speeches(db_session = db_session, etl_datetime = etl_date, options = options)

def transform_phase(db_session,schema_name):

    execute_sql_folder(db_session, './SQL_Commands', ETLStep.HOOK, table_types = [TABLE_TYPE.DIM])
    list_df_title_pairs = get_sentiment_analysis_results(db_session, [FinvizWebScrape, PoliticianSpeeches])
    create_and_store_into_table(db_session, list_df_title_pairs, sql_table_type=TABLE_TYPE.FACT, destination_schema=schema_name)
    execute_sql_folder(db_session, './SQL_Commands', ETLStep.HOOK, table_types=[TABLE_TYPE.FACT, TABLE_TYPE.AGG])

def load_phase(db_session):
    execute_sql_folder(db_session, './SQL_Commands', ETLStep.HOOK, table_types=[TABLE_TYPE.VIEW])

def execute_hook():

    schema_name = DestinationDatabase.SCHEMA_NAME

    etl_step = ETLStep.HOOK.value
    logger_string_postfix = Logger.EXECUTE.value
    show_logger_message(etl_step, logger_string_postfix)

    
    try:
        logger_string_postfix = Logger.CREATE_CONNECTION.value
        show_logger_message(etl_step, logger_string_postfix)
        db_session = create_connection()

        logger_string_postfix = Logger.CREATE_CHECKPOINT.value
        show_logger_message(etl_step, logger_string_postfix)
        create_etl_checkpoint(db_session,schema_name)

        logger_string_postfix = Logger.RETRIEVE_LAST_ETL.value
        show_logger_message(etl_step, logger_string_postfix)

        etl_date, does_etl_time_exists = return_etl_last_updated_date(
            db_session)

        extract_phase(db_session = db_session, etl_date = etl_date, does_etl_exists= does_etl_time_exists )
    
        transform_phase(db_session = db_session,schema_name = schema_name)
        
        load_phase(db_session = db_session)
    
        logger_string_postfix = Logger.UPSERT_ETL.value
        show_logger_message(etl_step, logger_string_postfix)

        insert_or_update_etl_checkpoint(db_session, schema_name, does_etl_time_exists, datetime.now())

        logger_string_postfix = Logger.CLOSE_DB_CONNECTION.value
        show_logger_message(etl_step, logger_string_postfix)
        close_connection(db_session)

    except Exception as error:
        suffix = str(error)
        error_prefix = ErrorHandling.HOOK_SQL_ERROR
        show_error_message(error_prefix.value, suffix)
        raise Exception("Important Step Failed")
