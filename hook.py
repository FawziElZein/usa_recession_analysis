from database_handler import execute_query, create_connection, close_connection,return_data_as_df
from pandas_data_handler import return_insert_into_sql_statement_from_df
from lookups import ErrorHandling,InputTypes,ETLStep,DestinationDatabase,FinvizWebScrape
from datetime import datetime
from misc_handler import execute_sql_folder, create_insert_sql
from logging_handler import show_error_message
from webscrape import get_webscrape_data_from_finviz,get_usa_webscrapping_data,get_states_webscraping_data
from sentiment_analysis import store_sentiment_analysis_into_fact_table
from faang_stock_market_prices import get_faang_historical_prices
from politicians_speeches_and_social_media_posts import get_politician_social_media_posts

def create_etl_checkpoint(db_session):
    try:
        query = """
            CREATE TABLE IF NOT EXISTS dw_reporting.etl_checkpoint
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

def insert_or_update_etl_checkpoint(db_session,does_etl_time_exists, etl_date = None):

    if does_etl_time_exists:

        status,status_message = ErrorHandling.ETL_UPDATE_CHECKPOINT_ERROR,"updating"
        insert_update_stmnt = f"UPDATE dw_reporting.etl_checkpoint SET etl_last_run_date = '{etl_date}'"

    else:

        status,status_message = ErrorHandling.ETL_INSERT_CHECKPOINT_ERROR,"inserting"
        insert_update_stmnt = f"INSERT INTO dw_reporting.etl_checkpoint (etl_last_run_date) VALUES ('{etl_date}')"

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
            file_executor= query,
            input_type= InputTypes.SQL,
            db_session= db_session
        )
        if len(etl_df) == 0:
            return_date = datetime(2007,1,1)
        else:
            return_date = etl_df['etl_last_run_date'].iloc[0]
            does_etl_time_exists = True
    except Exception as error:
        suffix = str(error)
        error_prefix = ErrorHandling.RETURN_ETL_LAST_UPDATE_ERROR
        show_error_message(error_prefix.value, suffix)
    finally:
        return return_date,does_etl_time_exists

def execute_hook():
    try:
        db_session = create_connection()
        create_etl_checkpoint(db_session)
        etl_date, does_etl_time_exists = return_etl_last_updated_date(db_session)

        # get_faang_historical_prices(db_session,etl_datetime=etl_date)
        # get_webscrape_data_from_finviz(db_session=db_session,etl_date=etl_date,does_etl_exists=does_etl_time_exists)
        # get_usa_webscrapping_data(db_session = db_session,etl_datetime = etl_date,does_etl_exists = does_etl_time_exists)
        # get_states_webscraping_data(db_session = db_session,etl_datetime = etl_date,does_etl_exists = does_etl_time_exists)
        get_politician_social_media_posts(db_session)
        #Might not be in need
        # create_insert_sql(db_session,src_names,df_titles,df_src_content, ETLStep.HOOK, etl_date)

        # store_sentiment_analysis_into_fact_table(db_session)
        execute_sql_folder(db_session, './SQL_Commands', ETLStep.HOOK)

        #last step
        insert_or_update_etl_checkpoint(db_session, does_etl_time_exists,datetime.now())
        close_connection(db_session)
    except Exception as error:
        suffix = str(error)
        error_prefix = ErrorHandling.HOOK_SQL_ERROR
        show_error_message(error_prefix.value, suffix)
        raise Exception("Important Step Failed")
        