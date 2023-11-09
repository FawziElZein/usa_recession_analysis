import psycopg2
from lookups import ErrorHandling, InputTypes
from logging_handler import show_error_message
from dateutil.parser import parse
import datetime
import pandas as pd
import os
from dotenv import load_dotenv
import warnings

load_dotenv()
database_name = os.getenv("DATABASE_NAME")
host = os.getenv("HOST")
port = os.getenv("PORT_NUMBER")
user = os.getenv("USER")
password = os.getenv("PASSWORD")

config_dict = {
    "database": database_name,
    "host":host,
    "port":port,
    "user":user,
    "password": password
}

def create_connection():
    db_session = None
    try:
        db_session = psycopg2.connect(**config_dict)
    except Exception as e:
        error_string_prefix = ErrorHandling.DB_CONNECT_ERROR.value
        error_string_suffix = str(e)
        show_error_message(error_string_prefix, error_string_suffix)
    finally:
        return db_session
    
def return_query(db_session,query):
    results = None
    try:
        cursor = db_session.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        db_session.commit()
    except Exception as e:
        error_string_prefix = ErrorHandling.DB_RETURN_QUERY_ERROR.value
        error_string_suffix = str(e)
        show_error_message(error_string_prefix, error_string_suffix)
    finally:
        return results


def parse_date_columns(dataframe,first_row):
    for index,val in zip(first_row.index, first_row):
        try:
            parsed_date = parse(val, fuzzy=True)
            if isinstance(parsed_date, datetime.datetime):
                dataframe[index] = pd.to_datetime(dataframe[index])
        except Exception as e:
            suffix = str(e)
            error_prefix = ErrorHandling.DATE_CONVERSION_ERROR.value
            show_error_message(error_prefix,suffix)
    
def return_data_as_df(file_executor, input_type, db_session = None):
    return_dataframe = None
    try:
        if input_type == InputTypes.CSV:
            return_dataframe = pd.read_csv(file_executor)
            parse_date_columns(return_dataframe,return_dataframe.iloc[0])
        elif input_type == InputTypes.EXCEL:
            return_dataframe = pd.read_excel(file_executor)
            parse_date_columns(return_dataframe,return_dataframe.iloc[0])
        elif input_type == InputTypes.SQL:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                return_dataframe = pd.read_sql_query(con= db_session, sql= file_executor)
        else:
            raise Exception("The file type does not exist, please check main function")
    except Exception as e:
        suffix = str(e)
        if input_type == InputTypes.CSV:
            error_prefix = ErrorHandling.RETURN_DATA_CSV_ERROR.value
        elif input_type == InputTypes.EXCEL:
            error_prefix = ErrorHandling.RETURN_DATA_EXCEL_ERROR.value
        elif input_type == InputTypes.SQL:
            error_prefix = ErrorHandling.RETURN_DATA_SQL_ERROR.value
        else:
            error_prefix = ErrorHandling.RETURN_DATA_UNDEFINED_ERROR.value
        show_error_message(error_prefix, suffix)
    finally:
        return return_dataframe


def get_column_names_from_sql_table(schema_name,table_name):

    db_session = create_connection()
    target_columns_query = f"""

                    SELECT 
                        column_name
                    FROM information_schema.columns
                    WHERE table_schema = '{schema_name}'
                    AND table_name = '{table_name}'; 
                """

    target_columns_result = return_query(db_session=db_session,query= target_columns_query)
    column_names = [item[0] for item in target_columns_result]
    return column_names
            
def execute_query(db_session, query):
    return_val = ErrorHandling.NO_ERROR
    try:
        cursor = db_session.cursor()
        cursor.execute(query)
        db_session.commit()
    except Exception as e:
        error_prefix = ErrorHandling.EXECUTE_QUERY_ERROR
        return_val = error_prefix
        suffix = str(e)
        show_error_message(error_prefix.value, suffix)
        
    finally:
        return return_val

def close_connection(db_session):
    db_session.close()
