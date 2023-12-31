from yahoofinancials import YahooFinancials
import pandas as pd
from database_handler import execute_query,return_query,parse_date_columns
from pandas_data_handler import return_insert_into_sql_statement_from_df
from lookups import Logger,DestinationDatabase,ErrorHandling,ETLStep
from datetime import datetime,timedelta
from logging_handler import show_error_message,show_logger_message
import pytz


def get_latest_datetime_from_stock_price_table(db_session,dst_schema):


    latest_date = None
    query = f"""
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = '{dst_schema}'
        AND table_name = 'dim_faang_stock_price'
        );
        """
    is_exists = return_query(db_session = db_session,query =  query)[0][0]

    if is_exists:
        latest_date_query = f"""
        SELECT
            CAST(MAX(formatted_date) AS TIMESTAMP) AS latest_datetime
        FROM {dst_schema}.dim_faang_stock_price
        """
        latest_date = return_query(db_session = db_session,query = latest_date_query)[0][0]
    return latest_date

def convert_local_to_utc(local_datetime):

    local_tz = pytz.timezone('Asia/Beirut')
    utc_datetime = local_tz.localize(local_datetime).astimezone(pytz.UTC)
    utc_date = utc_datetime.date()
    return utc_date


def get_stock_market_prices(db_session,etl_datetime, dst_schema = DestinationDatabase.SCHEMA_NAME.value):

    etl_step = ETLStep.HOOK.value
    logger_string_postfix = Logger.EXTRACT_DATA_FROM_YAHOO_FINANCE.value
    show_logger_message(etl_step, logger_string_postfix)

    latest_datetime = get_latest_datetime_from_stock_price_table(db_session,dst_schema)
    try:
        if not latest_datetime:
            latest_datetime = convert_local_to_utc(etl_datetime)

        if latest_datetime.weekday() == 5:
            latest_datetime += timedelta(days=1)
        
        latest_date_str = latest_datetime.strftime('%Y-%m-%d')

        end_datetime = convert_local_to_utc(datetime.today())
        end_date_str = end_datetime.strftime('%Y-%m-%d')

        tickers = ['META','AMZN','AAPL','NFLX','GOOGL']
        data = {}
        for ticker in tickers:
            yahoo_financials = YahooFinancials(ticker)
            historical_data = yahoo_financials.get_historical_price_data(latest_date_str, end_date_str, "daily")
            if historical_data[ticker]:
                data[ticker] = historical_data[ticker]['prices']
        for ticker, prices in data.items():
            df = pd.DataFrame(prices)
            parse_date_columns(df,df.iloc[0])
            df = df.drop('date', axis=1).set_index('formatted_date')
            df['volume'] = df['volume'].astype(float)
            df.dropna(inplace=True)
            source = 'yahoo_finance'
            df_name = ticker + '_stock_price'
            dst_table = f"stg_{source}_{df_name}"            
            if len(df):
                insert_stmt = return_insert_into_sql_statement_from_df( df, dst_schema, dst_table)
                execute_query(db_session=db_session, query= insert_stmt)

    except Exception as e:
        error_string_prefix = ErrorHandling.GET_YAHOO_FINANCE_STOCK_PRICE_FAILED
        error_string_postfix = str(e)
        show_error_message(error_string_prefix,error_string_postfix)
        raise Exception(error_string_prefix)

