from enum import Enum

class ErrorHandling(Enum):
    DB_CONNECT_ERROR = "DB Connect Error"
    DB_RETURN_QUERY_ERROR = "DB Return Query Error"
    API_ERROR = "Error calling API"
    RETURN_DATA_CSV_ERROR = "Error returning CSV"
    RETURN_DATA_EXCEL_ERROR = "Error returning Excel"
    RETURN_DATA_SQL_ERROR = "Error returning SQL"
    RETURN_DATA_UNDEFINED_ERROR = "Cannot find File type"
    EXECUTE_QUERY_ERROR = "Error executing the query"
    CREATE_TABLE_ERROR = "Error creating new table"
    INSERT_INTO_TABLE_ERROR = "Error inserting into table"
    STAGING_DATA_ERROR = "Error staging recent data"
    CREATE_INSERT_STAGING_TABLES_ERROR = "Error creating/inserting into staging tables"
    ETL_CHECKPOINT_ERROR = "Error creating ETL checkpoint"
    ETL_INSERT_CHECKPOINT_ERROR = "Error inserting ETL checkpoint"
    ETL_UPDATE_CHECKPOINT_ERROR = "Error updating ETL checkpoint"
    FUNCTION_NA = "Function not available"
    NO_ERROR = "No Errors"
    PREHOOK_SQL_ERROR = "Prehook: SQL Error"
    HOOK_SQL_ERROR = "Hook: SQL Error"
    DATE_CONVERSION_ERROR = "Warning: column is not a date format"
    RETURN_ETL_LAST_UPDATE_ERROR = "Error returning ETL last update"
    GET_YAHOO_FINANCE_STOCK_PRICE_FAILED = "Error fetching stock prices from yahoo finance library"
    WEBSCRAPE_DATA_FROM_FINVIZ = "Unable to webscrape data from Finviz website"
    WEBSCRAPE_PAGE_FAILED = "Webscrapping failed"
    WEBSCRAPE_PAGE_NOT_FOUND = "Unable to webscrape a page"
    WEBSCRAPE_UNEXPECTED_ERROR = "An unexpected error occured while web scrapping page"
    ARIMA_ERROR = "Error in ARIMA implementation"
    PROCESS_PREDICTION_RESULTS_ERROR = "Error processing prediction results"
    GDP_FORECAST_ERROR = "Error forecasting gdp value"
    OPEN_AI_ERROR = "Error requesting sentiment analysis results from OpenAI"
    SENTIMENTS_RESULT_ERROR = "Error processing sentiment results"
    CREATE_AND_STORE_INTO_FACT_AGG_TABLE_ERROR = "Error create and storing into fact and agg tables"

    
class Logger:
    LOG_LEVEL = 'INFO'

class LoggerMessages:
    SQL_FOLDER_EXECUTION = "SQL folder successfully executed"
    STOCK_PRICES_RETRIEVAL = "FAANG stock prices successfully fetched"
    WEBSCRAPE_DATA_FROM_FINVIZ = "FINVIZ stock news successfully fetched"
    WEBSCRAPE_USA_DATA_FROM_FRED_ECONMIC_WEBSITE = "U.S.A economic dataset successfully fetched from Fred Economic Website"
    WEBSCRAPE_USA_STATES_DATA_FROM_FRED_ECONMIC_WEBSITE = "U.S.A state wise econmic dataset successfully fetched from Fred Economic Website"
    WEBSCRAPE_POLITICIANS_SPEECHES = "U.S Politicians speeches successfully fetched"
    SENTIMENTS_ANALYSIS = "successfully processed text into sentiments scores"
    CREATE_AND_STORE_INTO_FACT_AGG_TABLE = "successfully created and addded dataframe into fact/agg tables"

class CHROME_EXECUTOR:
    PATH = "C:\Program Files\Google\Chrome\Application\chrome.exe"
    
class InputTypes(Enum):
    SQL = "SQL"
    CSV = "CSV"
    EXCEL = "Excel"
    HTTP = "Http"
    

class PoliticianSpeeches(Enum):
    URL = 'https://millercenter.org/the-presidency/presidential-speeches'
    SOURCE = "miller_center"
    TABLE_TITLE = "presidential_speeches"
    COLUMNS_NAME = ['date', 'speech_title','speaker_name', 'speech']
    TEXT_COLUMN_NAME = 'speech'


class FinvizWebScrape(Enum):
    URL = "https://finviz.com/quote.ashx?t="
    SOURCE = "finviz"
    TABLE_TITLE = "financial_news"
    COLUMNS_NAME = ['ticker', 'date', 'time','title', 'text','url']
    TICKERS = ['META','AMZN','AAPL','NFLX','GOOGL']
    TEXT_COLUMN_NAME = 'text'

class FredEconomicDataWebScrape(Enum):
    URL = "https://fred.stlouisfed.org/series/"
    SOURCE = "fred_economic_data"
    KPI = ['GDPC1','PCE','GPDI','NETEXP','GCEC1','IMPGS']
    KPIS_PER_STATE = ['NGSP','UR','MEHOINUSXXA672N','PCE']
    STATE_INITIALS = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    ]
    DEPENDENT_VAR = 'gdp'
    DEPENDENT_INDEPENDENT_VARS = ['gdp','pce','gpdi','netexp','gcec','impgs','average_negative','average_neutral','average_positive','average_compound']
    ARIMA_ORDER = (4, 1, 3)
    FUTURE_FORECAST_PERIODS = 20
    FORECAST_TABLE_TITLE = 'forecasted_gdp'

class DestinationDatabase(Enum):
    SCHEMA_NAME = "dw_reporting"


class ETLStep(Enum):
    PRE_HOOK = "prehook"
    HOOK = "hook"

class TABLE_TYPE:
    DIM = "dim"
    FACT = "fact"
    AGG = "agg"
    VIEW = "views"