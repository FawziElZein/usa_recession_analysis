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
    WEBSCRAPE_PAGE_FAILED = "Webscrapping failed"
    WEBSCRAPE_PAGE_NOT_FOUND = "Unable to webscrape a page"
    WEBSCRAPE_UNEXPECTED_ERROR = "An unexpected error occured while web scrapping page"
    
class InputTypes(Enum):
    SQL = "SQL"
    CSV = "CSV"
    EXCEL = "Excel"
    HTTP = "Http"
    

class PoliticianSpeeches(Enum):
    CSV_RESOURCE = [0,'https://millercenter.org/the-presidency/presidential-speeches']
    SOURCE = "miller_center"
    TABLE_TITLE = "presidential_speeches"

class FinvizWebScrape(Enum):
    URL = "https://finviz.com/quote.ashx?t="
    SOURCE = "finviz"
    TABLE_TITLE = "financial_news"
    COLUMNS_NAME = ['ticker', 'date', 'time','title', 'text','url']
    TICKERS = ['META','AMZN','AAPL','NFLX','GOOGL']

class FredEconomicDataWebScrape(Enum):
    URL = "https://fred.stlouisfed.org/series/"
    SOURCE = "fred_economic_data"
    KPI = ['GPDI','NETEXP','GCEC1','IMPGS']
    # KPIS_PER_STATE = ['NGSP','UR','MEHOINUSXXA672N','PCE']
    # STATE_INITIALS = [
    # 'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    # 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    # 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    # 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    # 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    # ]
    KPIS_PER_STATE = ['PCE']
    STATE_INITIALS = ['AZ','CT','DE','ID','RI']


class DestinationDatabase(Enum):
    SCHEMA_NAME = "dw_reporting"


class DateField(Enum):
    FINVIZ_FINANCIAL_NEWS = "financial_news_date" # NOT NEEDED NOW

class ETLStep(Enum):
    PRE_HOOK = "prehook"
    HOOK = "hook"
