from faang_stock_market_prices import get_faang_historical_prices
from database_handler import create_connection
from datetime import datetime
from lookups import CHROME_EXECUTOR
from webscrape import get_usa_webscrapping_data

etl_date = datetime(2007,10,23)
db_session = create_connection()
get_usa_webscrapping_data(db_session,etl_datetime=etl_date,does_etl_exists=True,chrome_exec_path = CHROME_EXECUTOR.PATH)