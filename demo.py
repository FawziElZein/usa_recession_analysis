from webscrape import get_states_webscraping_data,get_usa_webscrapping_data
from database_handler import create_connection
from datetime import datetime
from prehook import execute_prehook
db_session = create_connection()

execute_prehook()
get_usa_webscrapping_data(db_session = db_session,etl_datetime = datetime(2007,1,1),does_etl_exists = False)
# get_states_webscraping_data(db_session,datetime(2023,10,12),True)

