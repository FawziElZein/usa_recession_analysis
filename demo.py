from webscrape_data_handler import get_politician_speeches
from database_handler import create_connection
from datetime import datetime
get_politician_speeches(create_connection(),datetime(2022,10,10))