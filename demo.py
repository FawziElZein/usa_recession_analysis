from faang_stock_market_prices import get_faang_historical_prices
from database_handler import create_connection
from datetime import datetime
from lookups import CHROME_EXECUTOR
from webscrape import get_usa_webscrapping_data
import logging

log_level = "INFO"
logging.info(log_level)
logger = logging.getLogger()
logger.setLevel(log_level)
logger.info("Schedule is running")
