from lookups import Logger
import logging

logger = logging.getLogger()
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

def show_error_message(error_string_prefix,error_string_suffix ):
    error_message = error_string_prefix + " : " + error_string_suffix
    print(error_message)

def show_logger_message(logger_string_prefix,logger_string_postfix):

    logging.basicConfig(filename = "logger_file.log",level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setLevel(logging.INFO)
    
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.info(f'{logger_string_prefix}: {logger_string_postfix}')
