from lookups import Logger
import logging

logger = logging.getLogger()
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
previous_etl_step = None
counter = 0

def show_error_message(error_string_prefix,error_string_suffix ):
    error_message = error_string_prefix + " : " + error_string_suffix
    print(error_message)

def show_logger_message(etl_step,logger_string_postfix):

    global previous_etl_step
    global counter

    if etl_step != previous_etl_step:

        previous_etl_step = etl_step
        
        logger_string_prefix = ''
        logger_string_postfix = f'{logger_string_postfix} {etl_step}'
        counter = 0
    else:
        counter +=1
        logger_string_prefix = f"Step {counter}: "
        
    logging.basicConfig(filename = "app.log",level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.info(f'{logger_string_prefix}{logger_string_postfix}')

