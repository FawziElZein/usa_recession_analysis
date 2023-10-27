from lookups import Logger
import logging
def show_error_message(error_string_prefix,error_string_suffix ):
    error_message = error_string_prefix + " : " + error_string_suffix
    print(error_message)


def show_logger_message(logger_string_prefix,logger_string_postfix):

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info(logger_string_prefix +" : "+ logger_string_postfix)
