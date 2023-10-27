def show_error_message(error_string_prefix,error_string_suffix ):
    error_message = error_string_prefix + " : " + error_string_suffix
    print(error_message)


def show_logger_message(logger_string_prefix,logger_string_postfix):

    log_level = Logger.LOG_LEVEL.value
    logging.info(log_level)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.info(logger_string_prefix +" : "+ logger_string_postfix)
