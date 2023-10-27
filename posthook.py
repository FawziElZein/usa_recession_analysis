from database_handler import execute_query, create_connection,return_query
import misc_handler
from lookups import DestinationDatabase,ETLStep,Logger
from misc_handler import return_staging_tables_as_list
from logging_handler import show_logger_message

def truncate_staging_tables(schema_name, table_list, db_session):
    for table_name in table_list:
        truncate_query = f"""
        DO $$ 
        DECLARE
            index_to_drop TEXT;
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema_name.value}' AND table_name = '{table_name}') THEN
                TRUNCATE TABLE {schema_name.value}.{table_name};
                RAISE NOTICE 'Table truncated successfully.';
                SELECT 
                    CONCAT('{schema_name.value}.',indexname) 
                INTO index_to_drop
                FROM pg_indexes 
                WHERE tablename = '{table_name}'
                AND SUBSTRING(indexname FROM 1 FOR 4) = 'idx_';
                IF index_to_drop IS NOT NULL THEN
                    EXECUTE 'DROP INDEX ' || index_to_drop;
                ELSE 
                    RAISE NOTICE 'Index not found';
                END IF;
            ELSE
                RAISE NOTICE 'Table does not exist.';
            END IF;
        END $$;
            """
        execute_query(db_session, truncate_query)



def execute_posthook():
    logger_string_prefix = ETLStep.POST_HOOK.value
    logger_string_postfix = Logger.START.value
    show_logger_message(logger_string_prefix,logger_string_postfix)

    db_session = create_connection()
    tables = return_staging_tables_as_list(db_session)
    truncate_staging_tables(DestinationDatabase.SCHEMA_NAME, tables, db_session)

    logger_string_postfix = Logger.SUCCESS_MESSAGE.value
    show_logger_message(logger_string_prefix,logger_string_postfix)
    