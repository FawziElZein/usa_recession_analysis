from database_handler import execute_query, create_connection,return_query
import misc_handler
from lookups import DestinationDatabase

def return_staging_tables_as_list(db_session):
    
    get_staging_tables_query = """
    SELECT 
        table_name
    FROM information_schema.tables
    WHERE
        table_type = 'BASE TABLE'
        AND table_schema = 'dw_reporting'
        AND table_name LIKE 'stg%';
    """
    staging_tables = return_query(db_session, get_staging_tables_query)

    table_names = [item[0] for item in staging_tables]
    return table_names

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
    db_session = create_connection()
    tables = return_staging_tables_as_list(db_session)
    truncate_staging_tables(DestinationDatabase.SCHEMA_NAME, tables, db_session)
