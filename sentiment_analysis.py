from lookups import FinvizWebScrape, DestinationDatabase, InputTypes
from pandas_data_handler import return_data_as_df,return_insert_into_sql_statement_from_df,return_create_statement_from_df
from database_handler import execute_query
import os
from bs4 import BeautifulSoup
import re
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from datetime import datetime
import pandas as pd
from misc_handler import create_sql_staging_table_index

def get_data_from_staging_table(db_session, source_name=FinvizWebScrape.SOURCE, table_title=FinvizWebScrape.TABLE_TITLE, destination_schema_name=DestinationDatabase.SCHEMA_NAME):

    src_schema = destination_schema_name.value
    src_table = f"stg_{source_name.value}_{table_title.value}"

    select_query = f"""
    SELECT
        CONCAT(ticker,'-',title) AS ticker_title,
        ticker,
        title,
        date,
        time,
        text,
        url
    FROM {src_schema}.{src_table}
            """
    df_financial_news = return_data_as_df(file_executor=select_query, input_type=InputTypes.SQL, db_session=db_session)
    df_financial_news.set_index(df_financial_news.columns[0],inplace=True)
    return df_financial_news




def preprocess_text(text):
    # Preprocessing function
    stop_words = set(stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()

    if isinstance(text, str):
        # remove punctuation and special characters
        # r'[^\w\s]' : matches any character that is not a word character (alphanumeric or underscore) or a whitespace character
        text = re.sub(r'[^\w\s]', '', text)
        # convert to lowercase
        text = text.lower()
        # tokenize text
        tokens = nltk.word_tokenize(text)
        # remove stop words
        tokens = [token for token in tokens if token not in stop_words]
        # lemmatize text
        tokens = [lemmatizer.lemmatize(token) for token in tokens]
        # join tokens back into text
        text = ' '.join(tokens)
    return text


def analyze_sentiment(df):

    vader = SentimentIntensityAnalyzer()

    scores = df['text'].apply(preprocess_text).apply(
        vader.polarity_scores).tolist()

    scores_df = pd.DataFrame(scores)

    df = df.join(scores_df)
    return df


def create_and_store_into_fact_table(db_session,df, destination_schema=DestinationDatabase.SCHEMA_NAME, source_name=FinvizWebScrape.SOURCE, table_title=FinvizWebScrape.TABLE_TITLE):

    
    target_schema = destination_schema.value
    dst_table = f"fact_{table_title.value}"

    create_stmt = return_create_statement_from_df(df_financial_news_sentiment,target_schema,dst_table)
    execute_query(db_session=db_session,query=create_stmt)
    
    create_sql_staging_table_index(db_session,target_schema,dst_table,df.index.name)

    upsert_query = return_insert_into_sql_statement_from_df(df,target_schema,dst_table,is_upsert=True)
    execute_query(db_session,upsert_query)


def store_sentiment_analysis_into_fact_table(db_session):

    df_financial_news = get_data_from_staging_table(db_session)
    if len(df_financial_news):
        df_financial_news_sentiment = analyze_sentiment(df_financial_news)
        create_and_store_into_fact_table(db_session,df_financial_news_sentiment)


