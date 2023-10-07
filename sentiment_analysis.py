from lookups import FinvizWebScrape,DestinationDatabase,InputTypes
from pandas_data_handler import return_data_as_df
import os
from bs4 import BeautifulSoup
import re
import nltk
from urllib.request import urlopen, Request
from urllib.parse import urlparse
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from datetime import datetime
import pandas as pd


def preprocess_text(text):
    # Preprocessing function
    stop_words = set(stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()

    if isinstance(text, str):
        # remove punctuation and special characters
        text = re.sub(r'[^\w\s]', '', text)  # r'[^\w\s]' : matches any character that is not a word character (alphanumeric or underscore) or a whitespace character
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


def get_data_from_staging_table(db_session, source_name = FinvizWebScrape.SOURCE, table_title = FinvizWebScrape.TABLE_TITLE,destination_schema_name = DestinationDatabase.SCHEMA_NAME):

    dst_table = f"stg_{source_name.value}_{table_title.value}"
    src_schema = destination_schema_name.value

    select_query = f"""
    SELECT
        ticker,
        date,
        time,
        text
    FROM {src_schema}.{dst_table}
            """
    df_financial_news = return_data_as_df(file_executor=select_query,input_type=InputTypes.SQL,db_session= db_session)
    return df_financial_news



def analyze_sentiment(db_session):
    
    df_financial_news = get_data_from_staging_table(db_session)

    vader = SentimentIntensityAnalyzer()

    scores = df_financial_news['text'].apply(preprocess_text).apply(vader.polarity_scores).tolist()

    scores_df = pd.DataFrame(scores)

    df_financial_news = df_financial_news.join(scores_df)

    print(df_financial_news)



# Create table named fact_financial_news_sentiment

# insert into that table the resulting data from df_financial_news
