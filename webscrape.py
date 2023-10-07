import os
from bs4 import BeautifulSoup
import pandas as pd
from urllib.request import urlopen, Request
from urllib.parse import urlparse
from urllib.error import HTTPError
from lookups import FinvizWebScrape, ErrorHandling,DestinationDatabase
from datetime import datetime
from logging_handler import show_error_message
from pandas_data_handler import return_insert_into_sql_statement_from_df
from database_handler import execute_query
def get_websites(enum_url, enum_tickers):
    web_url = enum_url.value
    tickers = enum_tickers.value

    news_tables = {}
    try:
        for tick in tickers:
            url = web_url + tick
            req = Request(url=url, headers={"User-Agent": "Chrome"})
            response = urlopen(req)
            if response.getcode() == 200:
                html = BeautifulSoup(response, "html.parser")
                news_table = html.find(id='news-table')
                news_tables[tick] = news_table
            else:
                print(
                    f"Webscrapping failed: Unable to web scrape {url}", response.status_code)
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    return news_tables


def scrape_website_store_into_staging_table(db_session,etl_date, enum_website=FinvizWebScrape):
    news_list = []
    news_id = 0
    news_tables = get_websites(
        enum_url=FinvizWebScrape.FINVIZ_URL, enum_tickers=FinvizWebScrape.TICKERS)
    current_time = datetime.now()

    for tick, news_table in news_tables.items():
        all_tr = news_table.findAll('tr')
        i = 0
        while i<len(all_tr) and etl_date<current_time:
            tr = all_tr[i]
            i += 1

            title = text = tr.a.get_text()
            date_scrape = tr.td.text.split()

            if len(date_scrape) == 1:
                time = date_scrape[0]

            else:
                date = date_scrape[0]
                if date == 'Today':
                    date = datetime.now().date()
                else:
                    parsed_date = datetime.strptime(date, "%b-%d-%y")
                    date = parsed_date.strftime("%Y-%m-%d")
                time = date_scrape[1]

                current_time_str = f"{date} {time}"
                current_time = datetime.strptime(
                    current_time_str, "%Y-%m-%d %I:%M%p")
            try:
                url = tr.a['href']
                parsed_url = urlparse(url)
                if parsed_url.netloc == 'finance.yahoo.com':
                    req = Request(url=url, headers={"User-Agent": "Chrome"})
                    response = urlopen(req)
                    if response.getcode() == 200:
                        html = BeautifulSoup(response, "html.parser")

                        caas_body_div = html.find('div', class_='caas-body')

                        if caas_body_div:
                            for p in caas_body_div.findAll('p'):
                                text += ' ' + p.text
                    else:
                        error_string_prefix = ErrorHandling.WEBSCRAPE_PAGE_FAILED.value
                        error_string_suffix = f"Unable to web scrape {url}, HTTP status code: " +response.getcode()
                        show_error_message(error_string_prefix,error_string_suffix)
            except Exception as e:
                if isinstance(e,HTTPError) and e.code ==404:
                    error_string_prefix = ErrorHandling.WEBSCRAPE_PAGE_NOT_FOUND.value
                    error_string_suffix = str(e) +" "+ url
                else:
                    error_string_prefix = ErrorHandling.WEBSCRAPE_UNEXPECTED_ERROR.value
                    error_string_suffix = str(e)
                show_error_message(error_string_prefix,error_string_suffix)
            finally:
                if etl_date < current_time:
                    news_list.append([tick, date, time, title, text])
                    news_id += 1

    staging_df = pd.DataFrame(news_list, columns=FinvizWebScrape.COLUMNS_NAME.value)

    if len(staging_df):
        dst_table = f"stg_{FinvizWebScrape.SOURCE.value}_{FinvizWebScrape.TABLE_TITLE.value}"
        insert_stmt = return_insert_into_sql_statement_from_df(staging_df, DestinationDatabase.SCHEMA_NAME.value, dst_table)
        execute_query(db_session=db_session, query= insert_stmt)
