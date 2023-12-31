import os
from bs4 import BeautifulSoup
from tempfile import mkdtemp
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import pandas as pd
import time
import requests
from urllib.request import urlopen, Request
from urllib.parse import urlparse
from urllib.error import HTTPError
from lookups import Logger,ETLStep,FinvizWebScrape, ErrorHandling,DestinationDatabase,FredEconomicDataWebScrape,PoliticianSpeeches,CHROME_EXECUTOR
from datetime import datetime,timedelta
from logging_handler import show_error_message,show_logger_message
from pandas_data_handler import return_create_statement_from_df,return_insert_into_sql_statement_from_df,download_webscrape_csv_to_dataframe
from database_handler import execute_query,return_query
from misc_handler import create_sql_table_index

def get_html_format(enum_url, enum_tickers):
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
                error_string_prefix = ErrorHandling.WEBSCRAPE_PAGE_FAILED.value
                error_string_suffix = f"Unable to web scrape {url} "+ response.status_code
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
        return news_tables



def get_last_news_date(db_session, enum_tickers):

    tickers = enum_tickers.value

    latest_date = datetime.now()
    for ticker in tickers:

        latest_news_date_query = f"""
            SELECT
                MAX(CAST(CONCAT(CAST(CAST(date AS DATE) AS TEXT),' ',CAST(time AS TEXT)) AS TIMESTAMP))
            FROM dw_reporting.fact_financial_news
            WHERE ticker = '{ticker}'
        """
        latest_news_ticker_date = return_query(db_session = db_session,query =  latest_news_date_query)[0][0]
        if latest_news_ticker_date<latest_date:
            latest_date = latest_news_ticker_date
    return latest_date


def get_finviz_news_webscrapping_data(db_session,etl_date,does_etl_exists,enum_website= FinvizWebScrape):

    news_list = []
    news_tables = get_html_format(
        enum_url=enum_website.URL, enum_tickers=enum_website.TICKERS)

    
    if does_etl_exists:
        etl_date = get_last_news_date(db_session,enum_tickers=enum_website.TICKERS)


    for tick, news_table in news_tables.items():
        all_tr = news_table.findAll('tr')
        i = 0
        current_time = datetime.now()

        while i<len(all_tr) and etl_date<current_time:
            tr = all_tr[i]
            i += 1
            
            if tr.a != None:
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
                url = tr.a['href']
                
                try:
                    parsed_url = urlparse(url)
                    if parsed_url.netloc == 'finance.yahoo.com':
                        req = Request(url=url,headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4924.54 Safari/537.36"})
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
                        news_list.append([tick, date, time, title, text, url])

    df = pd.DataFrame(news_list, columns=FinvizWebScrape.COLUMNS_NAME.value)
    return df


def get_stock_market_news(db_session,etl_date,does_etl_exists,enum_website=FinvizWebScrape):

    etl_step = ETLStep.HOOK.value
    logger_string_postfix = f"{Logger.EXTRACT_DATA_FROM_WEBSITE.value} {FinvizWebScrape.SOURCE.value}"
    show_logger_message(etl_step, logger_string_postfix)

    try:
        df = get_finviz_news_webscrapping_data(db_session,etl_date,does_etl_exists)
        
        if len(df):
            source = enum_website.SOURCE.value
            table_title = enum_website.TABLE_TITLE.value
            dst_table = f"stg_{source}_{table_title}"
            insert_stmt = return_insert_into_sql_statement_from_df(df, DestinationDatabase.SCHEMA_NAME.value, dst_table)
            execute_query(db_session=db_session, query= insert_stmt)
        

    except Exception as e:
        error_string_prefix = ErrorHandling.WEBSCRAPE_DATA_FROM_FINVIZ.value
        error_string_suffix = str(e)
        show_error_message(error_string_prefix,error_string_suffix)
        raise Exception(error_string_prefix)

def get_usa_economic_data(db_session,etl_datetime,does_etl_exists,options,chrome_exec_path = CHROME_EXECUTOR.PATH.value):

    etl_step = ETLStep.HOOK.value
    logger_string_postfix = f"{Logger.EXTRACT_US_DATA_FROM_WEBSITE.value} {FredEconomicDataWebScrape.SOURCE.value} website"
    show_logger_message(etl_step, logger_string_postfix)


    main_url = FredEconomicDataWebScrape.URL.value
    schema_name = DestinationDatabase.SCHEMA_NAME.value
    max_wait_time = 20
    kpis = FredEconomicDataWebScrape.KPI.value

    try:
        if does_etl_exists:
            etl_datetime += timedelta(days=1)

        chrome = webdriver.Chrome(options=options, service=webdriver.ChromeService("/opt/chromedriver"))

        for kpi in kpis:
            try:
                url = main_url + kpi
                chrome.get(url)
                response = requests.get(url)
                if response.status_code == 200:

                    end_date_input_value  = ''
                    while end_date_input_value == '':
                        end_date_input = WebDriverWait(chrome, max_wait_time).until(
                        EC.presence_of_element_located((By.ID, "input-coed"))
                        )
                        end_date_input_value = end_date_input.get_attribute('value')
                    
                    

                    if etl_datetime < datetime.strptime(end_date_input_value, '%Y-%m-%d'):

                        input_field = chrome.find_element(By.ID, "input-cosd")
                        time.sleep(1)
                        input_field.clear()
                        time.sleep(1)

                        etl_date_str = etl_datetime.strftime('%Y-%m-%d')
                        input_field.send_keys(etl_date_str)
                        time.sleep(1)
                        download_button = chrome.find_element(By.ID, "download-button")
                        download_button.click()

                        time.sleep(1)


                        download_menu = chrome.find_element(By.ID, "fg-download-menu")

                        li_elements = download_menu.find_elements(By.TAG_NAME, "li")


                        for li_element in li_elements:
                                anchor_tag = li_element.find_element(By.TAG_NAME, "a")
                                if anchor_tag:
                                    if "CSV" in anchor_tag.text:
                                        href_link = anchor_tag.get_attribute("href")
                                        df = download_webscrape_csv_to_dataframe(href_link)
                                        time.sleep(1)
                                        df.set_index(df.columns[0],inplace=True)
                                        table_name = f"stg_{FredEconomicDataWebScrape.SOURCE.value}_{kpi.lower()}"
                                        insert_stmt = return_insert_into_sql_statement_from_df(df,schema_name = schema_name,table_name = table_name)
                                        execute_query(db_session,insert_stmt)
                                        time.sleep(1)
                else:
                    error_string_prefix = ErrorHandling.WEBSCRAPE_PAGE_FAILED.value
                    error_string_suffix = f"Unable to web scrape {inner_url}, HTTP status code: " +response.getcode()
                    show_error_message(error_string_prefix,error_string_suffix)
            
            except Exception as e:
                if isinstance(e,HTTPError) and e.code ==404:
                    error_string_prefix = ErrorHandling.WEBSCRAPE_PAGE_NOT_FOUND.value
                    error_string_suffix = str(e) +" "+ url
                else:
                    error_string_prefix = ErrorHandling.WEBSCRAPE_UNEXPECTED_ERROR.value
                    error_string_suffix = str(e)
                show_error_message(error_string_prefix,error_string_suffix)


    except Exception as e:
        error_string_prefix = ErrorHandling.WEBSCRAPE_USA_DATA_ERROR.value
        error_string_suffix = str(e)
        show_error_message(error_string_prefix,error_string_suffix)
    finally:
        chrome.quit()

    

def get_states_economic_data(db_session,etl_datetime,does_etl_exists,options,chrome_exec_path = CHROME_EXECUTOR.PATH.value):



    main_url = FredEconomicDataWebScrape.URL.value
    df_list = []

    source = FredEconomicDataWebScrape.SOURCE.value
    schema_name = DestinationDatabase.SCHEMA_NAME.value
    kpis = FredEconomicDataWebScrape.KPIS_PER_STATE.value
    states = FredEconomicDataWebScrape.STATE_INITIALS.value

    etl_step = ETLStep.HOOK.value
    logger_string_postfix = f"{Logger.EXTRACT_US_STATES_DATA_FROM_WEBSITE.value} {source} website"
    show_logger_message(etl_step, logger_string_postfix)

    try:

        if does_etl_exists:
            etl_datetime += timedelta(days=1)

        

        max_wait_time = 20

        for kpi in kpis:
            for state in states:
                try:
                    chrome = webdriver.Chrome(options=options, service=webdriver.ChromeService("/opt/chromedriver"))

                    if kpi== 'MEHOINUSXXA672N':
                        state_kpi = kpi.replace('XX',state)
                    else:
                        state_kpi = state + kpi
                    
                    url = main_url + state_kpi
                    chrome.get(url)
                    response = requests.get(url)
                    if response.status_code == 200:
                        end_date_input_value = ''
                        while end_date_input_value == '':
                            end_date_input = WebDriverWait(chrome, max_wait_time).until(
                            EC.presence_of_element_located((By.ID, "input-coed"))
                            )
                            end_date_input_value = end_date_input.get_attribute('value')
                        
                        if etl_datetime < datetime.strptime(end_date_input_value, '%Y-%m-%d'):

                            input_field = chrome.find_element(By.ID, "input-cosd")
                            time.sleep(1)
                            start_date_input_value = input_field.get_attribute('value')

                            if etl_datetime > datetime.strptime(start_date_input_value, '%Y-%m-%d'):
                                input_field.clear()
                                time.sleep(1)

                                etl_date_str = etl_datetime.strftime('%Y-%m-%d')
                                input_field.send_keys(etl_date_str)
                                time.sleep(1)
                            
                            download_button = chrome.find_element(By.ID, "download-button")
                            download_button.click()

                            time.sleep(1)

                            download_menu = chrome.find_element(By.ID, "fg-download-menu")

                            li_elements = download_menu.find_elements(By.TAG_NAME, "li")


                            for li_element in li_elements:
                                anchor_tag = li_element.find_element(By.TAG_NAME, "a")
                                if anchor_tag:
                                    if "CSV" in anchor_tag.text:
                                        href_link = anchor_tag.get_attribute("href")
                                        df = download_webscrape_csv_to_dataframe(href_link)
                                        time.sleep(1)
                                        
                                        if kpi== 'MEHOINUSXXA672N':
                                            kpi_value = 'mehoin'
                                        else:
                                            kpi_value = kpi.lower()
                                        df_ = df.copy()
                                        df_['state'] = state
                                        df_['state_date'] = df_['state'] + '_' + df_['DATE'].astype(str)
                                        df_.columns.values[1] = kpi_value
                                        table_name = f"stg_{source}_states_{kpi_value}"
                                        df_.set_index('state_date',inplace=True)
                                        insert_stmt = return_insert_into_sql_statement_from_df(df_,schema_name,table_name,is_upsert=True)
                                        execute_query(db_session,insert_stmt)
                    else:
                        error_string_prefix = ErrorHandling.WEBSCRAPE_PAGE_FAILED.value
                        error_string_suffix = f"Unable to web scrape {inner_url}, HTTP status code: " +response.getcode()
                        show_error_message(error_string_prefix,error_string_suffix)
                    chrome.quit()
                except Exception as e:
                    if isinstance(e,HTTPError) and e.code ==404:
                        error_string_prefix = ErrorHandling.WEBSCRAPE_PAGE_NOT_FOUND.value
                        error_string_suffix = str(e) +" "+ url
                    else:
                        error_string_prefix = ErrorHandling.WEBSCRAPE_UNEXPECTED_ERROR.value
                        error_string_suffix = str(e)
                    show_error_message(error_string_prefix,error_string_suffix)
    


    except Exception as e:
        error_string_prefix = ErrorHandling.WEBSCRAPE_USA_STATES_DATA_ERROR.value
        error_string_suffix = str(e)
        show_error_message(error_string_prefix,error_string_suffix)
    finally:
        chrome.quit()

  
def get_politician_speeches(db_session,etl_datetime,options,chrome_exec_path = CHROME_EXECUTOR.PATH.value):

    schema_name = DestinationDatabase.SCHEMA_NAME.value
    source_title = PoliticianSpeeches.SOURCE.value
    table_title = PoliticianSpeeches.TABLE_TITLE.value
    table_name = f"stg_{source_title}_{table_title}"

    etl_step = ETLStep.HOOK.value
    logger_string_postfix = f"{Logger.EXTRACT_DATA_FROM_WEBSITE.value} {source_title} website"
    show_logger_message(etl_step, logger_string_postfix)

    try:
        
        chrome = webdriver.Chrome(options=options, service=webdriver.ChromeService("/opt/chromedriver"))

        news_tables = {}

        url = PoliticianSpeeches.URL.value
        chrome.get(url)
        response = requests.get(url)

        number_of_scrolls = 4

        for _ in range(number_of_scrolls):
            chrome.find_element("tag name", 'body').send_keys(Keys.END)
            time.sleep(4)


        max_wait_time = 10
        last_view_position = -1
        is_recent_data_available = True

        while is_recent_data_available:

            speeches_main_page = WebDriverWait(chrome, max_wait_time).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "landing-page-body"))
                    )

            soup = BeautifulSoup(speeches_main_page.get_attribute('outerHTML'), 'html.parser')

            views_rows = soup.find_all('div', class_='views-row')
            
            inner_chrome = webdriver.Chrome(options=options, service=webdriver.ChromeService("/opt/chromedriver"))

            for row in views_rows:
                
                try:
                    inner_url = row.find('a')['href']
                    a_tag_title = row.find('a')
                    speech_title = a_tag_title.get_text(strip=True)
                    speech_date = speech_title.split(':')[0]
                    df = pd.DataFrame()
                    speech_datetime = pd.to_datetime(speech_date)
                    df['date'] = [speech_datetime]
                    df['speech_title'] = [speech_title.split(':')[1]]

                    if speech_datetime >= etl_datetime:
                        
                        inner_chrome.get(inner_url)
                        response = requests.get(inner_url)
                        
                        if response.status_code == 200:
                            div_speaker_name = WebDriverWait(inner_chrome, max_wait_time).until(
                                EC.element_to_be_clickable((By.ID, "more-media"))
                            )

                            div_speaker_name_html = div_speaker_name.get_attribute('outerHTML')
                            div_speaker_name_html_soup  = BeautifulSoup(div_speaker_name_html, 'html.parser')
                            h3_element = div_speaker_name_html_soup.find('h3')

                            df['speaker_name'] = [h3_element.text.strip()[5:-9]]
                            insert_stmt = return_insert_into_sql_statement_from_df(df,schema_name,table_name,is_upsert=True)
                            execute_query(db_session,insert_stmt)
                        else:
                            error_string_prefix = ErrorHandling.WEBSCRAPE_PAGE_FAILED.value
                            error_string_suffix = f"Unable to web scrape {inner_url}, HTTP status code: " +response.getcode()
                            show_error_message(error_string_prefix,error_string_suffix)
                    else:
                        inner_chrome.quit()
                        is_recent_data_available = False
                        break
                except Exception as e:
                    if isinstance(e,HTTPError) and e.code ==404:
                        error_string_prefix = ErrorHandling.WEBSCRAPE_PAGE_NOT_FOUND.value
                        error_string_suffix = str(e) +" "+ url
                    else:
                        error_string_prefix = ErrorHandling.WEBSCRAPE_UNEXPECTED_ERROR.value
                        error_string_suffix = str(e)
                    show_error_message(error_string_prefix,error_string_suffix)
                    inner_chrome.quit()
            

    except Exception as e:
        error_string_prefix = ErrorHandling.WEBSCRAPE_POLITICIANS_SPEECHES_DATA_ERROR.value
        error_string_suffix = str(e)
        show_error_message(error_string_prefix,error_string_suffix)
    finally:
        chrome.quit()
