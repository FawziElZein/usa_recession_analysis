import os
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time
import requests
from urllib.request import urlopen, Request
from urllib.parse import urlparse
from urllib.error import HTTPError
from lookups import FinvizWebScrape, ErrorHandling,DestinationDatabase,FredEconomicDataWebScrape
from datetime import datetime,timedelta
from logging_handler import show_error_message
from pandas_data_handler import return_create_statement_from_df,return_insert_into_sql_statement_from_df,download_webscrape_csv_to_dataframe
from database_handler import execute_query,return_query
from misc_handler import create_sql_staging_table_index

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
                print(
                    f"Webscrapping failed: Unable to web scrape {url}", response.status_code)
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
                MAX(CAST(CONCAT(CAST(date AS TEXT),' ',CAST(time AS TEXT)) AS TIMESTAMP))
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
        print("latest_date")
        print(etl_date)

    for tick, news_table in news_tables.items():
        all_tr = news_table.findAll('tr')
        i = 0
        
        current_time = datetime.now()

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
            
            url = tr.a['href']
            
            try:
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
                    news_list.append([tick, date, time, title, text, url])

    df = pd.DataFrame(news_list, columns=FinvizWebScrape.COLUMNS_NAME.value)
    return df

def store_into_staging_table(db_session,staging_df,source,table_title):

    if len(staging_df):
        dst_table = f"stg_{source}_{table_title}"
        insert_stmt = return_insert_into_sql_statement_from_df(staging_df, DestinationDatabase.SCHEMA_NAME.value, dst_table)
        execute_query(db_session=db_session, query= insert_stmt)
    
def get_webscrape_data_from_finviz(db_session,etl_date,does_etl_exists,enum_website=FinvizWebScrape):

    df = get_finviz_news_webscrapping_data(db_session,etl_date,does_etl_exists)
    store_into_staging_table(db_session = db_session,staging_df= df, source = enum_website.SOURCE.value,table_title = enum_website.TABLE_TITLE.value)


def get_gdp_measurements_names(symbol):

    if symbol == 'GDPC1':
        return 'real_gross_domestic_product'
    if symbol == 'PCE':
        return 'personal_consumption_expenditures'
    if symbol == 'GPDI':
        return 'gross_private_domestic_investment'
    if symbol == 'NETEXP':
        return 'net_exports_of_goods_and_services'
    if symbol == 'GCEC1':
        return 'real_government_consumption_expenditures_and_gross_investment'
    if symbol == 'IMPGS':
        return 'imports_of_goods_and_services'

def get_kpi_name(symbol):
    if symbol == 'NGSP':
        return 'gross_domestic_product'
    if symbol == 'UR':
        return 'unemployment_rate'
    if symbol == 'MEHOINUSXXA672N':
        return 'real_median_household_income'
    if symbol == 'PCE':
        return 'personal_consumption_expenditures'
    
def get_usa_webscrapping_data(db_session,etl_datetime,does_etl_exists):

    main_url = FredEconomicDataWebScrape.URL.value
    schema_name = DestinationDatabase.SCHEMA_NAME.value

    if does_etl_exists:
        etl_datetime += timedelta(days=1)
    
    max_wait_time = 20
    kpis = FredEconomicDataWebScrape.KPI.value
    driver = webdriver.Chrome()
    for kpi in kpis:
        
        url = main_url + kpi
        driver.get(url)
        # Send a GET request to the URL
        response = requests.get(url)
        end_date_input_value  = ''
        while end_date_input_value == '':
            end_date_input = WebDriverWait(driver, max_wait_time).until(
            EC.presence_of_element_located((By.ID, "input-coed"))
            )
            # end_date_input = driver.find_element(By.ID, "input-coed")
            end_date_input_value = end_date_input.get_attribute('value')
        
        # end_date_input = driver.find_element(By.ID, "input-coed")
        # end_date_input_value = end_date_input.get_attribute('value')

        if etl_datetime < datetime.strptime(end_date_input_value, '%Y-%m-%d'):

            input_field = driver.find_element(By.ID, "input-cosd")
            time.sleep(1)
            input_field.clear()
            time.sleep(1)

            etl_date_str = etl_datetime.strftime('%Y-%m-%d')
            input_field.send_keys(etl_date_str)
            time.sleep(1)
            # Find the button by its ID and click it
            download_button = driver.find_element(By.ID, "download-button")
            download_button.click()

            # # Wait for a few seconds to ensure that the menu has expanded
            time.sleep(1)


            # # Access the UL tag with id "fg-download-menu"
            download_menu = driver.find_element(By.ID, "fg-download-menu")

            li_elements = download_menu.find_elements(By.TAG_NAME, "li")


            for li_element in li_elements:
                    # Check if the li element contains an anchor tag
                    anchor_tag = li_element.find_element(By.TAG_NAME, "a")
                    if anchor_tag:
                        # Check if the text of the anchor tag contains the word "CSV"
                        if "CSV" in anchor_tag.text:
                            href_link = anchor_tag.get_attribute("href")
                            df = download_webscrape_csv_to_dataframe(href_link)
                            print(df)
                            # df.columns.values[1] = get_kpi_name(kpi)
                            df.set_index(df.columns[0],inplace=True)
                            
                            table_name = f"stg_{FredEconomicDataWebScrape.SOURCE.value}_{kpi}"
                            # create_stmt = return_create_statement_from_df(df,schema_name,table_name)
                            # execute_query(db_session,create_stmt)
                            # create_sql_staging_table_index(db_session,schema_name,table_name,'date')
                            insert_stmt = return_insert_into_sql_statement_from_df(df,schema_name,table_name)
                            execute_query(db_session,insert_stmt)

    driver.quit()

# def store_dfs_into_sql(db_session,df_list):


#     kpis = FredEconomicDataWebScrape.KPIS_PER_STATE.value
#     states = FredEconomicDataWebScrape.STATE_INITIALS.value
#     schema_name = DestinationDatabase.SCHEMA_NAME.value
#     source = FredEconomicDataWebScrape.SOURCE.value
#     progress_counter = 0

#     for df in df_list:
#         print(progress_counter)
#         progress_counter+=1
#         state = df.columns[-1][:2]
#         kpi = df.columns[-1][2:]
#         df_ = df.copy()
#         df_['state'] = state
#         df_['state_date'] = df_['state'] + '_' + df_['DATE'].astype(str)
#         df_.columns.values[1] = kpi.lower()
#         table_name = f"stg_{source}_states_{kpi.lower()}"
#         df_.set_index('state_date',inplace=True)
#         # print(df_)
#         # print(table_name)
#         insert_stmt = return_insert_into_sql_statement_from_df(df_,schema_name,table_name)
#         execute_query(db_session,insert_stmt)

def get_states_webscraping_data(db_session,etl_datetime,does_etl_exists):

    main_url = FredEconomicDataWebScrape.URL.value
    df_list = []

    if does_etl_exists:
        etl_datetime += timedelta(days=1)

    source = FredEconomicDataWebScrape.SOURCE.value
    schema_name = DestinationDatabase.SCHEMA_NAME.value
    kpis = FredEconomicDataWebScrape.KPIS_PER_STATE.value
    states = FredEconomicDataWebScrape.STATE_INITIALS.value
    driver = webdriver.Chrome()
    max_wait_time = 20
    try:
        for kpi in kpis:
            for state in states:
                
                if kpi== 'MEHOINUSXXA672N':
                    state_kpi = kpi.replace('XX',state)
                else:
                    state_kpi = state + kpi
                print(state_kpi)
                url = main_url + state_kpi
                driver.get(url)
                # print(url)
                # Send a GET request to the URL
                response = requests.get(url)
                end_date_input_value = ''
                while end_date_input_value == '':
                    end_date_input = WebDriverWait(driver, max_wait_time).until(
                    EC.presence_of_element_located((By.ID, "input-coed"))
                    )
                    # end_date_input = driver.find_element(By.ID, "input-coed")
                    end_date_input_value = end_date_input.get_attribute('value')
                
                print("end_date_input_value",end_date_input_value)
                if etl_datetime < datetime.strptime(end_date_input_value, '%Y-%m-%d'):

                    input_field = driver.find_element(By.ID, "input-cosd")
                    time.sleep(1)
                    start_date_input_value = input_field.get_attribute('value')

                    if etl_datetime > datetime.strptime(start_date_input_value, '%Y-%m-%d'):
                        input_field.clear()
                        time.sleep(1)

                        etl_date_str = etl_datetime.strftime('%Y-%m-%d')
                        input_field.send_keys(etl_date_str)
                        time.sleep(1)
                    
                    # Find the button by its ID and click it
                    download_button = driver.find_element(By.ID, "download-button")
                    download_button.click()

                    # # Wait for a few seconds to ensure that the menu has expanded
                    time.sleep(1)

                    # # Access the UL tag with id "fg-download-menu"
                    download_menu = driver.find_element(By.ID, "fg-download-menu")

                    li_elements = download_menu.find_elements(By.TAG_NAME, "li")


                    for li_element in li_elements:
                        # Check if the li element contains an anchor tag
                        anchor_tag = li_element.find_element(By.TAG_NAME, "a")
                        if anchor_tag:
                            # Check if the text of the anchor tag contains the word "CSV"
                            if "CSV" in anchor_tag.text:
                                href_link = anchor_tag.get_attribute("href")
                                df = download_webscrape_csv_to_dataframe(href_link)
                                time.sleep(1)
                                # print(df)
                                
                                if kpi== 'MEHOINUSXXA672N':
                                    kpi_value = 'mehoin'
                                    # state = df.columns[-1][:2]
                                else:
                                    kpi_value = kpi.lower()
                                df_ = df.copy()
                                df_['state'] = state
                                df_['state_date'] = df_['state'] + '_' + df_['DATE'].astype(str)
                                df_.columns.values[1] = kpi_value
                                table_name = f"stg_{source}_states_{kpi_value}"
                                df_.set_index('state_date',inplace=True)
                                # print(df_)
                                # print(table_name)
                                insert_stmt = return_insert_into_sql_statement_from_df(df_,schema_name,table_name)
                                execute_query(db_session,insert_stmt)
                                # df_list.append(df)

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Ensure that the driver is closed before proceeding with any other operations
        print(df_list)
        driver.quit()
        # return df_list

    


