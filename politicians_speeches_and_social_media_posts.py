from lookups import DestinationDatabase,SocialMediaPoliticianPosts
from pandas_data_handler import return_create_statement_from_df,return_insert_into_sql_statement_from_df, download_csv_to_dataframe
from database_handler import execute_query
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import re
import time
import requests
import pandas as pd

def get_politician_social_media_posts(db_session,etl_datetime):

    schema_name = DestinationDatabase.SCHEMA_NAME.value
    source_title = SocialMediaPoliticianPosts.SOURCE.value
    table_title = SocialMediaPoliticianPosts.TABLE_TITLE.value
    table_name = f"stg_{source_title}_{table_title}"

    driver = webdriver.Chrome()  
    news_tables = {}

    url = f"https://millercenter.org/the-presidency/presidential-speeches"  
    driver.get(url)
    response = requests.get(url)

    prev_height = driver.execute_script("return document.body.scrollHeight")
    new_height = None
    max_wait_time = 20
    last_view_position = -1

    while new_height!= prev_height:
        prev_height = new_height
        
        speeches_main_page = WebDriverWait(driver, max_wait_time).until(
                EC.presence_of_element_located((By.CLASS_NAME, "landing-page-body"))
                )

        soup = BeautifulSoup(speeches_main_page.get_attribute('outerHTML'), 'html.parser')

        views_rows = soup.find_all('div', class_='views-row')

        first_view_position = last_view_position + 1
        last_view_position = len(views_rows)

        
        for row in views_rows[first_view_position:last_view_position]:
            try:
                inner_url = row.find('a')['href']
                a_tag_title = row.find('a')
                speech_title = a_tag_title.get_text(strip=True)
                speech_date = speech_title.split(':')[0]
                df = pd.DataFrame()
                df['date'] = [pd.to_datetime(speech_date)]
                df['speech_title'] = [speech_title.split(':')[1]]

                driver.get(inner_url)
                response = requests.get(inner_url)

                WebDriverWait(driver, max_wait_time).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "transcript-btn-inner"))
                )

                transcript_button = WebDriverWait(driver, max_wait_time).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "transcript-btn-inner"))
                )
                time.sleep(10)

                transcript_button.click()

                div_speaker_name = WebDriverWait(driver, max_wait_time).until(
                    EC.element_to_be_clickable((By.ID, "more-media"))
                )

                div_speaker_name_html = div_speaker_name.get_attribute('outerHTML')
                div_speaker_name_html_soup  = BeautifulSoup(div_speaker_name_html, 'html.parser')
                h3_element = div_speaker_name_html_soup.find('h3')

                df['speaker_name'] = [h3_element.text.strip()[5:-9]]

                transcript_text = WebDriverWait(driver, max_wait_time).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "transcript-inner"))
                )
                
                paragraphs = transcript_text.find_elements(By.TAG_NAME, 'p')
                text = ''

                for paragraph in paragraphs:
                    text+=paragraph.text + '\n'
                
                df['speech'] = [text]
                insert_stmt = return_insert_into_sql_statement_from_df(df,schema_name,table_name)
                execute_query(db_session,insert_stmt)
            except Exception as e:
                print("an error occured while webscrapping",e)
                
        driver.find_element("tag name", 'body').send_keys(Keys.END)
        time.sleep(3)
        new_height = driver.execute_script("return document.body.scrollHeight")
        

    driver.quit()




