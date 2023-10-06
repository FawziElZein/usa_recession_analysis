import os
from bs4 import BeautifulSoup
import pandas as pd
from urllib.request import urlopen, Request
from urllib.parse import urlparse
from lookups import FinvizWebScrape
from datetime import datetime


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


def scrape_website(etl_date, enum_website=FinvizWebScrape):
    news_list = []
    news_id = 0
    news_tables = get_websites(
        enum_url=FinvizWebScrape.FINVIZ_URL, enum_tickers=FinvizWebScrape.TICKERS)
    current_time = datetime.now()

    for tick, news_table in news_tables.items():
        all_tr = news_table.findAll('tr')
        i = 0
        while i<len(all_tr) and current_time > etl_date:
            tr = all_tr[i]
            i += 1

            text = tr.a.get_text()
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
                        # print("title : ",tr.a.get_text())

                        caas_body_div = html.find('div', class_='caas-body')

                        if caas_body_div:
                            for p in caas_body_div.findAll('p'):
                                text += ' ' + p.text
                    else:
                        print(f"Webscrapping failed: Unable to web scrape {url}",
                              response.status_code)
            except Exception as e:
                print(f"An error occurred: {str(e)}")
            finally:
                if current_time > etl_date:
                    news_list.append([news_id, tick, date, time, text])
                    news_id += 1

    return FinvizWebScrape.SOURCE.value, FinvizWebScrape.TABLE_TITLE.value, pd.DataFrame(news_list, columns=FinvizWebScrape.COLUMNS_NAME.value)
