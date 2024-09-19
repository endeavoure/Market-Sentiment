import requests
import pandas as pd
import feedparser
import time
import selenium
import json
import os

from bs4 import BeautifulSoup
from variables import api_key

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def APIparse(df: pd.DataFrame, num_pages: int) -> pd.DataFrame:
    """
    Парсит новости используя NewsAPI

    :param DataFrame df: Датафрейм, куда запишутся данные
    :param int num_pages: Количество страниц, подвергнутых парсингу

    :return: DataFrame df с записанными новостями (title/description/publishedAt)

    """
    for i in range(4):
        url = ('https://newsapi.org/v2/everything?'
            'q=экономика OR финансы OR деньги OR бизнес OR рынки OR акции OR облигации OR отчётность&'
            'language=ru&'
            'from=2024-07-20&to=2024-08-19&'
            'sortBy=publishedAt&'  
            f'page={i+1}&'
            f'apiKey={api_key}')

        # Получаем данные и записываем в формат датафрейма
        response = requests.get(url)
        news_data = response.json()
        articles = news_data['articles']
        data = []

        for article in articles:
            data.append({
                'title': article['title'],
                'description': article['description'],
                'publishedAt': article['publishedAt']
            })

        # Записываем данные в датафрейм
        temp_df = pd.DataFrame(data)
        df = pd.concat([df, temp_df], ignore_index=True)

        return df
    
def RSSparse(df: pd.DataFrame, link: str) -> pd.DataFrame:
    """
    Парсит новости используя RSS страничку портала

    :param DataFrame df: Датафрейм, куда запишутся данные
    :param str link: URL RSS страницы портала

    :return: DataFrame df с записанными новостями (title/description/publishedAt)

    """
    feed = feedparser.parse(link)
    data = []
    for entry in feed.entries:
        # Блок try/except на случай отсутсвия на страничке записей с summary новостей
        try:
            data.append({
            'title': entry.title,
            'description': entry.summary,
            'publishedAt': entry.published
            })
        except:
            data.append({
            'title': entry.title,
            'description': '',
            'publishedAt': entry.published
            })
    
    # Записываем данные в датафрейм
    temp_df = pd.DataFrame(data)
    df = pd.concat([df, temp_df], ignore_index=True)

    return df

def MKparse(years: tuple, months: tuple, days: tuple) -> dict:
    """
    Парсит новости используя BS4
    :param years tuple: Диапазон годов для парсинга новостей
    :param months tuple: Диапазон месяцев для парсинга новостей
    :param days tuple: Диапазон дней для парсинга новостей

    :return: dict с записанными новостями (title/description/publishedAt)

    """
    annual = []
    month = []
    date = []
    data = {'title': [], 'description': [], 'publishedAt': []}

    for i in range(years[0], years[1]):
        annual.append(i)
    for i in range(months[0], months[1]):
        month.append(i)
    for i in range(days[0], days[1]):
        date.append(i)

    for i in annual:
        for j in month:
            for k in date:
                print(f'Date:{i}/{j}/{k}')
                url = f"https://www.mk.ru/economics/{i}/{j}/{k}/"
                response = requests.get(url)

                if response.status_code == 200:
                    html_content = response.text
                    soup = BeautifulSoup(html_content, 'html.parser')
                    news_title = soup.find('ul', class_='article-listing__grid-list').find_all('h3', class_='article-preview__title')
                    news_description = soup.find_all('p', class_='article-preview__desc')
                    news_date = f'{k}/{j}/{i}'

                    for tit in news_title:
                        title = tit.get_text()
                        data['title'].append(title)
                        print(title)
                        data['publishedAt'].append(news_date)
                        time.sleep(0.10)

                    for desc in news_description:
                        if desc is None:
                            data['description'].append(0)
                            print(f'No description at data {news_date}')
                        else:
                            descr = desc.get_text()
                            data['description'].append(descr)
                            time.sleep(0.10)

                    time.sleep(0.10)

                else:
                    print(f'Ошибка при загрузке страницы: {response.status_code}')
                    time.sleep(0.10)
    return data

def RecommendationsBCS() -> pd.DataFrame:
    """
    Парсит финансовые рекомендации с bcs при помощи BS4 и Selenium
    :return: pd.DataFrame, включающий в себя titles, hrefs, decriptions и dates 

    """
    url = 'https://bcs-express.ru/category/torgovye-rekomendacii'
    driver = webdriver.Chrome()
    driver.get(url)

    data = {'title': [], 'href': []}
    scroll_pause_time = 2
    scroll_height = driver.execute_script("return document.body.scrollHeight")
    last_height = 0
    button = None

    while True:
        driver.execute_script("window.scrollBy(0, 800)")
        time.sleep(scroll_pause_time)
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        titles = soup.find_all('a', class_='iKzE')

        for headline in titles:
            data['title'].append(headline)
            href = headline.get('href')
            data['href'].append(href)

        new_scroll_height = driver.execute_script("return document.body.scrollHeight")
        
        if new_scroll_height == scroll_height:
            print('Достигнут конец страницы')
            try:
                button = WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-id='button-more']"))
                )
                driver.execute_script("arguments[0].scrollIntoView(true);", button)
                time.sleep(scroll_pause_time)
                driver.execute_script("arguments[0].click();", button)
                print("Кнопка успешно нажата.")
                time.sleep(scroll_pause_time)
            except:
                print('Заебал листать долбоёб')
                break
            
        scroll_height = new_scroll_height

    text_dict = {'title': [], 'href': []}

    for tit in list(set(data['title'])):
        title = tit.get_text()
        text_dict['title'].append(title)
        text_dict['href'].append(tit.get('href'))

    with open('/Users/alexanderknyshov/Desktop/LLM/Data/drafts/result.json', 'w') as fp:
        json.dump(text_dict, fp)

    df = pd.read_json('/Users/alexanderknyshov/Desktop/LLM/Data/drafts/result.json')
    links = df['hrefs']
    data_d = {'description': [], 'publishedAt': []}
    driver = webdriver.Chrome()
    counter = 0

    for link in links:
        counter += 1
        driver.get(link)
        if counter == 1:
            time.sleep(10)
        else:
            time.sleep(0.2)
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')

            dates = soup.find_all('time', class_='vwGl')
            time.sleep(0.1)
            descriptions = soup.find_all('div', class_='YjHz UBOr RkGZ')
            time.sleep(0.1)

            if descriptions == []:
                data_d['description'].append('void')
            if dates == []:
                data_d['publishedAt'].append('void')

            for description in descriptions:
                desc = description.get_text()
                data_d['description'].append(desc)
                
            for date in dates:
                d = date.get_text()
                data_d['publishedAt'].append(d)
            
            print(f'Описаний записано: {counter}')
            
            time.sleep(0.1)

    with open('/Users/alexanderknyshov/Desktop/LLM/Data/drafts/result_descriptions.json', 'w') as fp:
        json.dump(data_d, fp)
    
    df_descriptions = pd.read_json('/Users/alexanderknyshov/Desktop/LLM/Data/drafts/result_descriptions.json')
    df_titles = pd.read_json('/Users/alexanderknyshov/Desktop/LLM/Data/drafts/result.json')
    df_result = pd.concat([df_titles, df_descriptions], axis=1)
    df_result.to_json('/Users/alexanderknyshov/Desktop/LLM/Data/datasets/recommendations.json', orient='records', force_ascii=False)

    os.remove('/Users/alexanderknyshov/Desktop/LLM/Data/drafts/result.json')
    os.remove('/Users/alexanderknyshov/Desktop/LLM/Data/drafts/result_descriptions.json')

    return df_descriptions

def RecommendationsInvest(switch: int) -> pd.DataFrame:
    """
    Парсит финансовые рекомендации с invest при помощи BS4 и Selenium
    :return: pd.DataFrame, включающий в себя titles, decriptions и dates 

    """
    if switch == 1:
        url = 'https://investfuture.ru/stocks/articles'
    else:
        url = 'https://investfuture.ru/stocks/news'
    
    driver = webdriver.Chrome()
    driver.get(url)
    time.sleep(10)

    data = {'title': []}
    scroll_pause_time = 2
    last_height = 0
    button = None

    while True:
        driver.execute_script("window.scrollBy(0, 100)")
        time.sleep(scroll_pause_time)
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        titles = soup.find_all('div', class_='article-list row')

        for headline in titles:
            data['title'].append(headline)
        try:
            button = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "[id='a_next_page']"))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", button)
            time.sleep(scroll_pause_time)
            driver.execute_script("arguments[0].click();", button)
            print("Кнопка успешно нажата.")
            time.sleep(scroll_pause_time)
        except:
            print('Не вижу кнопку, ебашу дальше')
            break
        
    new = list(set(data['title']))
    for i in range(len(new)):
        new[i] = 'https://investfuture.ru/stocks' + new[i].find('a').get('href')

    data = {'title': [],
        'description': [],
        'publishedAt': []
        }

    counter = 0
    for link in new:
        counter += 1
        driver.get(link)
        if counter == 1:
            time.sleep(10)
        else:
            time.sleep(0.1)

        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')

        time.sleep(0.05)
        
        titles = soup.find_all('h1', attrs={'itemprop': 'name'})
        dates = soup.find_all('span', class_='published', attrs={'itemprop': 'datePublished'})
        descriptions = soup.find_all('div', attrs={'itemprop': 'articleBody'})

        for title in titles:
            tit = title.get_text()
            data['title'].append(tit)
            print(f'Title {counter}: {tit}')
        for date in dates:
            d = date.get_text()
            data['publishedAt'].append(d)
            print(f'Date {counter}: {d}')
        for description in descriptions:
            desc = description.get_text()
            data['description'].append(desc)
            print(f'Description {counter}: {desc}')
            
        time.sleep(0.05)

    df = pd.DataFrame(data)
    df.to_json('/Users/alexanderknyshov/Desktop/LLM/Data/datasets/invest_recommendations.json', orient='records', force_ascii=False)

    return df