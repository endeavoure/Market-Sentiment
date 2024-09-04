import requests
import pandas as pd
import feedparser
import time
from bs4 import BeautifulSoup
from variables import api_key


def APIparse(df, num_pages=1):
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
    
def RSSparse(df, link):
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