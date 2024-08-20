import requests
import pandas as pd
import feedparser
from variables import api_key


def APIparse(df, num_pages=1):
    """
    Парсит новости используя NewsAPI

    :param DataFrame df: Датафрейм, куда запишутся данные
    :param int num_pages: Количество страниц, подвергнутых парсингу

    :return: DataFrame df с записанными новостями

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

    :return: DataFrame df с записанными новостями

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