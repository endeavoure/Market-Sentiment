import requests
import pandas as pd
import feedparser
from variables import api_key
from parse_functions import APIparse, RSSparse

# Cоздаём заготовку в форме датафрейма для записи данных и перформим первоначальную запись данных в json-строку (около 1000 записей)
columns = ['title', 'description', 'publishedAt']
df = pd.DataFrame(columns=columns)

# Создаём список источников для RSSparse
URL_list = ["https://lenta.ru/rss/news/economics/", "https://www.kommersant.ru/RSS/corp.xml",
            "https://www.vedomosti.ru/rss/news", "https://vz.ru/rss.xml"]

# Парсим данные по NewsApi
df = APIparse(df, num_pages=4)

# Парсим данные по RSS
for i in range(len(URL_list)):
    df = RSSparse(df, URL_list[i])

# Записываем данные в JSON-строку
df.to_json('financial_news.json', orient='records', lines=True, force_ascii=False)