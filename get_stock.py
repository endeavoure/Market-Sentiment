import requests
from datetime import datetime, timedelta
import re
import locale
import pandas as pd


class CustomError(Exception):
    pass


# По дате и тикеру выдает значение:
# ("открытие в начале следующего дня после новости" - "открытие в начале дня новости") / "открытие в начале дня новости"
def get_growth(ticker: str, date: str, format: str, language='ru_RU.UTF-8'):
    now = datetime.now()
    one_day = timedelta(days=1)
    locale.setlocale(locale.LC_TIME, language)

    # Обработка форматов, не обрабатывающихся автоматически, и получение дат для запроса на биржу
    try:
        if re.search("Сегодня", date):
            return None
        elif re.search("Вчера", date):
            start = now - one_day
            end = now
        elif re.search("Позавчера", date):
            start = now - timedelta(days=2)
            end = now - one_day
        elif re.search(r"\d{4}", date) is None:
            date = datetime.strptime(f"{date} {str(now.year)}", f"{format} %Y")
            start = date
            end = date + one_day
        else:
            date = datetime.strptime(date, format)
            start = date
            end = date + one_day

    except Exception as e:
        raise CustomError(f"Ошибка в получении дат вокруг новости: {e}")

    # Получение ответа от Мосбиржи
    url = (
        f'http://iss.moex.com/iss/engines/stock/markets/shares/securities/'
        f'{ticker}/candles.json?from={start.date()}&till={end.date()}&interval=24'
    )
    j = requests.get(url).json()

    # Функция для сдвига дней в запросе, чтобы не попадали на выходные
    def fix_holidays(j, start, end):
        if len(j["candles"]["data"]) == 2:
            return j
        elif len(j["candles"]["data"]) == 1:
            while len(j["candles"]["data"]) != 2:
                if end == datetime.now():
                    raise CustomError("Новость свежая, после её выхода не было торгов")
                if start.date() == datetime.strptime(j["candles"]["data"][0][6], "%Y-%m-%d %H:%M:%S").date():
                    end = end + one_day
                elif end.date() == datetime.strptime(j["candles"]["data"][0][6], "%Y-%m-%d %H:%M:%S").date():
                    start = start - one_day
                else:
                    raise CustomError("Ошибка в выявлении выходных")
                j = requests.get(
                    f'http://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json'
                    f'?from={start.date()}&till={end.date()}&interval=24'
                ).json()
            return j
        elif len(j["candles"]["data"]) == 0:
            if end == datetime.now():
                raise CustomError("Новость свежая, после её выхода не было торгов")
            end = end + one_day
            start = start - one_day
            j = requests.get(
                f'http://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json'
                f'?from={start.date()}&till={end.date()}&interval=24'
            ).json()
            return fix_holidays(j, start, end)

    j = fix_holidays(j, start, end)

    # Преобразование к удобному виду
    data = [
        {k: r[i] for i, k in enumerate(j['candles']['columns'])}
        for r in j['candles']['data']
    ]

    # Вывод данных
    return [(data[1]["open"] - data[0]["open"]) / data[0]['open'], data]


if __name__ == "__main__":
    print(get_growth(ticker="MOEX", date="12 Сентября", format="%d %B"))
