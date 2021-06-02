import requests
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import re
from tqdm.auto import tqdm
import pandas as pd
from datetime import date
from collections import Counter
from typing import List, Dict, Any
session = requests.session()
ua = UserAgent(verify_ssl=False)
headers = {'User-Agent': ua.random}


# Парсим главную страницу гисметео и собираем ссылки на прогнозы погоды на 10 дней для популярных городов.
def get_links() -> List[str]:
    city_hrefs = []
    url = 'https://www.gismeteo.ru/'
    req = session.get(url, headers=headers)
    page = req.text
    soup = BeautifulSoup(page, 'html.parser')
    for city in soup.find_all('noscript', {'id': 'noscript'}):
        find_href = re.compile('href="/(.*?)" title="')
        a = str(city)
        b = find_href.findall(a)
        for h in b:
            href = url + h + '10-days/'
            city_hrefs.append(href)
    return city_hrefs


# В этой функции мы собираем список с 10 словарями-прогнозами для одного города.
# Я решила брать прогнозы с виджетов, то есть я для каждого дня перехожу на конкретно его страницу по ссылке.
# Насколько я поняла, это не противоречит формулировке домашнего задания, так как эти ссылки я достаю со страницы
# "10-дней"
def load_forecast(city: str) -> List[dict]:
    list_of_dicts = []
    hrefs = []
    req = session.get(city, headers=headers, stream=True)
    page = req.text
    soup = BeautifulSoup(page, 'html.parser')
    for link in soup.find_all('div', {'class': 'w_date'}):
        find_href = re.compile('" href="/(.*?)"><div class="w_date__day">')
        a = str(link)
        b = ''.join(find_href.findall(a))
        hrefs.append(b)
    for i in range(10):
        forecast = {}
        href = 'https://www.gismeteo.ru/' + hrefs[i]
        r = session.get(href, headers=headers)
        p = r.text
        s = BeautifulSoup(p, 'html.parser')
        div_date = str(s.find_all('div', {'class': 'fill'}))
        find_date = re.compile('data-sunrise="(.*?) ')
        date = find_date.findall(div_date)
        forecast['date'] = ''.join(date)
        div_city = str(s.find_all('span', {'class': 'locality'}))
        find_city = re.compile('<span class="value-title" title="(.*?)"')
        city = find_city.findall(div_city)
        forecast['city'] = ''.join(city)
        div_summary = str(s.find_all('div', {'class': 'tab tooltip'}))
        find_summary = re.compile('<div class="tab tooltip" data-text="(.*?)"')
        summary = find_summary.findall(div_summary)
        forecast['summary'] = ''.join(summary)
        find_temp = re.compile('<span class="unit unit_temperature_c">(.*?)<')
        if i == 0:
            div_temp = str(s.find_all('div', {'class': 'tabtempline_inner tabtemp_0line_inner'}))
            temp = find_temp.findall(div_temp)
            forecast['min_temp'] = int(temp[0].replace('−', '-'))
            if len(temp) != 2:
                forecast['max_temp'] = int(temp[0].replace('−', '-'))
            else:
                forecast['max_temp'] = int(temp[1].replace('−', '-'))
        else:
            div_temp = str(s.find_all('div', {'class': 'tabtempline_inner tabtemp_1line_inner'}))
            temp = find_temp.findall(div_temp)
            forecast['min_temp'] = int(temp[0].replace('−', '-'))
            if len(temp) != 2:
                forecast['max_temp'] = int(temp[0].replace('−', '-'))
            else:
                forecast['max_temp'] = int(temp[1].replace('−', '-'))
        div_max_wind_speed = str(s.find_all('div', {'class': 'w_wind__warning w_wind__warning_'}))
        div_max_wind_speed = div_max_wind_speed.replace('\n', '')
        div_max_wind_speed = div_max_wind_speed.replace(' ', '')
        find_max_wind = re.compile('<spanclass="unitunit_wind_m_s">(.*?)</span>')
        max_wind_speed = max(find_max_wind.findall(div_max_wind_speed))
        forecast['max_wind_speed'] = max_wind_speed
        div_prec = str(s.find_all('div', {'class': 'widget__row widget__row_table widget__row_precipitation'}))
        div_prec = div_prec.replace('\n', '')
        div_prec = div_prec.replace(' ', '')
        find_prec = re.compile('bottom:3px">(.*?)</div>')
        prec = find_prec.findall(div_prec)
        if len(prec) == 0:
            forecast['precipitation'] = 0
        precipitation = 0.0
        for x in prec:
            x = x.replace(',', '.')
            precipitation = precipitation + float(x)
        forecast['precipitation'] = precipitation
        div_pressure = str(s.find_all('span', {'class': 'unit unit_pressure_mm_hg_atm'}))
        find_pressure = re.compile('<span class="unit unit_pressure_mm_hg_atm">([0-9]*?)</span>')
        min_pressure = min(find_pressure.findall(div_pressure))
        max_pressure = max(find_pressure.findall(div_pressure))
        forecast['min_pressure'] = int(min_pressure)
        forecast['max_pressure'] = int(max_pressure)
        list_of_dicts.append(forecast)
    return list_of_dicts


# Эта функция проходится по каждому городу из списка, собирает его прогноз и возращает один список словарей
# с прогнозами для всех городов
def load_all_forecasts() -> List[dict]:
    city_hrefs = get_links()
    all_forecasts = []
    for city in tqdm(city_hrefs):
        forecast = load_forecast(city)
        all_forecasts.extend(forecast)
    return all_forecasts


# Создаем из нашего списка словарей датафрейм, меняем дату на формат pandas datetime, добавляем дни недели и среднее
# скользящее для каждого города с помощью вспомогательной функции
def make_df(func, all_forecasts: list) -> pd.DataFrame:
    df = pd.DataFrame(all_forecasts)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['day_of_week'] = df['date'].dt.weekday
    df = df.groupby('city').apply(func)
    return df


# та самая вспомогательная функция, которая считает среднее скользящее
def add_max_temp_rolling(df: pd.DataFrame) -> pd.DataFrame:
    df['max_temp_rolling'] = df['max_temp'].rolling(3).mean()
    return df


# Здесь мы находим лучший город из датафрейма. Для начала, мы создаем новый датафрейм, в котором оставляем только
# те строки, где день недели = сб или вс (5 или 6). Далее мы рассматриваем три ситуации:
# 1) Сегодня суббота. В таком случае мы для каждого города выбрасываем из датафрейма первые сб и вс
# (остаются только следующие выходные)
# 2) Сегодня воскресенье. Тогда мы выбрасываем для каждого города выбрасываем из датафрейма это вс
# (остаются только следующие выходные)
# 3) Сегодня не суббота и не воскресенье. Следовательно, для каждого города мы оставляем в датафрейме только
# первые сб и вс
# Затем считаем среднюю температуру в каждом городе и возвращаем лучший город и дату нужной нам субботы
def find_best_city(df: pd.DataFrame) -> (str, str):
    df2 = df.copy()
    df2 = df2[df2.day_of_week.isin([5, 6])]
    current_date = date.today().strftime('%Y-%m-%d')
    list_5_1 = list(df2[(df2.date == current_date) & (df2.day_of_week == 5)].index.values.astype(int))
    list_6_1 = list(df2[(df2.date == current_date) & (df2.day_of_week == 6)].index.values.astype(int))
    list_5_2 = []
    for i in list_5_1:
        list_5_2.append(i+1)
    listi_5 = list_5_1 + list_5_2
    if len(listi_5) != 0:
        df2 = df2.drop(listi_5)
    elif len(list_6_1) != 0:
        df2 = df2.drop(list_6_1)
    else:
        list_cities = list(df2['city'])
        list_drop = []
        for c in list_cities:
            weekends = list(df2[(df.city == c)].index.values.astype(int))
            list_drop.append(weekends[0])
            list_drop.append(weekends[1])
        df2 = df2.drop(list_drop)
    dict_mean = {}
    list_cities = list(df2['city'])
    list_min = list(df2['min_temp'])
    list_max = list(df2['max_temp'])
    i = 0
    while True:
        mean = (list_min[i] + list_max[i] + list_min[i+1] + list_max[i+1]) / 4
        dict_mean[list_cities[i]] = mean
        i += 2
        if i >= (len(list_cities) - 1):
            break
    best_city = Counter(dict_mean).most_common(1)[0][0]
    dates = list(df2[(df.city == best_city)]['date'])
    best_date = dates[0]
    dep_date = str(best_date)[:10]
    return best_city, dep_date


# Ищем iata код для нашего лучшего города, достаем билеты (параметр "depart_date" в апи не работает, к сожалению)
# ищем самый дешевый билет на нашу дату
def find_cheapest_ticket(best_city: str, dep_date: str) -> Dict[str, Any]:
    iata_req = requests.get(
        f'https://www.travelpayouts.com/widgets_suggest_params?q=Из%20Москвы%20в%20{best_city}'
    ).json()
    iata = iata_req['destination']['iata']
    aviasales_req = requests.get('http://min-prices.aviasales.ru/calendar_preload',
    params={
        "origin_iata": "MOW",
        "destination": iata,
        "depart_date": dep_date,
        "one_way": "true"
    }).json()
    best_price = 0
    dict_answer = {}
    for d in aviasales_req['best_prices']:
        if d['depart_date'] == dep_date:
            if best_price == 0:
                best_price = d['value']
            if d['value'] < best_price:
                best_price = d['value']
    if best_price == 0:
        dict_answer['error_text'] = 'Билетов нет'
    else:
        dict_answer['price'] = int(best_price)
    return dict_answer


def main():
    all_forecasts = load_all_forecasts()
    df = make_df(add_max_temp_rolling, all_forecasts)
    best_city, dep_date = find_best_city(df)
    dict_answer = find_cheapest_ticket(best_city, dep_date)
    print('Советую Вам слетать в ближайшую субботу ', dep_date)
    print('в ', best_city)
    for key, values in dict_answer.items():
        print('Самый дешевый билет:', values, ' рублей')


if __name__ == "__main__":
    main()
