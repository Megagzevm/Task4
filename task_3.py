import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import sqlite3


url = "https://hh.ru/search/vacancy"
user_agent = {'User-agent': 'Mozilla/5.0'}
url_vacancy = []
vacancy_skills = []
count = 0
data_to_sql = []

# если делаю как в примере к уроку получаю только 50 вакансий, если наращиваю показатель page получаю необходимые ссылки
for n in range (10):
    url_params = {
        "text": "middle python developer",
        "search_field": "name",
        "per_page": 30,
        "page": n
    }
    # получаем список URL вакансий
    search_result = requests.get(url, headers=user_agent, params=url_params)
    soup = BeautifulSoup(search_result.content.decode(), 'lxml')
    with open("search_results.txt", "w", encoding="UTF-8") as f:
        f.write(search_result.content.decode())
    data = json.loads(soup.find('template', attrs={'id': 'HH-Lux-InitialState'}).text)

    for vacancy in data['vacancySearchResult']['vacancies']:
        url_vacancy.append(vacancy.get('links').get('desktop'))
# отбираем уникальные url вакансий
url_vacancy = set(url_vacancy)
# парсим вакансии и записываем нужные данные в список
for vacancy in url_vacancy:
    # если срабатывает капча, переходим к следующей вакансии
    try:
        result_search = requests.get(vacancy, headers=user_agent, params=url_params)
        soup_vacancy = BeautifulSoup(result_search.content.decode(), 'lxml')
        company_name = soup_vacancy.find('a', attrs={'data-qa': 'vacancy-company-name'}).text
        position = soup_vacancy.find('h1', attrs={'data-qa': 'vacancy-title'}).text
        vacancy_description = soup_vacancy.find('div', attrs={'data-qa': 'vacancy-description'}).text
        key_skills = soup_vacancy.findAll('div', attrs={'data-qa': "bloko-tag bloko-tag_inline skills-element"})
        for s in key_skills:
            vacancy_skills.append(s.text)
        vacancy_skills2 =', '.join(vacancy_skills) # преобразуем для записи в бд
        # проверяем что вакансия содержит данные о скилах
        if len(vacancy_skills) > 0:
            data_to_sql.append(
                {"company_name": company_name, "position": position,
                 "job_description": vacancy_description,
                 "key_skills": vacancy_skills2})
            vacancy_skills.clear()
            count += 1
    except AttributeError:
        print(vacancy)
        continue
    if count == 100:
        break

df = pd.DataFrame(data_to_sql)

# создаем/соединяемся с БД
connection = sqlite3.connect('hw1.db')
# записываем данные в таблицу БД
df.to_sql(name='vacancies', con=connection, if_exists='replace', index=False)
# Фиксируем изменения
connection.commit()
# Проверяем результат
p2 = pd.read_sql('select * from vacancies', connection)
print(p2)
# Закрываем соединение
connection.close()
