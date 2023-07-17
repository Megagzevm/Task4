
import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import sqlite3
import time

list_uniq_vacancy = []
key_skills = []
d = []
count = 0
url = "https://api.hh.ru/vacancies"

for i in range(20):
    url_params = {
            "text": "middle python development",
             "search_field": "name",
             "per_page": "30",
             "page": i
    }

    # получаем список вакансий
    result = requests.get(url, params=url_params)
    vacancies = result.json().get('items')


    for vacancy in vacancies:
        list_uniq_vacancy.append(vacancy['url'])
list_url_vacancy = set(list_uniq_vacancy)

for url_vacancy in list_url_vacancy:
    time.sleep(0.5)
    try:
        result_vacancy = requests.get(url_vacancy)
        vacancy_data = result_vacancy.json()
        company_name = vacancy_data['employer']['name']
        position = vacancy_data['name']
        job_description = vacancy_data['description']
        skills = vacancy_data['key_skills']
        if skills:
            for skill in skills:
                key_skills.append(skill['name'])
        key_skills2 =', '.join(key_skills) # преобразуем для записи в бд
        if count < 101:
            d.append({"company_name": company_name, "position": position, "job_description": job_description, "key_skills": key_skills2})
            count += 1
    except KeyError:
        print(url_vacancy)
        continue

df = pd.DataFrame(d)

#создаем/соединяемся с БД
connection = sqlite3.connect('hw1.db')
# записываем данные в таблицу БД
df.to_sql(name='vacancies2', con=connection, if_exists='replace', index=False)
# Фиксируем изменения
connection.commit()
# Проверяем результат
p2 = pd.read_sql('select * from vacancies2', connection)
print(p2)
# Закрываем соединение
connection.close()