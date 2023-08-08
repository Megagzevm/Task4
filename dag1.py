import urllib.request
import pandas as pd
import zipfile
import sqlite3
import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from pathlib import Path
import logging

default_args = {
    'owner': 'suchilin',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def unzip_egrul(**kwargs):
    logger = logging.getLogger(__name__)
    # распоковываем архив
    data_zip = zipfile.ZipFile('/Users/aleksandrsucilin/PycharmProjects/pythonProject/home/egrul.json.zip', 'r')
    d = []
    path_unzip = '/Users/aleksandrsucilin/PycharmProjects/pythonProject/home/file_path'
    zip_list = data_zip.namelist()
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    connection = sqlite_hook.get_conn()
    # отбираем данные по телеком компаниям, по коду ОКВЭД
    for i in zip_list:
        data_zip.extract(i, path=path_unzip)
        names = pd.read_json(f'/Users/aleksandrsucilin/PycharmProjects/pythonProject/home/file_path/{i}')
        for c in range(len(names)):
            try:
                if '61.' in names['data'][c]['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'] and '.61' not in \
                        names['data'][c]['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД']:
                    d.append({"Название компании": names['full_name'][c],
                              "ОКВЭД": names['data'][c]['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'],
                              "ИНН": names['data'][c]['ИНН'], "КПП": names['data'][c]['КПП'],
                              "ОГРН": names['data'][c]['ОГРН']})
            except LookupError:
                logger.warning(f'LookuoError {i}')
                continue
        data_file = Path(f'/Users/aleksandrsucilin/PycharmProjects/pythonProject/home/file_path/{i}')
        # после чтения стираем файлик
        data_file.unlink()
    logger.info('process unzip egrul successfully')
    df = pd.DataFrame(d)
    df.to_sql('table_date_egrul', con=connection, if_exists='append')
    logger.info('process load to table_date_egrul successfully')
    #df.to_csv('file_date_egrul')


def pars_vacancy():
    logger = logging.getLogger(__name__)
    list_uniq_vacancy = []
    key_skills = []
    d = []
    count = 0
    url = "https://api.hh.ru/vacancies"
    user_agent = {'User-agent': 'Mozilla/5.0'}
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    connection = sqlite_hook.get_conn()

    for i in range(100):
        logger.warning(f'page: {i}')
        time.sleep(0.5)
        url_params = {
            "text": "middle python development",
            "search_field": "name",
            "per_page": "40",
            "page": i
        }

        # получаем список вакансий
        result = requests.get(url, params=url_params, headers=user_agent)
        vacancies = result.json().get('items')
        try:
            for vacancy in vacancies:
                list_uniq_vacancy.append(vacancy['url'])
        except TypeError:
            logger.warning(f'TypeError: {vacancy["id"]}')
            continue

    list_url_vacancy = set(list_uniq_vacancy)

    for url_vacancy in list_url_vacancy:
        time.sleep(0.5)
        try:
            result_vacancy = requests.get(url_vacancy, headers=user_agent)
            vacancy_data = result_vacancy.json()
            company_name = vacancy_data['employer']['name']
            position = vacancy_data['name']
            skills = vacancy_data['key_skills']
            if skills:
                for skill in skills:
                    key_skills.append(skill['name'])
                    key_skills2 = ', '.join(key_skills)  # преобразуем для записи в бд
            d.append({"url_vacancy": url_vacancy, "company_name": company_name, "position": position,
                      "key_skills": key_skills2})
            key_skills = []
            count += 1
        except KeyError:
            logger.warning(f'KeyError: {url_vacancy}')
            continue
        else:
            logger.info(f'Successfully: {url_vacancy}')
    # звгружаем данные в бд
    df = pd.DataFrame(d)
    logger.warning(f'Successfully: {len(df)}')
    df.to_sql('table_date_vacancy',con=connection, if_exists='append')
    logger.info('load vacancy to table_date_vacancy Successfully')
    #df.to_csv('vacancy')


def top_skills():
    logger = logging.getLogger(__name__)
    skills = []
    list_arr = []
    top = []
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    connection = sqlite_hook.get_conn()
    df_telecom_company = pd.read_sql('select * from table_date_egrul', connection)
    df_vacancy = pd.read_sql('select * from table_date_vacancy', connection)
    #df_telecom_company = pd.read_csv('/Users/aleksandrsucilin/file_date_egrul')
    #df_vacancy = pd.read_csv('/Users/aleksandrsucilin/PycharmProjects/pythonProject/vacancy')

    for i in range(len(df_vacancy)):
        for c in df_telecom_company['Название компании']:
            if df_vacancy['company_name'][i] in c:
                skills.append(df_vacancy['key_skills'][i])
                break
    for i in range(len(skills)):
        list_arr.append(skills[i].split(','))
    flat_list = [item for sublist in list_arr for item in sublist]

    uniq_skills = set(flat_list)
    list_uniq_skills = list(uniq_skills)

    for i in range(len(list_uniq_skills)):
        top.append({"skill": list_uniq_skills[i], "count": flat_list.count(list_uniq_skills[i])})
    df = pd.DataFrame(top)

    df.to_sql('table_top_skills', con=connection, if_exists='append')
    logger.info('load top10 to table_top_skills Successfully')
    print(df.sort_values(by=['count'], ascending=False).head(10).to_string(index=False))
    logger.info('all processes completed successfully')


with DAG(
    dag_id='final_project',
    default_args=default_args,
    description="DAG for download file",
    start_date=datetime(2023, 7, 31, 22, 30),
    schedule_interval='@daily'
) as dag:

    # task1 = BashOperator(
    #     task_id='download_file',
    #     bash_command='curl https://ofdata.ru/open-data/download/egrul.json.zip -o /Users/aleksandrsucilin/PycharmProjects/pythonProject/home/egrul.json.zip'
    # )

    task2 = PythonOperator(
        task_id='unzip_file',
        python_callable=unzip_egrul
    )

    task3 = PythonOperator(
        task_id='pars_vacancy',
        python_callable=pars_vacancy
    )

    task4 = PythonOperator(
        task_id='top10',
        python_callable=top_skills
    )

    task2 >> task3 >> task4


