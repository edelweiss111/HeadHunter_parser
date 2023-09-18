import psycopg2
from src.classes import HeadHunterAPI
import requests
from src.config import config

EMPLOYERS = ['Яндекс', 'Сбер', 'Ростелеком', 'Касперский', 'Ростех',
             'Айтеко', '1С', 'OCS', 'МОНТ', 'Газпром']


def main():
    params = config()
    db_name = 'headhunter_vacancies'

    data = get_data(EMPLOYERS)
    create_database(params, db_name)
    save_data_to_database(data, db_name, params)


def get_data(employers_list: list):
    """Получает данные по работодателям с платформы 'hh.ru'"""
    vacancies_data = []
    for item in employers_list:
        # Делаем запрос на API HH
        hh = HeadHunterAPI(item)
        employers = hh.get_employers()
        # Выбираем первую кампанию из ответа
        vacancies_response = requests.get(employers[0]['vacancies_url']).json()
        employers_response = requests.get(employers[0]['url']).json()

        # Если есть кампания с точным совпадением названия выбераем ее
        for employer in employers:
            if employer['name'].lower() == item.lower():
                vacancies_response = requests.get(employer['vacancies_url']).json()
                employers_response = requests.get(employer['url']).json()
                break
        # Сохраняем данные в список
        vacancies_data.append({
            'employer': {
                'id': employers_response['id'],
                'name': employers_response['name'],
                'open_vacancies': employers_response['open_vacancies'],
                'url': employers_response['alternate_url'],
                'site_url': employers_response['site_url']
            },
            'vacancies': vacancies_response['items']
        })
    return vacancies_data


def create_database(params: dict, db_name: str) -> None:
    """Создает новую базу данных."""
    try:
        conn = psycopg2.connect(database='postgres', **params)
        conn.autocommit = True
        cur = conn.cursor()
        # Удаляем существующие подключения к базе
        cur.execute(f"SELECT pg_terminate_backend(pg_stat_activity.pid) "
                    f"FROM pg_stat_activity "
                    f"WHERE pg_stat_activity.datname = '{db_name}' "
                    f"AND pid <> pg_backend_pid()")
        cur.execute("DROP DATABASE " + db_name)
        cur.execute("CREATE DATABASE " + db_name)
        cur.close()
        conn.close()
    except psycopg2.errors.InvalidCatalogName:
        # Если базы данных с таким именем не существует
        cur.execute("CREATE DATABASE " + db_name)
        cur.close()
        conn.close()
    except psycopg2.errors.Error as e:
        raise e

    connect = psycopg2.connect(database=db_name, **params)
    try:
        with connect as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        'CREATE TABLE employers ('
                        'employer_id int PRIMARY KEY, '
                        'employer_name varchar(100), '
                        'open_vacancies int, '
                        'url varchar(100), '
                        'site_url varchar(100)'
                        ')'
                    )
                    cur.execute(
                        'CREATE TABLE vacancies ('
                        'vacancy_id int PRIMARY KEY, '
                        'vacancy_name varchar(100), '
                        'city varchar(50), '
                        'salary_from int, '
                        'salary_to int, '
                        'salary_currency char(3), '
                        'requirements text, '
                        'url varchar(100), '
                        'employer_id int NOT NULL,'
                        'FOREIGN KEY (employer_id) REFERENCES employers(employer_id)'
                        ')'
                    )
                except psycopg2.errors.Error as e:
                    raise e
    finally:
        conn.close()


def save_data_to_database(data: list[dict], db_name: str, params: dict):
    """Сохраняет данные о работодателях в базу данных"""
    connect = psycopg2.connect(database=db_name, **params)
    try:
        with connect as conn:
            with conn.cursor() as cur:
                for employer in data:
                    # Заполняем таблицу employers
                    employer_id = employer['employer']['id']
                    employer_name = employer['employer']['name']
                    open_vacancies = employer['employer']['open_vacancies']
                    employer_url = employer['employer']['url']
                    site_url = employer['employer']['site_url']
                    try:
                        cur.execute(
                            f'INSERT INTO employers '
                            f'VALUES (%s, %s, %s, %s, %s)',
                            (employer_id, employer_name, open_vacancies, employer_url, site_url)
                        )
                    except psycopg2.errors.Error as e:
                        print(e)
                        continue
                    # Заполняем таблицу vacancies
                    for vacancy in employer['vacancies']:
                        try:
                            vacancy_id = vacancy['id']
                            vacancy_name = vacancy['name']
                            city = vacancy['area']['name']
                            salary_from = vacancy['salary']['from']
                            salary_to = vacancy['salary']['to']
                            salary_currency = vacancy['salary']['currency']
                            requirements = vacancy['snippet']['requirement']
                            url = vacancy['alternate_url']
                        except TypeError:
                            # Поле salary может быть пустым
                            vacancy_id = vacancy['id']
                            vacancy_name = vacancy['name']
                            city = vacancy['area']['name']
                            salary_from = None
                            salary_to = None
                            salary_currency = None
                            requirements = vacancy['snippet']['requirement']
                            url = vacancy['alternate_url']
                        try:
                            cur.execute(
                                f'INSERT INTO vacancies '
                                f'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                (vacancy_id, vacancy_name, city, salary_from, salary_to,
                                 salary_currency, requirements, url, employer_id)
                            )
                        except psycopg2.errors.Error as e:
                            print(e)
                            continue
    finally:
        conn.close()
