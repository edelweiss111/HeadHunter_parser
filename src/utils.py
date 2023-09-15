import psycopg2
from classes import HeadHunterAPI
import requests

EMPLOYERS = ['Яндекс', 'Сбер']


def main():
    pass


def get_data(employers_list: list):
    """Получает данные по работодателям с платформы 'hh.ru'"""
    employers_data = []
    for item in employers_list:

        hh = HeadHunterAPI(item)
        employers = hh.get_employers()

        for employer in employers:
            if employer['name'] == item:
                response = requests.get(employer['vacancies_url']).json()

        employers_data.append({
            'employer': item,
            'vacancies': response['items']
        })
    return employers_data


def create_database(params: dict, db_name: str, employers: list) -> None:
    """Создает новую базу данных."""
    conn = psycopg2.connect(database='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP DATABASE " + db_name)
    cur.execute("CREATE DATABASE " + db_name)
    cur.close()
    conn.close()

    connect = psycopg2.connect(database=db_name, **params)
    with connect as conn:
        with conn.cursor() as cur:
            for employer in employers:
                cur.execute(
                    f'CREATE TABLE {employer} ('
                    'supplier_id serial PRIMARY KEY, '
                    'company_name varchar(50), '
                    'contact varchar(100), '
                    'address varchar(100), '
                    'phone varchar(20), fax varchar(20), '
                    'homepage text, '
                    'products text'
                    ')'
                )


def save_data_to_database(data: list[dict], db_name: str, params: dict):
    """Сохраняет данные о работодателях в базу данных"""
    connect = psycopg2.connect(database=db_name, **params)
    with connect as conn:
        with conn.cursor() as cur:
            cur.execute()


print(get_data(EMPLOYERS))
