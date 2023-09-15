import requests


class HeadHunterAPI:
    """Класс для работы с API платформы HeadHunter"""

    def __init__(self, keyword: str):
        self.employers_url = 'https://api.hh.ru/employers'
        self.vacancies_url = 'https://api.hh.ru/vacancies'
        self.params = {
            'text': keyword,
            'area': 113,
            'only_with_vacancies': True,
            'page': 0,
            'per_page': 100,
        }

    def get_employers(self):
        """Метод, который возвращает работодателей по заданному параметру"""

        response = requests.get(self.employers_url, params=self.params)
        return response.json()['items']
