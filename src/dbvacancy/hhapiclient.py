import requests
from typing import Dict, Any, List


class HHAPIClient:
    BASE_URL: str = "https://api.hh.ru"

    def get_employer(self, employer_id: str) -> Dict[str, Any]:
        """
        Получает данные работодателя по его id.
        :param employer_id: Идентификатор работодателя.
        :return: Словарь с данными работодателя.
        """
        url = f"{self.BASE_URL}/employers/{employer_id}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_vacancies(self, employer_id: str) -> Dict[str, Any]:
        """
        Получает список вакансий работодателя по его id.
        :param employer_id: Идентификатор работодателя.
        :return: Словарь с данными о вакансиях.
        """
        url = f"{self.BASE_URL}/vacancies"
        params = {"employer_id": employer_id}
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_currencies(self) -> List[Dict[str, Any]]:
        """
        Получает справочник валют с актуальными курсами.
        :return: Список словарей с информацией о валютах.
        """
        url = f"{self.BASE_URL}/dictionaries"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get("currency", [])
