import os
from dotenv import load_dotenv
from tabulate import tabulate
from dbmanager import DBManager
from hhapiclient import HHAPIClient

# Загружаем переменные окружения из .env
load_dotenv()
DB_URL = os.getenv("DB_URL")
EMPLOYER_IDS = os.getenv("EMPLOYER_IDS", "").split(",")


def drop_tables(db):
    """
    Удаляет таблицы vacancies, employers и currencies, если они существуют.
    Используется для полного сброса базы данных перед созданием новой структуры.
    """
    with db.conn.cursor() as cursor:
        cursor.execute("""
            DROP TABLE IF EXISTS vacancies CASCADE;
            DROP TABLE IF EXISTS employers CASCADE;
            DROP TABLE IF EXISTS currencies CASCADE;
        """)
    db.conn.commit()


def initialize_db(db):
    """
    Создает таблицы employers, currencies и vacancies с нужной структурой.
    """
    with db.conn.cursor() as cursor:
        # Создаем таблицу работодателей
        cursor.execute("""
            CREATE TABLE employers (
                id BIGINT PRIMARY KEY,
                name TEXT NOT NULL,
                alternate_url TEXT NOT NULL
            );
        """)
        # Создаем таблицу валют
        cursor.execute("""
            CREATE TABLE currencies (
                code TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                rate NUMERIC NOT NULL
            );
        """)
        # Создаем таблицу вакансий
        cursor.execute("""
            CREATE TABLE vacancies (
                id BIGINT PRIMARY KEY,
                name TEXT NOT NULL,
                employer_id BIGINT REFERENCES employers(id) ON DELETE CASCADE,
                salary_from INT,
                salary_to INT,
                salary_currency TEXT REFERENCES currencies(code),
                description TEXT,
                alternate_url TEXT NOT NULL UNIQUE,
                published_at TIMESTAMP NOT NULL
            );
        """)
    db.conn.commit()


def load_data(db, hh_client):
    """
    Загружает данные из API hh.ru:
      - Работодателей (employers) по списку EMPLOYER_IDS
      - Валюты
      - Вакансии для каждого работодателя
    """
    # Загружаем данные о работодателях и сохраняем в БД
    for employer_id in EMPLOYER_IDS:
        employer_data = hh_client.get_employer(employer_id)
        db.add_employer(
            employer_id=int(employer_data["id"]),
            name=employer_data["name"],
            alternate_url=employer_data["alternate_url"]
        )

    # Загружаем справочник валют и сохраняем в БД
    currencies = hh_client.get_currencies()
    for currency in currencies:
        db.add_currency(
            code=currency["code"],
            name=currency["name"],
            rate=currency["rate"]
        )

    # Отслеживаем добавленные вакансии, чтобы не добавлять дубликаты
    processed_vacancy_ids = set()

    # Загружаем вакансии для каждого работодателя и сохраняем в БД
    for employer_id in EMPLOYER_IDS:
        vacancies_data = hh_client.get_vacancies(employer_id)
        for vacancy in vacancies_data.get("items", []):
            vacancy_id = int(vacancy["id"])
            if vacancy_id in processed_vacancy_ids:
                continue
            processed_vacancy_ids.add(vacancy_id)

            # Добавляем вакансию в БД
            db.add_vacancy(
                vacancy_id=vacancy_id,
                name=vacancy["name"],
                employer_id=int(vacancy["employer"]["id"]),
                salary_from=vacancy["salary"]["from"] if vacancy.get("salary") and vacancy["salary"].get(
                    "from") else None,
                salary_to=vacancy["salary"]["to"] if vacancy.get("salary") and vacancy["salary"].get("to") else None,
                salary_currency=vacancy["salary"]["currency"] if vacancy.get("salary") and vacancy["salary"].get(
                    "currency") else None,
                description=vacancy.get("snippet", {}).get("responsibility", ""),
                alternate_url=vacancy["alternate_url"],
                published_at=vacancy["published_at"]
            )


def interactive_menu(db, hh_client):
    """
    Интерфейс взаимодействия с пользователем.
    Позволяет выполнять выборку данных из базы с выводом в виде таблиц.
    """
    while True:
        print("\nВыберите действие:")
        print("1. Вывести список всех компаний и количество вакансий")
        print("2. Вывести список всех вакансий")
        print("3. Показать среднюю зарплату по вакансиям")
        print("4. Показать вакансии с зарплатой выше средней")
        print("5. Найти вакансии по ключевому слову")
        print("6. Удалить все таблицы и загрузить данные заново")
        print("7. Выйти")

        choice = input("\nВведите номер действия: ").strip()

        if choice == "1":
            companies = db.get_companies_and_vacancies_count()
            if companies:
                headers = ["Компания", "Вакансии"]
                print("\nСписок компаний и количество вакансий:")
                print(tabulate(companies, headers=headers, tablefmt="grid"))
            else:
                print("\nНет данных о компаниях.")

        elif choice == "2":
            vacancies = db.get_all_vacancies()
            if vacancies:
                headers = ["Компания", "Вакансия", "Зарплата", "Ссылка"]
                table = []
                for company, vacancy, salary_from, salary_to, currency, url in vacancies:
                    if salary_from or salary_to:
                        salary = f"от {salary_from}" if salary_from else ""
                        if salary_to:
                            salary += f" до {salary_to}"
                        if currency:
                            salary += f" {currency}"
                    else:
                        salary = "Не указана"
                    table.append([company, vacancy, salary, url])
                print("\nСписок всех вакансий:")
                print(tabulate(table, headers=headers, tablefmt="grid"))
            else:
                print("\nНет данных о вакансиях.")

        elif choice == "3":
            avg_salary = db.get_avg_salary()
            if avg_salary:
                print(f"\nСредняя зарплата: {avg_salary:.2f}")
            else:
                print("\nНет данных для расчета средней зарплаты.")

        elif choice == "4":
            vacancies = db.get_vacancies_with_higher_salary()
            if vacancies:
                headers = ["Компания", "Вакансия", "Зарплата", "Ссылка"]
                table = []
                for company, vacancy, salary_from, salary_to, currency, url in vacancies:
                    if salary_from or salary_to:
                        salary = f"от {salary_from}" if salary_from else ""
                        if salary_to:
                            salary += f" до {salary_to}"
                        if currency:
                            salary += f" {currency}"
                    else:
                        salary = "Не указана"
                    table.append([company, vacancy, salary, url])
                print("\nВакансии с зарплатой выше средней:")
                print(tabulate(table, headers=headers, tablefmt="grid"))
            else:
                print("\nНет вакансий с зарплатой выше средней.")

        elif choice == "5":
            keyword = input("\nВведите ключевое слово: ").strip()
            vacancies = db.get_vacancies_with_keyword(keyword)
            if vacancies:
                headers = ["Компания", "Вакансия", "Зарплата", "Ссылка"]
                table = []
                for company, vacancy, salary_from, salary_to, currency, url in vacancies:
                    if salary_from or salary_to:
                        salary = f"от {salary_from}" if salary_from else ""
                        if salary_to:
                            salary += f" до {salary_to}"
                        if currency:
                            salary += f" {currency}"
                    else:
                        salary = "Не указана"
                    table.append([company, vacancy, salary, url])
                print("\nНайденные вакансии:")
                print(tabulate(table, headers=headers, tablefmt="grid"))
            else:
                print(f"\nВакансий с ключевым словом '{keyword}' не найдено.")

        elif choice == "6":
            print("\nУдаление таблиц и повторная загрузка данных...")
            drop_tables(db)
            initialize_db(db)
            load_data(db, hh_client)
            print("\nДанные успешно загружены!")

        elif choice == "7":
            print("\nЗавершение работы.")
            break
        else:
            print("\nНеверный ввод. Попробуйте снова.")


def main():
    hh_client = HHAPIClient()

    with DBManager(DB_URL) as db:
        # Удаляем старые таблицы и создаем новую структуру
        drop_tables(db)
        initialize_db(db)

        # Загружаем данные из API hh.ru: работодатели, валюты, вакансии
        load_data(db, hh_client)

        # Запускаем интерактивное меню
        interactive_menu(db, hh_client)


if __name__ == "__main__":
    main()
