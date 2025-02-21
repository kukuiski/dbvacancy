import psycopg2
from typing import List, Tuple, Optional


class DBManager:
    def __init__(self, db_url: str):
        """
        Инициализация подключения к базе данных.
        :param db_url: Строка подключения к БД.
        """
        self.conn = psycopg2.connect(db_url)

    def __enter__(self) -> "DBManager":
        """
        Позволяет использовать объект класса в конструкции with.
        :return: Объект DBManager.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """
        Автоматически закрывает соединение при выходе из блока with.
        """
        self.close()

    def close(self) -> None:
        """
        Закрывает соединение с базой данных.
        """
        self.conn.close()

    def add_vacancy(
            self,
            vacancy_id: int,
            name: str,
            employer_id: int,
            salary_from: Optional[int],
            salary_to: Optional[int],
            salary_currency: Optional[str],
            description: str,
            alternate_url: str,
            published_at: str,
    ) -> None:
        """
        Добавляет вакансию в базу данных, если её ещё нет.
        Если вакансия с таким id уже существует, данные не обновляются.
        """
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO vacancies (id, name, employer_id, salary_from, salary_to, salary_currency, description, alternate_url, published_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
                """,
                (vacancy_id, name, employer_id, salary_from, salary_to, salary_currency, description, alternate_url,
                 published_at),
            )
        self.conn.commit()

    def add_employer(self, employer_id: int, name: str, alternate_url: str) -> None:
        """
        Добавляет работодателя в базу данных, если его ещё нет.
        """
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO employers (id, name, alternate_url)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
                """,
                (employer_id, name, alternate_url),
            )
        self.conn.commit()

    def add_currency(self, code: str, name: str, rate: float) -> None:
        """
        Добавляет валюту в базу данных, если её ещё нет.
        """
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO currencies (code, name, rate)
                VALUES (%s, %s, %s)
                ON CONFLICT (code) DO NOTHING;
                """,
                (code, name, rate),
            )
        self.conn.commit()

    def get_companies_and_vacancies_count(self) -> List[Tuple[str, int]]:
        """
        Возвращает список компаний и количество вакансий для каждой компании.
        """
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT e.name, COUNT(v.id) as vacancies_count
                FROM employers e
                LEFT JOIN vacancies v ON e.id = v.employer_id
                GROUP BY e.name
                ORDER BY vacancies_count DESC;
                """
            )
            return cursor.fetchall()

    def get_all_vacancies(self) -> List[Tuple[str, str, Optional[int], Optional[int], Optional[str], str]]:
        """
        Возвращает список всех вакансий с информацией о компании, названии вакансии,
        зарплате (нижняя и верхняя границы, валюта) и ссылке на вакансию.
        """
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT e.name, v.name, v.salary_from, v.salary_to, v.salary_currency, v.alternate_url
                FROM vacancies v
                JOIN employers e ON v.employer_id = e.id
                ORDER BY v.salary_from DESC NULLS LAST;
                """
            )
            return cursor.fetchall()

    def get_avg_salary(self) -> Optional[float]:
        """
        Рассчитывает среднюю зарплату по вакансиям с учетом валют.
        Если указаны обе границы зарплаты, берется их среднее, иначе используется доступное значение.
        """
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT AVG(
                    CASE 
                        WHEN v.salary_from IS NOT NULL AND v.salary_to IS NOT NULL THEN ((v.salary_from + v.salary_to) / 2.0)
                        WHEN v.salary_from IS NOT NULL THEN v.salary_from
                        WHEN v.salary_to IS NOT NULL THEN v.salary_to
                        ELSE NULL
                    END / COALESCE(c.rate, 1))
                FROM vacancies v
                LEFT JOIN currencies c ON v.salary_currency = c.code
                WHERE v.salary_from IS NOT NULL OR v.salary_to IS NOT NULL;
                """
            )
            result = cursor.fetchone()
            return result[0] if result else None

    def get_vacancies_with_higher_salary(self) -> List[Tuple[str, str, int, int, Optional[str], str]]:
        """
        Возвращает список вакансий, зарплата по которым выше средней.
        Средняя зарплата рассчитывается с учетом валют.
        """
        avg_salary = self.get_avg_salary()
        if avg_salary is None:
            return []
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT e.name, v.name, COALESCE(v.salary_from, 0), COALESCE(v.salary_to, 0), v.salary_currency, v.alternate_url
                FROM vacancies v
                JOIN employers e ON v.employer_id = e.id
                LEFT JOIN currencies c ON v.salary_currency = c.code
                WHERE CASE 
                        WHEN v.salary_from IS NOT NULL AND v.salary_to IS NOT NULL THEN ((v.salary_from + v.salary_to) / 2.0) / COALESCE(c.rate, 1)
                        WHEN v.salary_from IS NOT NULL THEN v.salary_from / COALESCE(c.rate, 1)
                        WHEN v.salary_to IS NOT NULL THEN v.salary_to / COALESCE(c.rate, 1)
                        ELSE NULL
                      END > %s
                ORDER BY v.salary_from DESC NULLS LAST;
                """,
                (avg_salary,),
            )
            return cursor.fetchall()

    def get_vacancies_with_keyword(self, keyword: str) -> List[
        Tuple[str, str, Optional[int], Optional[int], Optional[str], str]]:
        """
        Ищет вакансии, в названии которых содержится указанное слово.
        Регистронезависимый поиск осуществляется посредством приведения строки и ключевого слова к нижнему регистру.
        """
        keyword = f"%{keyword}%"
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT e.name, v.name, v.salary_from, v.salary_to, v.salary_currency, v.alternate_url
                FROM vacancies v
                JOIN employers e ON v.employer_id = e.id
                WHERE LOWER(v.name) LIKE LOWER(%s)
                ORDER BY v.salary_from DESC NULLS LAST;
                """,
                (keyword,),
            )
            return cursor.fetchall()
